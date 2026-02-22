/*
 * Copyright 2015-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.benchmarks.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.benchmarks.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.checkPublicationResult;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationStreamId;
import static io.aeron.benchmarks.aeron.AeronUtil.idleStrategy;
import static io.aeron.benchmarks.aeron.AeronUtil.launchEmbeddedMediaDriverIfConfigured;
import static io.aeron.benchmarks.aeron.AeronUtil.receiverIndex;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceStreamId;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which echoes original messages back to the sender, with a configurable periodic stall.
 * Implemented as a flat state machine: LIVE, STALLING, RECOVERING.
 * Runs until the JVM exits via shutdown signal.
 *
 * Recovery modes:
 *   GAP          - accept the gap, wait for a new image
 *   REPLAY_MERGE - replay from archive and merge back to live
 *   REPLAY_MERGE_V2 - not yet implemented
 *
 * Subscription lifecycle (REPLAY_MERGE mode):
 *   1. liveSubscription starts as the initial multicast subscription.
 *   2. When its image closes: liveSubscription is closed, state -> RECOVERING.
 *   3. A fresh mergeSubscription is created for ReplayMerge.
 *   4. When isMerged(): ReplayMerge wrapper is closed; mergeSubscription is promoted
 *      to liveSubscription (it now carries the live image). state -> LIVE.
 *   5. When that image closes: repeat from step 2.
 *   One subscription is active at any time; each subscription is closed when its image closes.
 */
public final class RecoveringEchoNode implements AutoCloseable, Runnable
{
    static final String PAUSE_MS_PROP                         = "recovering.echo.pause.ms";
    static final String PAUSE_EVERY_MS_PROP                   = "recovering.echo.pause.every.ms";
    static final String RECOVERY_MODE_PROP                    = "recovering.echo.recovery.mode";
    static final String ARCHIVE_CONTROL_CHANNEL_PROP          = "recovering.echo.archive.control.channel";
    static final String ARCHIVE_CONTROL_STREAM_PROP           = "recovering.echo.archive.control.stream";
    static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP = "recovering.echo.archive.control.response.channel";
    static final String REPLAY_CHANNEL_PROP                   = "recovering.echo.replay.channel";
    static final String REPLAY_DESTINATION_PROP               = "recovering.echo.replay.destination";
    static final String RECORDING_ID_PROP                     = "recovering.echo.recording.id";

    private static final long LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter
        .ofPattern("HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());

    enum RecoveryMode { GAP, REPLAY_MERGE, REPLAY_MERGE_V2 }

    enum State { LIVE, STALLING, RECOVERING }

    // -------------------------------------------------------------------------
    // Fixed node fields
    // -------------------------------------------------------------------------

    private final BufferClaim bufferClaim = new BufferClaim();
    private final FragmentHandler fragmentHandler;
    private final ExclusivePublication publication;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;
    private final long pauseNs;
    private final long pauseEveryNs;
    private final boolean stallingEnabled;
    private final RecoveryMode recoveryMode;

    // REPLAY_MERGE archive resources
    private final AeronArchive aeronArchive;
    private final String replayChannelBase;
    private final String replayDestination;
    private final long recordingId;

    // -------------------------------------------------------------------------
    // Run-loop state
    // -------------------------------------------------------------------------

    private State state = State.LIVE;

    // Current image being polled in LIVE/STALLING states.
    private Image image;
    private long nextStallDeadlineNs = Long.MAX_VALUE;
    private long stallDeadlineNs     = 0;

    // The subscription that owns `image`. Initially the multicast subscription; after each
    // recovery it is the promoted mergeSubscription. Closed when its image closes.
    private Subscription liveSubscription;

    // Exists only while a ReplayMerge is in progress. On merge completion, promoted to
    // liveSubscription; replayMerge wrapper is closed and nulled.
    private ReplayMerge  replayMerge;
    private Subscription mergeSubscription;

    private long lostPosition;
    private long recoveryStartNs;
    private long recoveryDeadlineNs       = Long.MAX_VALUE;
    private long recoveryArchiveFragments;
    private long recoveryLiveFragments;
    private boolean recoveryInArchivePhase;
    private boolean recoveryImageAcquiredLogged;
    private boolean recoveryLiveAddedLogged;
    private long    recoveryPreCheckLastLogNs = Long.MIN_VALUE;

    // -------------------------------------------------------------------------
    // Counters
    // -------------------------------------------------------------------------

    private long totalMessagesEchoed      = 0;
    private long currentLivePhaseMessages = 0;
    private long totalLiveMessages        = 0;
    private long totalArchiveMessages     = 0;
    private long stallCount               = 0;
    private int  recoveryAttempts         = 0;
    private long requestReceived          = 0;
    private long responsesSend            = 0;

    // Diagnostics
    private long    lastDiagTotalEchoed    = 0;
    private long    lastDiagNs             = 0;
    private long    firstMessageNs         = 0;
    private boolean imageClosedDuringStall = false;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    RecoveringEchoNode(final AtomicBoolean running)
    {
        this(running, launchEmbeddedMediaDriverIfConfigured(), connect(), true, receiverIndex());
    }

    RecoveringEchoNode(
        final AtomicBoolean running,
        final MediaDriver mediaDriver,
        final Aeron aeron,
        final boolean ownsAeronClient,
        final int receiverIndex)
    {
        this.running         = running;
        this.mediaDriver     = mediaDriver;
        this.aeron           = aeron;
        this.ownsAeronClient = ownsAeronClient;

        this.pauseNs         = TimeUnit.MILLISECONDS.toNanos(Long.getLong(PAUSE_MS_PROP, 0));
        this.pauseEveryNs    = TimeUnit.MILLISECONDS.toNanos(Long.getLong(PAUSE_EVERY_MS_PROP, 0));
        this.stallingEnabled = pauseNs > 0 && pauseEveryNs > 0;
        this.recoveryMode    = RecoveryMode.valueOf(
            System.getProperty(RECOVERY_MODE_PROP, RecoveryMode.GAP.name()));

        System.out.println("pauseMs: "      + Long.getLong(PAUSE_MS_PROP, 0));
        System.out.println("pauseEveryMs: " + Long.getLong(PAUSE_EVERY_MS_PROP, 0));
        System.out.println("recoveryMode: " + recoveryMode);
        System.out.println("EchoNode.init(receiverIndex=" + receiverIndex + ")");
        System.out.println("  publication (source):       channel=" +
            sourceChannel() + " stream=" + sourceStreamId());
        System.out.println("  subscription (destination): channel=" +
            destinationChannel() + " stream=" + destinationStreamId());

        publication = aeron.addExclusivePublication(sourceChannel(), sourceStreamId());
        System.out.println("  publication created: sessionId=" + publication.sessionId());

        liveSubscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
        System.out.println("  subscription created");

        if (recoveryMode == RecoveryMode.REPLAY_MERGE)
        {
            this.recordingId       = Long.getLong(RECORDING_ID_PROP, 0);
            this.replayChannelBase = System.getProperty(REPLAY_CHANNEL_PROP);
            this.replayDestination = System.getProperty(REPLAY_DESTINATION_PROP);

            System.out.printf("%s connecting to archive: controlChannel=%s, controlStream=%s, " +
                    "controlResponseChannel=%s%n",
                ts(),
                System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP),
                System.getProperty(ARCHIVE_CONTROL_STREAM_PROP),
                System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP));

            this.aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP))
                .controlRequestStreamId(Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0))
                .controlResponseChannel(System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP)));

            System.out.printf("%s archive connected. recordingId=%d, replayChannelBase=%s, " +
                    "replayDestination=%s, liveDestination=%s%n",
                ts(), recordingId, replayChannelBase, replayDestination, destinationChannel());
        }
        else
        {
            this.recordingId       = -1;
            this.replayChannelBase = null;
            this.replayDestination = null;
            this.aeronArchive      = null;
        }

        fragmentHandler = (buffer, offset, length, header) ->
        {
            requestReceived++;
            if (lastDiagNs == 0)
            {
                firstMessageNs      = System.nanoTime();
                lastDiagNs          = firstMessageNs;
                lastDiagTotalEchoed = 0;
            }
            long result;
            while ((result = publication.tryClaim(length, bufferClaim)) <= 0)
            {
                checkPublicationResult(result);
            }
            bufferClaim.flags(header.flags()).putBytes(buffer, offset, length).commit();
            responsesSend++;
        };
    }

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

    private static String ts()
    {
        return TS_FMT.format(Instant.now());
    }

    private static void log(final String fmt, final Object... args)
    {
        final Object[] fullArgs = new Object[args.length + 1];
        fullArgs[0] = ts();
        System.arraycopy(args, 0, fullArgs, 1, args.length);
        System.out.printf("%s " + fmt + "%n", fullArgs);
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    public void run()
    {
        awaitConnected(
            () -> liveSubscription.isConnected() && publication.availableWindow() > 0,
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        final IdleStrategy idle = idleStrategy();

        image = liveSubscription.imageAtIndex(0);
        log("Image acquired: sessionId=%d, position=%d, mtu=%d, correlationId=%d",
            image.sessionId(), image.position(), image.mtuLength(), image.correlationId());

        if (stallingEnabled)
        {
            nextStallDeadlineNs = System.nanoTime() + pauseEveryNs;
        }

        log("State machine starting in LIVE");

        long nextLogNs = System.nanoTime() + LOG_INTERVAL_NS;

        while (running.get())
        {
            final int workCount = doWork();
            idle.idle(workCount);

            final long nowNs = System.nanoTime();
            if (nowNs >= nextLogNs)
            {
                logDiagnostics();
                nextLogNs = nowNs + LOG_INTERVAL_NS;
            }
        }

        log("Shutdown. totalEchoed=%,d, totalLive=%,d, totalArchive=%,d, " +
                "stallCount=%,d, recoveryAttempts=%d",
            totalMessagesEchoed, totalLiveMessages, totalArchiveMessages,
            stallCount, recoveryAttempts);
    }

    // -------------------------------------------------------------------------
    // Dispatch
    // -------------------------------------------------------------------------

    private int doWork()
    {
        switch (state)
        {
            case LIVE:       return doLive();
            case STALLING:   return doStall();
            case RECOVERING: return doRecovery();
            default:
                throw new IllegalStateException("Unknown state: " + state);
        }
    }

    // -------------------------------------------------------------------------
    // LIVE
    // -------------------------------------------------------------------------

    private int doLive()
    {
        if (stallingEnabled && System.nanoTime() >= nextStallDeadlineNs)
        {
            stallCount++;
            stallDeadlineNs = System.nanoTime() + pauseNs;
            log("LIVE -> STALLING: stall #%,d, pausing %,d ms, totalEchoed=%,d, imagePosition=%d",
                stallCount, pauseNs / 1_000_000, totalMessagesEchoed, image.position());
            state = State.STALLING;
            return 1;
        }

        if (image.isClosed())
        {
            lostPosition = image.position();
            log("Image CLOSED in LIVE: position=%d, totalEchoed=%,d, currentLivePhase=%,d",
                lostPosition, totalMessagesEchoed, currentLivePhaseMessages);
            totalLiveMessages        += currentLivePhaseMessages;
            currentLivePhaseMessages  = 0;

            // Image is gone — close the subscription that owned it.
            // A fresh mergeSubscription will be created for the next ReplayMerge.
            closeAll(liveSubscription);
            liveSubscription = null;

            state = State.RECOVERING;
            return 1;
        }

        final int fragments = image.poll(fragmentHandler, FRAGMENT_LIMIT);
        if (fragments > 0)
        {
            totalMessagesEchoed      += fragments;
            currentLivePhaseMessages += fragments;
        }

        return fragments;
    }

    // -------------------------------------------------------------------------
    // STALLING
    // -------------------------------------------------------------------------

    private int doStall()
    {
        if (image.isClosed() && !imageClosedDuringStall)
        {
            imageClosedDuringStall = true;
            log("Image CLOSED during STALLING #%,d: position=%d, totalEchoed=%,d",
                stallCount, image.position(), totalMessagesEchoed);
        }

        if (System.nanoTime() >= stallDeadlineNs)
        {
            log("STALLING -> LIVE: stall #%,d complete, imageClosed=%b, imagePosition=%d",
                stallCount, image.isClosed(), image.position());
            nextStallDeadlineNs    = System.nanoTime() + pauseEveryNs;
            imageClosedDuringStall = false;
            state = State.LIVE;
            return 1;
        }

        return 0;
    }

    // -------------------------------------------------------------------------
    // RECOVERING — dispatch
    // -------------------------------------------------------------------------

    private int doRecovery()
    {
        switch (recoveryMode)
        {
            case GAP:             return doRecoveryGap();
            case REPLAY_MERGE:    return doRecoveryReplayMerge();
            case REPLAY_MERGE_V2: return doRecoveryReplayMergeV2();
            default:
                throw new IllegalStateException("Unknown recovery mode: " + recoveryMode);
        }
    }

    // -------------------------------------------------------------------------
    // RECOVERING — GAP
    // -------------------------------------------------------------------------

    private int doRecoveryGap()
    {
        if (liveSubscription == null)
        {
            liveSubscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
        }

        if (liveSubscription.imageCount() > 0)
        {
            image = liveSubscription.imageAtIndex(0);
            log("RECOVERING (GAP): new image acquired. sessionId=%d, position=%d",
                image.sessionId(), image.position());
            state = State.LIVE;
            return 1;
        }

        return 0;
    }

    // -------------------------------------------------------------------------
    // RECOVERING — REPLAY_MERGE
    // -------------------------------------------------------------------------

    private int doRecoveryReplayMerge()
    {
        if (replayMerge != null && System.nanoTime() > recoveryDeadlineNs)
        {
            throw new IllegalStateException(
                "ReplayMerge TIMED OUT on attempt #" + recoveryAttempts +
                    ", lostPosition=" + lostPosition +
                    ", elapsedMs=" + ((System.nanoTime() - recoveryStartNs) / 1_000_000) +
                    ", archiveFragments=" + recoveryArchiveFragments +
                    ", liveFragments=" + recoveryLiveFragments);
        }

        if (replayMerge == null)
        {
            recoveryStartNs             = System.nanoTime();
            recoveryDeadlineNs          = recoveryStartNs + connectionTimeoutNs();
            recoveryArchiveFragments    = 0;
            recoveryLiveFragments       = 0;
            recoveryInArchivePhase      = true;
            recoveryImageAcquiredLogged = false;
            recoveryLiveAddedLogged     = false;

            final long archivePosition = aeronArchive.getRecordingPosition(recordingId);

            if (archivePosition <= lostPosition)
            {
                final long nowNs = System.nanoTime();
                if (nowNs - recoveryPreCheckLastLogNs >= LOG_INTERVAL_NS)
                {
                    log("RECOVERING (REPLAY_MERGE) pre-check: archive not ahead — " +
                            "lostPosition=%d, archivePosition=%d, waiting...",
                        lostPosition, archivePosition);
                    recoveryPreCheckLastLogNs = nowNs;
                }
                recoveryStartNs    = 0;
                recoveryDeadlineNs = Long.MAX_VALUE;
                return 0;
            }

            recoveryAttempts++;
            recoveryPreCheckLastLogNs = Long.MIN_VALUE;

            log("RECOVERING #%d (REPLAY_MERGE) pre-check: lostPosition=%d, " +
                    "archivePosition=%d, delta=%d",
                recoveryAttempts, lostPosition, archivePosition, archivePosition - lostPosition);

            final long[] startPosHolder  = new long[1];
            final long[] stopPosHolder   = new long[1];
            final int[]  sessionIdHolder = new int[1];
            final int found = aeronArchive.listRecording(
                recordingId,
                (controlSessionId, correlationId, recordingId1, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                {
                    sessionIdHolder[0] = sessionId;
                    startPosHolder[0]  = startPosition;
                    stopPosHolder[0]   = stopPosition;
                });

            if (found == 0)
            {
                throw new IllegalStateException("Recording not found for recordingId=" + recordingId);
            }

            final int recordingSessionId = sessionIdHolder[0];
            log("RECOVERING #%d (REPLAY_MERGE) positions: requestedReplayPosition=%d, " +
                    "recordingStartPosition=%d, recordingStopPosition=%d, " +
                    "currentArchivePosition=%d, bytesToReplay=%d",
                recoveryAttempts,
                lostPosition, startPosHolder[0], stopPosHolder[0],
                archivePosition, archivePosition - lostPosition);

            final String replayChannel = new ChannelUriStringBuilder(replayChannelBase)
                .sessionId(recordingSessionId)
                .build();

            log("RECOVERING #%d (REPLAY_MERGE): initiating. recordingId=%d, " +
                    "recordingSessionId=%d, replayChannel=%s, replayDestination=%s, liveDestination=%s",
                recoveryAttempts, recordingId, recordingSessionId,
                replayChannel, replayDestination, destinationChannel());

            mergeSubscription = aeron.addSubscription(
                new ChannelUriStringBuilder()
                    .media("udp")
                    .controlMode("manual")
                    .sessionId(recordingSessionId)
                    .build(),
                destinationStreamId());

            replayMerge = new ReplayMerge(
                mergeSubscription,
                aeronArchive,
                replayChannel,
                replayDestination,
                destinationChannel(),
                recordingId,
                lostPosition);

            return 1;
        }

        if (replayMerge.hasFailed())
        {
            throw new IllegalStateException(
                "ReplayMerge FAILED on attempt #" + recoveryAttempts +
                    ", lostPosition=" + lostPosition +
                    ", archiveFragments=" + recoveryArchiveFragments +
                    ", liveFragments=" + recoveryLiveFragments);
        }

        final int fragments = replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT);

        if (recoveryInArchivePhase)
        {
            recoveryArchiveFragments += fragments;
        }
        else
        {
            recoveryLiveFragments += fragments;
        }

        if (!recoveryImageAcquiredLogged && replayMerge.image() != null)
        {
            final Image img = replayMerge.image();
            log("RECOVERING #%d (REPLAY_MERGE): replay image ACQUIRED. " +
                    "sessionId=%d, joinPosition=%d, currentPosition=%d, " +
                    "requestedPosition=%d, joinDelta=%d",
                recoveryAttempts,
                img.sessionId(), img.joinPosition(), img.position(),
                lostPosition, img.joinPosition() - lostPosition);
            recoveryImageAcquiredLogged = true;
        }

        if (!recoveryLiveAddedLogged && replayMerge.isLiveAdded())
        {
            final Image img = replayMerge.image();
            log("RECOVERING #%d (REPLAY_MERGE): live destination ADDED. " +
                    "archiveFragments=%,d, imagePosition=%d",
                recoveryAttempts,
                recoveryArchiveFragments, img != null ? img.position() : -1L);
            recoveryInArchivePhase  = false;
            recoveryLiveAddedLogged = true;
        }

        if (replayMerge.isMerged())
        {
            final Image mergedImage = mergeSubscription.imageAtIndex(0);
            final long elapsedMs    = (System.nanoTime() - recoveryStartNs) / 1_000_000;

            log("RECOVERING #%d (REPLAY_MERGE): MERGED. " +
                    "sessionId=%d, joinPosition=%d, position=%d, " +
                    "requestedPosition=%d, joinDelta=%d, " +
                    "archiveFragments=%,d, liveFragments=%,d, elapsedMs=%,d",
                recoveryAttempts,
                mergedImage.sessionId(), mergedImage.joinPosition(), mergedImage.position(),
                lostPosition, mergedImage.joinPosition() - lostPosition,
                recoveryArchiveFragments, recoveryLiveFragments, elapsedMs);

            totalArchiveMessages += recoveryArchiveFragments;
            totalMessagesEchoed  += recoveryArchiveFragments + recoveryLiveFragments;

            log("Merge #%d counters: archiveDelta=%,d, liveDelta=%,d, " +
                    "totalArchive=%,d, totalLive=%,d, total=%,d",
                recoveryAttempts,
                recoveryArchiveFragments, recoveryLiveFragments,
                totalArchiveMessages, totalLiveMessages, totalMessagesEchoed);

            // Close only the ReplayMerge wrapper — mergeSubscription stays open.
            // Promote mergeSubscription to liveSubscription: it now carries the live image.
            replayMerge.close();
            replayMerge = null;

            image             = mergedImage;
            liveSubscription  = mergeSubscription;
            mergeSubscription = null;

            currentLivePhaseMessages = 0;
            nextStallDeadlineNs      = stallingEnabled ? System.nanoTime() + pauseEveryNs : Long.MAX_VALUE;
            state                    = State.LIVE;
            return 1;
        }

        return fragments;
    }

    // -------------------------------------------------------------------------
    // RECOVERING — REPLAY_MERGE_V2
    // -------------------------------------------------------------------------

    private int doRecoveryReplayMergeV2()
    {
        throw new UnsupportedOperationException("REPLAY_MERGE_V2 is not yet implemented");
    }

    // -------------------------------------------------------------------------
    // Periodic diagnostics
    // -------------------------------------------------------------------------

    private void logDiagnostics()
    {
        final String stateDetails;
        switch (state)
        {
            case LIVE:
                stateDetails = String.format(
                    "imagePosition=%d, nextStallInMs=%,d",
                    image.position(),
                    stallingEnabled
                        ? Math.max(0, (nextStallDeadlineNs - System.nanoTime()) / 1_000_000)
                        : -1);
                break;

            case STALLING:
                stateDetails = String.format(
                    "imagePosition=%d, stallRemainingMs=%,d",
                    image.position(),
                    Math.max(0, (stallDeadlineNs - System.nanoTime()) / 1_000_000));
                break;

            case RECOVERING:
                switch (recoveryMode)
                {
                    case GAP:
                        stateDetails = String.format(
                            "mode=GAP, lostPosition=%d, waitingForImage=%b",
                            lostPosition,
                            liveSubscription == null || liveSubscription.imageCount() == 0);
                        break;

                    case REPLAY_MERGE:
                    {
                        final Image rImg = replayMerge != null ? replayMerge.image() : null;
                        stateDetails = String.format(
                            "mode=REPLAY_MERGE, attempt=%d, lostPosition=%d, " +
                                "archiveFragments=%,d, liveFragments=%,d, " +
                                "imageAcquired=%b, liveAdded=%b, merged=%b, " +
                                "replayImagePosition=%d, elapsedMs=%,d, remainingMs=%,d",
                            recoveryAttempts,
                            lostPosition,
                            recoveryArchiveFragments,
                            recoveryLiveFragments,
                            recoveryImageAcquiredLogged,
                            recoveryLiveAddedLogged,
                            replayMerge != null && replayMerge.isMerged(),
                            rImg != null ? rImg.position() : -1L,
                            recoveryStartNs > 0 ? (System.nanoTime() - recoveryStartNs) / 1_000_000 : 0,
                            recoveryDeadlineNs == Long.MAX_VALUE ? Long.MAX_VALUE :
                                Math.max(0, (recoveryDeadlineNs - System.nanoTime()) / 1_000_000));
                        break;
                    }

                    case REPLAY_MERGE_V2:
                        stateDetails = "mode=REPLAY_MERGE_V2, not yet implemented";
                        break;

                    default:
                        throw new IllegalStateException("Unknown recovery mode: " + recoveryMode);
                }
                break;

            default:
                throw new IllegalStateException("Unknown state: " + state);
        }

        final long nowNs           = System.nanoTime();
        final long echoDelta       = totalMessagesEchoed - lastDiagTotalEchoed;
        final long elapsedMs       = lastDiagNs > 0 ? (nowNs - lastDiagNs) / 1_000_000 : 0;
        final long ratePerSec      = elapsedMs > 0 ? echoDelta * 1000 / elapsedMs : 0;
        final long totalElapsedMs  = firstMessageNs > 0 ? (nowNs - firstMessageNs) / 1_000_000 : 0;
        final long totalRatePerSec = totalElapsedMs > 0 ? totalMessagesEchoed * 1000 / totalElapsedMs : 0;
        lastDiagTotalEchoed        = totalMessagesEchoed;
        lastDiagNs                 = nowNs;

        log("[DIAG] state=%s, rate=%,d/s, totalRate=%,d/s, totalEchoed=%,d, requests=%,d, responses=%,d, " +
                "totalLive=%,d, totalArchive=%,d, currentLivePhase=%,d, " +
                "stallCount=%,d, recoveryAttempts=%d | %s",
            state,
            ratePerSec,
            totalRatePerSec,
            totalMessagesEchoed,
            requestReceived,
            responsesSend,
            totalLiveMessages,
            totalArchiveMessages,
            currentLivePhaseMessages,
            stallCount,
            recoveryAttempts,
            stateDetails);
    }

    // -------------------------------------------------------------------------
    // Close
    // -------------------------------------------------------------------------

    public void close()
    {
        log("StallingEchoNode: closing");
        if (replayMerge != null)
        {
            replayMerge.close();
            replayMerge = null;
        }
        closeAll(mergeSubscription);
        closeAll(aeronArchive);
        closeAll(liveSubscription);
        closeAll(publication);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
        log("StallingEchoNode: closed");
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir     = Configuration.resolveLogsDir();
        final int  receiverIndex = AeronUtil.receiverIndex();

        final AtomicBoolean running = new AtomicBoolean(true);
        try (
            ShutdownSignalBarrier shutdownSignalBarrier =
                new ShutdownSignalBarrier(() -> running.set(false));
            RecoveringEchoNode node = new RecoveringEchoNode(running))
        {
            Thread.currentThread().setName("echo-" + receiverIndex);

            try
            {
                node.run();
            }
            finally
            {
                System.out.println("Requests received: " + node.requestReceived);
                System.out.println("Responses send: "    + node.responsesSend);

                final String prefix = "echo-node-" + receiverIndex + "-";
                AeronUtil.dumpAeronStats(
                    node.aeron.context().cncFile(),
                    outputDir.resolve(prefix + "aeron-stat.txt"),
                    outputDir.resolve(prefix + "errors.txt"));
            }
        }
    }
}