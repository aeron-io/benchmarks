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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.RECEIVER_INDEX_OFFSET;
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
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which echoes original messages back to the sender, with a configurable periodic stall.
 *
 * <p>Stall behaviour is controlled via JVM system properties:
 * <ul>
 *   <li>{@code stalling.echo.pause.ms} - duration of each pause in milliseconds (default: 0, disabled)</li>
 *   <li>{@code stalling.echo.pause.every.ms} - interval between stalls in milliseconds; a stall is
 *       triggered when this interval has elapsed since the last stall and a fragment is being processed
 *       (default: 0, disabled)</li>
 *   <li>{@code stalling.echo.recovery.mode} - recovery mode: GAP, REPLAY_MERGE, REPLAY_MERGE_V2
 *       (default: GAP)</li>
 * </ul>
 *
 * <p>Both {@code stalling.echo.pause.ms} and {@code stalling.echo.pause.every.ms} must be non-zero
 * for stalling to be enabled.
 *
 * <p>For REPLAY_MERGE and REPLAY_MERGE_V2 modes, additional properties are required:
 * <ul>
 *   <li>{@code stalling.echo.archive.control.channel} - archive control request channel</li>
 *   <li>{@code stalling.echo.archive.control.stream} - archive control request stream id</li>
 *   <li>{@code stalling.echo.archive.control.response.channel} - archive control response channel</li>
 *   <li>{@code stalling.echo.replay.channel} - channel for replay</li>
 *   <li>{@code stalling.echo.replay.destination} - replay destination (e.g. localhost:0)</li>
 *   <li>{@code stalling.echo.recording.id} - recording id to replay from</li>
 * </ul>
 */
public final class StallingEchoNode implements AutoCloseable, Runnable
{
    static final String PAUSE_MS_PROP = "stalling.echo.pause.ms";
    static final String PAUSE_EVERY_MS_PROP = "stalling.echo.pause.every.ms";
    static final String RECOVERY_MODE_PROP = "stalling.echo.recovery.mode";
    static final String ARCHIVE_CONTROL_CHANNEL_PROP = "stalling.echo.archive.control.channel";
    static final String ARCHIVE_CONTROL_STREAM_PROP = "stalling.echo.archive.control.stream";
    static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP =
        "stalling.echo.archive.control.response.channel";
    static final String REPLAY_CHANNEL_PROP = "stalling.echo.replay.channel";
    static final String REPLAY_DESTINATION_PROP = "stalling.echo.replay.destination";
    static final String RECORDING_ID_PROP = "stalling.echo.recording.id";

    // -------------------------------------------------------------------------
    // Recovery mode
    // -------------------------------------------------------------------------

    enum RecoveryMode
    {
        GAP,
        REPLAY_MERGE,
        REPLAY_MERGE_V2
    }

    // -------------------------------------------------------------------------
    // Strategy interface
    // -------------------------------------------------------------------------

    interface RecoveryStrategy extends AutoCloseable
    {
        /**
         * Attempt to recover after the image has been lost.
         *
         * @param subscription    the subscription to recover on
         * @param position        the position at which the image was lost
         * @param fragmentHandler fragment handler to use during catch-up polling
         * @return a new {@link Image} to resume polling from, or {@code null} if no recovery is possible
         */
        Image recover(Subscription subscription, long position, FragmentHandler fragmentHandler);

        void close();
    }

    // -------------------------------------------------------------------------
    // Strategy: gap acceptance
    // -------------------------------------------------------------------------

    static final class GapAcceptanceRecoveryStrategy implements RecoveryStrategy
    {
        @Override
        public Image recover(
            final Subscription subscription,
            final long position,
            final FragmentHandler fragmentHandler)
        {
            System.out.printf(
                "[%s] GapAcceptance: image lost at position=%d, accepting gap%n",
                Thread.currentThread().getName(),
                position);
            return null;
        }

        @Override
        public void close()
        {
        }
    }

    // -------------------------------------------------------------------------
    // Strategy: replay merge v1
    // -------------------------------------------------------------------------

    static final class ReplayMergeRecoveryStrategy implements RecoveryStrategy
    {
        private final AeronArchive archive;
        private final Subscription mergeSubscription;
        private final String replayChannelBase;
        private final String replayDestination;
        private final long recordingId;
        long lastArchiveFragments;
        long lastLiveFragments;

        ReplayMergeRecoveryStrategy(final Aeron aeron)
        {
            this.replayChannelBase = System.getProperty(REPLAY_CHANNEL_PROP);
            this.replayDestination = System.getProperty(REPLAY_DESTINATION_PROP);
            this.recordingId = Long.getLong(RECORDING_ID_PROP, 0);

            System.out.printf("[%s] ReplayMergeRecoveryStrategy: connecting to archive...%n",
                Thread.currentThread().getName());
            System.out.printf(
                "[%s]   controlChannel=%s, controlStream=%s, controlResponseChannel=%s%n",
                Thread.currentThread().getName(),
                System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP),
                System.getProperty(ARCHIVE_CONTROL_STREAM_PROP),
                System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP));

            this.archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP))
                .controlRequestStreamId(Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0))
                .controlResponseChannel(System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP)));

            this.mergeSubscription = aeron.addSubscription(
                "aeron:udp?control-mode=manual", destinationStreamId());

            System.out.printf(
                "[%s] ReplayMergeRecoveryStrategy: archive connected. " +
                    "recordingId=%d, replayChannelBase=%s, replayDestination=%s, liveDestination=%s%n",
                Thread.currentThread().getName(),
                recordingId,
                replayChannelBase,
                replayDestination,
                destinationChannel());
        }

        @Override
        public Image recover(
            final Subscription subscription,
            final long position,
            final FragmentHandler fragmentHandler)
        {
            final String threadName = Thread.currentThread().getName();
            final long timeoutNs = connectionTimeoutNs();

            final int[] sessionIdHolder = new int[1];
            final int found = archive.listRecording(
                recordingId,
                (controlSessionId, correlationId, recordingId1, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                 mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                    sessionIdHolder[0] = sessionId);
            if (found == 0)
            {
                throw new IllegalStateException("Recording not found for recordingId=" + recordingId);
            }
            final int recordingSessionId = sessionIdHolder[0];
            final String replayChannel = new ChannelUriStringBuilder(replayChannelBase)
                .sessionId(recordingSessionId)
                .build();

            System.out.printf(
                "[%s] ReplayMerge: initiating from recordingId=%d, recordingSessionId=%d at position=%d, timeoutMs=%,d%n",
                threadName, recordingId, recordingSessionId, position, timeoutNs / 1_000_000);
            System.out.printf(
                "[%s] ReplayMerge:   replayChannel=%s, replayDestination=%s, liveDestination=%s%n",
                threadName, replayChannel, replayDestination, destinationChannel());

            try (ReplayMerge replayMerge = new ReplayMerge(
                mergeSubscription,
                archive,
                replayChannel,
                replayDestination,
                destinationChannel(),
                recordingId,
                position))
            {
                final long[] counts = new long[2]; // [0]=archiveFragments, [1]=liveFragments
                final Image merged = pollUntilMerged(
                    replayMerge, fragmentHandler, threadName, timeoutNs, counts);
                lastArchiveFragments = counts[0];
                lastLiveFragments = counts[1];
                return merged;
            }
        }

        private static Image pollUntilMerged(
            final ReplayMerge replayMerge,
            final FragmentHandler fragmentHandler,
            final String threadName,
            final long timeoutNs,
            final long[] countsOut)
        {
            final long deadlineNs = SystemNanoClock.INSTANCE.nanoTime() + timeoutNs;
            final long logIntervalNs = 1_000_000_000L;
            long lastLogNs = SystemNanoClock.INSTANCE.nanoTime();
            long pollIterations = 0;
            long archiveFragments = 0;
            long liveFragments = 0;
            boolean inArchivePhase = true;
            boolean imageAcquiredLogged = false;
            boolean liveAddedLogged = false;

            System.out.printf("[%s] ReplayMerge: poll loop started%n", threadName);

            while (!replayMerge.isMerged())
            {
                final int fragments = replayMerge.poll(fragmentHandler, FRAGMENT_LIMIT);
                pollIterations++;

                if (inArchivePhase)
                {
                    archiveFragments += fragments;
                }
                else
                {
                    liveFragments += fragments;
                }

                imageAcquiredLogged = logImageAcquiredOnce(
                    replayMerge, threadName, pollIterations, imageAcquiredLogged);

                if (!liveAddedLogged && replayMerge.isLiveAdded())
                {
                    final Image img = replayMerge.image();
                    System.out.printf(
                        "[%s] ReplayMerge: live destination ADDED — within merge window. " +
                            "archiveFragments=%,d, imagePosition=%d, iterations=%,d%n",
                        threadName,
                        archiveFragments,
                        img != null ? img.position() : -1L,
                        pollIterations);
                    inArchivePhase = false;
                    liveAddedLogged = true;
                }

                if (0 == fragments)
                {
                    final long nowNs = SystemNanoClock.INSTANCE.nanoTime();
                    checkFailed(replayMerge, threadName, pollIterations,
                        archiveFragments, liveFragments, imageAcquiredLogged, liveAddedLogged);
                    checkTimeout(replayMerge, threadName, timeoutNs, pollIterations,
                        archiveFragments, liveFragments, imageAcquiredLogged, liveAddedLogged,
                        nowNs, deadlineNs);

                    if (nowNs - lastLogNs >= logIntervalNs)
                    {
                        logProgress(replayMerge, threadName, pollIterations,
                            archiveFragments, liveFragments, imageAcquiredLogged, liveAddedLogged,
                            deadlineNs, nowNs);
                        lastLogNs = nowNs;
                    }
                }
            }

            final Image mergedImage = replayMerge.image();
            countsOut[0] = archiveFragments;
            countsOut[1] = liveFragments;

            System.out.printf(
                "[%s] ReplayMerge: MERGED successfully. " +
                    "newImage sessionId=%d, position=%d, " +
                    "archiveFragments=%,d, liveFragments=%,d, iterations=%,d%n",
                threadName,
                mergedImage.sessionId(),
                mergedImage.position(),
                archiveFragments,
                liveFragments,
                pollIterations);

            return mergedImage;
        }

        private static boolean logImageAcquiredOnce(
            final ReplayMerge replayMerge,
            final String threadName,
            final long pollIterations,
            final boolean alreadyLogged)
        {
            if (!alreadyLogged && replayMerge.image() != null)
            {
                final Image img = replayMerge.image();
                System.out.printf(
                    "[%s] ReplayMerge: replay image ACQUIRED — archive replay stream is flowing. " +
                        "sessionId=%d, position=%d, iterations=%,d%n",
                    threadName, img.sessionId(), img.position(), pollIterations);
                return true;
            }
            return alreadyLogged;
        }

        private static void checkFailed(
            final ReplayMerge replayMerge,
            final String threadName,
            final long pollIterations,
            final long archiveFragments,
            final long liveFragments,
            final boolean imageAcquiredLogged,
            final boolean liveAddedLogged)
        {
            if (replayMerge.hasFailed())
            {
                System.out.printf(
                    "[%s] ReplayMerge: FAILED. iterations=%,d, " +
                        "archiveFragments=%,d, liveFragments=%,d, " +
                        "imageAcquired=%b, liveAdded=%b%n",
                    threadName, pollIterations,
                    archiveFragments, liveFragments,
                    imageAcquiredLogged, liveAddedLogged);
                throw new IllegalStateException("ReplayMerge hasFailed()");
            }
        }

        private static void checkTimeout(
            final ReplayMerge replayMerge,
            final String threadName,
            final long timeoutNs,
            final long pollIterations,
            final long archiveFragments,
            final long liveFragments,
            final boolean imageAcquiredLogged,
            final boolean liveAddedLogged,
            final long nowNs,
            final long deadlineNs)
        {
            if (nowNs > deadlineNs)
            {
                System.out.printf(
                    "[%s] ReplayMerge: TIMED OUT after %,d ms. iterations=%,d, " +
                        "archiveFragments=%,d, liveFragments=%,d, " +
                        "imageAcquired=%b, liveAdded=%b%n",
                    threadName, timeoutNs / 1_000_000, pollIterations,
                    archiveFragments, liveFragments,
                    imageAcquiredLogged, liveAddedLogged);
                throw new IllegalStateException(
                    "Timed out waiting for replay merge after " + timeoutNs / 1_000_000 + " ms");
            }
        }

        private static void logProgress(
            final ReplayMerge replayMerge,
            final String threadName,
            final long pollIterations,
            final long archiveFragments,
            final long liveFragments,
            final boolean imageAcquiredLogged,
            final boolean liveAddedLogged,
            final long deadlineNs,
            final long nowNs)
        {
            final Image img = replayMerge.image();
            System.out.printf(
                "[%s] ReplayMerge: waiting... imageAcquired=%b, liveAdded=%b, " +
                    "imagePosition=%d, archiveFragments=%,d, liveFragments=%,d, " +
                    "iterations=%,d, remainingMs=%,d%n%s%n",
                threadName,
                imageAcquiredLogged,
                liveAddedLogged,
                img != null ? img.position() : -1L,
                archiveFragments,
                liveFragments,
                pollIterations,
                (deadlineNs - nowNs) / 1_000_000,
                replayMerge);
        }

        @Override
        public void close()
        {
            System.out.printf("[%s] ReplayMergeRecoveryStrategy: closing archive connection%n",
                Thread.currentThread().getName());
            closeAll(mergeSubscription);
            archive.close();
        }
    }

    // -------------------------------------------------------------------------
    // Strategy: replay merge v2 (in development)
    // -------------------------------------------------------------------------

    static final class ReplayMergeV2RecoveryStrategy implements RecoveryStrategy
    {
        ReplayMergeV2RecoveryStrategy()
        {
            // TODO: initialise archive connection and properties once v2 API is available
        }

        @Override
        public Image recover(
            final Subscription subscription,
            final long position,
            final FragmentHandler fragmentHandler)
        {
            // TODO: replace with ReplayMergeV2 API once available
            throw new UnsupportedOperationException("ReplayMerge v2 API not yet available");
        }

        @Override
        public void close()
        {
        }
    }

    // -------------------------------------------------------------------------
    // Node
    // -------------------------------------------------------------------------

    private final BufferClaim bufferClaim = new BufferClaim();
    private final FragmentHandler fragmentHandler;
    private final ExclusivePublication publication;
    private final Subscription subscription;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;
    private final long pauseNs;
    private final long pauseEveryNs;
    private final RecoveryStrategy recoveryStrategy;

    StallingEchoNode(final AtomicBoolean running)
    {
        this(running, launchEmbeddedMediaDriverIfConfigured(), connect(), true, receiverIndex());
    }

    StallingEchoNode(
        final AtomicBoolean running,
        final MediaDriver mediaDriver,
        final Aeron aeron,
        final boolean ownsAeronClient,
        final int receiverIndex)
    {
        this.running = running;
        this.mediaDriver = mediaDriver;
        this.aeron = aeron;
        this.ownsAeronClient = ownsAeronClient;

        this.pauseNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong(PAUSE_MS_PROP, 0));
        this.pauseEveryNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong(PAUSE_EVERY_MS_PROP, 0));

        final RecoveryMode recoveryMode = RecoveryMode.valueOf(
            System.getProperty(RECOVERY_MODE_PROP, RecoveryMode.GAP.name()));
        this.recoveryStrategy = createRecoveryStrategy(recoveryMode, aeron);

        System.out.println("pauseMs: " + Long.getLong(PAUSE_MS_PROP, 0));
        System.out.println("pauseEveryMs: " + Long.getLong(PAUSE_EVERY_MS_PROP, 0));
        System.out.println("recoveryMode: " + recoveryMode);
        System.out.println("EchoNode.init(receiverIndex=" + receiverIndex + ")");
        System.out.println("  publication (source):       channel=" +
            sourceChannel() + " stream=" + sourceStreamId());
        System.out.println("  subscription (destination): channel=" +
            destinationChannel() + " stream=" + destinationStreamId());

        publication = aeron.addExclusivePublication(sourceChannel(), sourceStreamId());
        System.out.println("  publication created: sessionId=" + publication.sessionId());

        subscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
        System.out.println("  subscription created");

        fragmentHandler = (buffer, offset, length, header) ->
        {
            if (buffer.getInt(offset + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN) == receiverIndex)
            {
                long result;
                while ((result = publication.tryClaim(length, bufferClaim)) <= 0)
                {
                    checkPublicationResult(result);
                }

                bufferClaim
                    .flags(header.flags())
                    .putBytes(buffer, offset, length)
                    .commit();
            }
        };
    }

    private static RecoveryStrategy createRecoveryStrategy(final RecoveryMode mode, final Aeron aeron)
    {
        switch (mode)
        {
            case GAP:
                return new GapAcceptanceRecoveryStrategy();
            case REPLAY_MERGE:
                return new ReplayMergeRecoveryStrategy(aeron);
            case REPLAY_MERGE_V2:
                return new ReplayMergeV2RecoveryStrategy();
            default:
                throw new IllegalArgumentException("Unknown recovery mode: " + mode);
        }
    }

    public void run()
    {
        awaitConnected(
            () -> subscription.isConnected() && publication.availableWindow() > 0,
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        final String threadName = Thread.currentThread().getName();
        final IdleStrategy idleStrategy = idleStrategy();
        final AtomicBoolean running = this.running;
        final long pauseNs = this.pauseNs;
        final long pauseEveryNs = this.pauseEveryNs;
        final boolean stallingEnabled = pauseNs > 0 && pauseEveryNs > 0;

        Image image = subscription.imageAtIndex(0);
        System.out.printf(
            "[%s] Image acquired: sessionId=%d, position=%d, mtu=%d, correlationId=%d%n",
            threadName, image.sessionId(), image.position(), image.mtuLength(), image.correlationId());

        long totalMessagesEchoed = 0;
        long liveMessagesThisRun = 0;
        long totalLiveMessages = 0;
        long totalArchiveMessages = 0;
        long stallCount = 0;
        int recoveryAttempts = 0;
        long nextStallDeadlineNs = stallingEnabled ? System.nanoTime() + pauseEveryNs : Long.MAX_VALUE;

        while (true)
        {
            final int fragments = image.poll(fragmentHandler, FRAGMENT_LIMIT);

            if (fragments > 0)
            {
                totalMessagesEchoed += fragments;
                liveMessagesThisRun += fragments;
                if (stallingEnabled && System.nanoTime() >= nextStallDeadlineNs)
                {
                    stallCount++;
                    nextStallDeadlineNs = doStall(
                        threadName, stallCount, pauseNs, totalMessagesEchoed, image.position());
                }
            }
            else
            {
                if (!running.get())
                {
                    totalLiveMessages += liveMessagesThisRun;
                    System.out.printf(
                        "[%s] Shutdown signal received. Exiting. " +
                            "totalMessagesEchoed=%,d, liveMessages=%,d, archiveMessages=%,d, " +
                            "stallCount=%,d, recoveryAttempts=%d%n",
                        threadName, totalMessagesEchoed, totalLiveMessages, totalArchiveMessages,
                        stallCount, recoveryAttempts);
                    return;
                }

                if (image.isClosed())
                {
                    totalLiveMessages += liveMessagesThisRun;
                    System.out.printf(
                        "[%s] Live phase ending: liveMessagesThisRun=%,d, " +
                            "totalLive=%,d, totalArchive=%,d, total=%,d%n",
                        threadName, liveMessagesThisRun,
                        totalLiveMessages, totalArchiveMessages, totalMessagesEchoed);
                    liveMessagesThisRun = 0;

                    image = doRecovery(image, threadName, totalMessagesEchoed, stallCount,
                        ++recoveryAttempts, totalLiveMessages, totalArchiveMessages);
                    if (null == image)
                    {
                        return;
                    }

                    if (recoveryStrategy instanceof ReplayMergeRecoveryStrategy)
                    {
                        final ReplayMergeRecoveryStrategy rm =
                            (ReplayMergeRecoveryStrategy) recoveryStrategy;
                        final long archiveDelta = rm.lastArchiveFragments;
                        final long liveDelta = rm.lastLiveFragments;
                        totalArchiveMessages += archiveDelta;
                        totalMessagesEchoed += archiveDelta + liveDelta;
                        System.out.printf(
                            "[%s] Merge #%d counters: archiveDelta=%,d, liveDelta=%,d, " +
                                "totalArchive=%,d, totalLive=%,d, total=%,d%n",
                            threadName, recoveryAttempts,
                            archiveDelta, liveDelta,
                            totalArchiveMessages, totalLiveMessages, totalMessagesEchoed);
                    }

                    nextStallDeadlineNs = System.nanoTime() + pauseEveryNs;
                }
            }

            idleStrategy.idle(fragments);
        }
    }

    private long doStall(
        final String threadName,
        final long stallCount,
        final long pauseNs,
        final long totalMessagesEchoed,
        final long imagePosition)
    {
        System.out.printf(
            "[%s] Stall #%,d triggered: pausing for %,d ms, " +
                "totalMessagesEchoed=%,d, imagePosition=%d%n",
            threadName, stallCount, pauseNs / 1_000_000, totalMessagesEchoed, imagePosition);

        final long stallDeadlineNs = System.nanoTime() + pauseNs;
        while (System.nanoTime() < stallDeadlineNs)
        {
            Thread.onSpinWait();
        }

        System.out.printf("[%s] Stall #%,d complete, resuming poll%n", threadName, stallCount);

        // Return the next stall deadline, reset from end of stall so intervals don't stack
        return System.nanoTime() + pauseEveryNs;
    }

    private Image doRecovery(
        final Image lostImage,
        final String threadName,
        final long totalMessagesEchoed,
        final long stallCount,
        final int recoveryAttempt,
        final long totalLiveMessages,
        final long totalArchiveMessages)
    {
        final long lostPosition = lostImage.position();
        System.out.printf(
            "[%s] Image CLOSED (lost): sessionId=%d, position=%d, " +
                "totalMessagesEchoed=%,d, stallCount=%,d, recoveryAttempt=%d%n",
            threadName, lostImage.sessionId(), lostPosition,
            totalMessagesEchoed, stallCount, recoveryAttempt);
        System.out.printf("[%s] Attempting recovery via: %s%n",
            threadName, recoveryStrategy.getClass().getSimpleName());

        final long recoveryStartNs = System.nanoTime();
        final Image newImage = recoveryStrategy.recover(subscription, lostPosition, fragmentHandler);
        final long recoveryElapsedMs = (System.nanoTime() - recoveryStartNs) / 1_000_000;

        if (null == newImage)
        {
            System.out.printf(
                "[%s] Recovery #%d returned null — no image available, exiting. " +
                    "elapsedMs=%,d, totalMessagesEchoed=%,d%n",
                threadName, recoveryAttempt, recoveryElapsedMs, totalMessagesEchoed);
            return null;
        }

        System.out.printf(
            "[%s] Recovery #%d SUCCESSFUL: newImage sessionId=%d, " +
                "position=%d, gapBytes=%,d, elapsedMs=%,d%n",
            threadName, recoveryAttempt, newImage.sessionId(),
            newImage.position(), newImage.position() - lostPosition, recoveryElapsedMs);

        return newImage;
    }

    public void close()
    {
        System.out.printf("[%s] StallingEchoNode: closing%n", Thread.currentThread().getName());
        closeAll(recoveryStrategy);
        closeAll(subscription);
        closeAll(publication);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
        System.out.printf("[%s] StallingEchoNode: closed%n", Thread.currentThread().getName());
    }

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir = Configuration.resolveLogsDir();
        final int receiverIndex = AeronUtil.receiverIndex();

        final AtomicBoolean running = new AtomicBoolean(true);
        try (
            ShutdownSignalBarrier shutdownSignalBarrier = new ShutdownSignalBarrier(() -> running.set(false));
            StallingEchoNode node = new StallingEchoNode(running))
        {
            Thread.currentThread().setName("echo-" + receiverIndex);

            node.run();

            final String prefix = "echo-node-" + receiverIndex + "-";
            AeronUtil.dumpAeronStats(
                node.aeron.context().cncFile(),
                outputDir.resolve(prefix + "aeron-stat.txt"),
                outputDir.resolve(prefix + "errors.txt"));
        }
    }
}