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
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.benchmarks.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.RECEIVER_INDEX_OFFSET;

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
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which echoes original messages back to the sender.
 * <p>
 * Every message carries a processing time field (nanoseconds). After echoing,
 * the node spins for the specified duration. A large processing time causes the
 * node to fall behind, the term buffer to fill, and the image to close —
 * triggering the recovery path.
 * <p>
 * Wire layout:
 * <pre>
 *   [TIMESTAMP_OFFSET..+7]           timestamp (long, little-endian)
 *   [RECEIVER_INDEX_OFFSET..+3]      receiver index (int, little-endian)
 *   [PROCESSING_TIME_OFFSET..+7]     processing time (long, nanoseconds, little-endian)
 *   [messageLength-8..end]           checksum (long, little-endian)
 * </pre>
 * <p>
 * Recovery strategy is selected at construction time via {@code recovering.echo.recovery.mode}:
 *   GAP             - accept the gap, wait for a new image
 *   REPLAY_MERGE    - replay from archive and merge back to live
 *   PERSISTENT_SUBSCRIPTION - replay via PersistentSubscription
 */
public final class RecoveringEchoNode implements AutoCloseable, Runnable
{
    static final String RECOVERY_MODE_PROP                    = "recovering.echo.recovery.mode";
    static final String ARCHIVE_CONTROL_CHANNEL_PROP          = "recovering.echo.archive.control.channel";
    static final String ARCHIVE_CONTROL_STREAM_PROP           = "recovering.echo.archive.control.stream";
    static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP = "recovering.echo.archive.control.response.channel";
    static final String REPLAY_CHANNEL_PROP                   = "recovering.echo.replay.channel";
    static final String REPLAY_DESTINATION_PROP               = "recovering.echo.replay.destination";
    static final String RECORDING_ID_PROP                     = "recovering.echo.recording.id";

    /**
     * Offset of the processing time field (long, nanoseconds) within a message.
     * Immediately follows the receiver index int field.
     */
    static final int PROCESSING_TIME_OFFSET = RECEIVER_INDEX_OFFSET + SIZE_OF_INT;

    // -------------------------------------------------------------------------
    // Node fields
    // -------------------------------------------------------------------------

    private final BufferClaim bufferClaim = new BufferClaim();
    private final FragmentHandler fragmentHandler;
    private final ExclusivePublication publication;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;
    private final EchoState echoState;

    // -------------------------------------------------------------------------
    // Counters
    // -------------------------------------------------------------------------

    private long requestReceived = 0;
    private long responsesSend   = 0;

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

        final String recoveryMode = System.getProperty(RECOVERY_MODE_PROP, "GAP").toUpperCase();

        System.out.println("recoveryMode: " + recoveryMode);
        System.out.printf("%s.init(receiverIndex=%s)%n", RecoveringEchoNode.class.getSimpleName(), receiverIndex);
        System.out.println("  publication (source):       channel=" +
            sourceChannel() + " stream=" + sourceStreamId());
        System.out.println("  subscription (destination): channel=" +
            destinationChannel() + " stream=" + destinationStreamId());

        publication = aeron.addExclusivePublication(sourceChannel(), sourceStreamId());
        System.out.println("  publication created: sessionId=" + publication.sessionId());

       // Wait for publication connection and archive readiness, transceiver will not start subscription until the archive started recording
        System.out.println("  awaiting publication connected...");
        AeronUtil.awaitConnected(
            () -> publication.isConnected() && publication.availableWindow() > 0,
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);
        System.out.println("  publication connected");


        fragmentHandler = (buffer, offset, length, header) ->
        {
            if (buffer.getInt(offset + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN) != receiverIndex)
            {
                return;
            }

            requestReceived++;

            long result;
            while ((result = publication.tryClaim(length, bufferClaim)) <= 0)
            {
                checkPublicationResult(result, idleStrategy());
            }
            bufferClaim.flags(header.flags()).putBytes(buffer, offset, length).commit();
            responsesSend++;

            final long processingTimeNs = buffer.getLong(offset + PROCESSING_TIME_OFFSET, LITTLE_ENDIAN);
            if (processingTimeNs > 0)
            {
                final long spinStartNs = System.nanoTime();
                while (System.nanoTime() - spinStartNs < processingTimeNs)
                {
                    Thread.onSpinWait();
                }
            }
        };

        switch (recoveryMode)
        {
            case "GAP":
                echoState = new GapState(aeron, publication, fragmentHandler);
                break;
            case "REPLAY_MERGE":
                echoState = new ReplayMergeState(aeron, publication, fragmentHandler);
                break;
            case "PERSISTENT_SUBSCRIPTION":
                echoState = new PersistentSubscriptionState(aeron, fragmentHandler);
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery mode: " + recoveryMode);
        }

        System.out.println("  subscription created");
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    public void run()
    {
        echoState.awaitConnected();

        final IdleStrategy idle = idleStrategy();

        while (running.get())
        {
            idle.idle(echoState.poll());
        }
    }

    // -------------------------------------------------------------------------
    // Close
    // -------------------------------------------------------------------------

    public void close()
    {
        closeAll(echoState);
        closeAll(publication);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir     = Configuration.resolveLogsDir();
        final int  receiverIndex = AeronUtil.receiverIndex();

        Thread.currentThread().setName("echo");

        final AtomicBoolean running = new AtomicBoolean(true);
        try (
            ShutdownSignalBarrier shutdownSignalBarrier =
                new ShutdownSignalBarrier(() -> running.set(false));
            RecoveringEchoNode node = new RecoveringEchoNode(running))
        {

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

    // =========================================================================
    // EchoState — common interface
    // =========================================================================

    interface EchoState extends AutoCloseable
    {
        void awaitConnected();
        int poll();
        void close();
    }

    // =========================================================================
    // GapState
    // =========================================================================

    static final class GapState implements EchoState
    {
        private final Aeron aeron;
        private final ExclusivePublication publication;
        private final FragmentHandler fragmentHandler;

        private Subscription liveSubscription;
        private Image image;
        private boolean live = true;

        GapState(final Aeron aeron, final ExclusivePublication publication, final FragmentHandler fragmentHandler)
        {
            this.aeron            = aeron;
            this.publication      = publication;
            this.fragmentHandler  = fragmentHandler;
            this.liveSubscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
        }

        public void awaitConnected()
        {
            AeronUtil.awaitConnected(
                () -> liveSubscription.isConnected() && publication.availableWindow() > 0,
                connectionTimeoutNs(),
                SystemNanoClock.INSTANCE);
            image = liveSubscription.imageAtIndex(0);
        }

        public int poll()
        {
            return live ? doLive() : doRecovery();
        }

        private int doLive()
        {
            if (image.isClosed())
            {
                closeAll(liveSubscription);
                liveSubscription = null;
                live = false;
                return 1;
            }

            return image.poll(fragmentHandler, FRAGMENT_LIMIT);
        }

        private int doRecovery()
        {
            if (liveSubscription == null)
            {
                liveSubscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
            }

            if (liveSubscription.imageCount() > 0)
            {
                image = liveSubscription.imageAtIndex(0);
                live  = true;
                return 1;
            }

            return 0;
        }

        public void close()
        {
            closeAll(liveSubscription);
        }
    }

    // =========================================================================
    // ReplayMergeState
    // =========================================================================

    static final class ReplayMergeState implements EchoState
    {
        private final Aeron aeron;
        private final ExclusivePublication publication;
        private final FragmentHandler fragmentHandler;
        private final AeronArchive aeronArchive;
        private final String replayChannelBase;
        private final String replayDestination;
        private final long recordingId;

        private Subscription liveSubscription;
        private Subscription mergeSubscription;
        private ReplayMerge  replayMerge;
        private Image image;
        private boolean live = true;

        private long lostPosition;
        private long recoveryStartNs;
        private long recoveryDeadlineNs      = Long.MAX_VALUE;
        private long recoveryArchiveFragments;
        private long recoveryLiveFragments;
        private boolean recoveryInArchivePhase;
        private int recoveryAttempts;

        ReplayMergeState(final Aeron aeron, final ExclusivePublication publication, final FragmentHandler fragmentHandler)
        {
            this.aeron             = aeron;
            this.publication       = publication;
            this.fragmentHandler   = fragmentHandler;
            this.recordingId       = Long.getLong(RECORDING_ID_PROP, 0);
            this.replayChannelBase = System.getProperty(REPLAY_CHANNEL_PROP);
            this.replayDestination = System.getProperty(REPLAY_DESTINATION_PROP);

            System.out.printf("%s connecting to archive: controlChannel=%s, controlStream=%s, " +
                    "controlResponseChannel=%s%n",
                ReplayMergeState.class.getSimpleName(),
                System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP),
                System.getProperty(ARCHIVE_CONTROL_STREAM_PROP),
                System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP));

            this.aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP))
                .controlRequestStreamId(Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0))
                .controlResponseChannel(System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP)));

            System.out.printf("  archive connected. recordingId=%d, replayChannelBase=%s, " +
                    "replayDestination=%s, liveDestination=%s%n",
                recordingId, replayChannelBase, replayDestination, destinationChannel());

            this.liveSubscription = aeron.addSubscription(destinationChannel(), destinationStreamId());
        }

        public void awaitConnected()
        {
            AeronUtil.awaitConnected(
                () -> liveSubscription.isConnected() && publication.availableWindow() > 0,
                connectionTimeoutNs(),
                SystemNanoClock.INSTANCE);
            image = liveSubscription.imageAtIndex(0);
        }

        public int poll()
        {
            return live ? doLive() : doRecovery();
        }

        private int doLive()
        {
            if (image.isClosed())
            {
                lostPosition = image.position();
                closeAll(liveSubscription);
                liveSubscription = null;
                live = false;
                return 1;
            }

            return image.poll(fragmentHandler, FRAGMENT_LIMIT);
        }

        private int doRecovery()
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
                recoveryStartNs          = System.nanoTime();
                recoveryDeadlineNs       = recoveryStartNs + connectionTimeoutNs();
                recoveryArchiveFragments = 0;
                recoveryLiveFragments    = 0;
                recoveryInArchivePhase   = true;

                final long archivePosition = aeronArchive.getRecordingPosition(recordingId);

                if (archivePosition <= lostPosition)
                {
                    recoveryStartNs    = 0;
                    recoveryDeadlineNs = Long.MAX_VALUE;
                    return 0;
                }

                recoveryAttempts++;

                final int[] sessionIdHolder = new int[1];
                final int found = aeronArchive.listRecording(
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

            if (recoveryInArchivePhase && replayMerge.isLiveAdded())
            {
                recoveryInArchivePhase = false;
            }

            if (replayMerge.isMerged())
            {
                replayMerge.close();
                replayMerge      = null;

                image             = mergeSubscription.imageAtIndex(0);
                liveSubscription  = mergeSubscription;
                mergeSubscription = null;
                live              = true;
                return 1;
            }

            return fragments;
        }

        public void close()
        {
            if (replayMerge != null)
            {
                replayMerge.close();
                replayMerge = null;
            }
            closeAll(mergeSubscription, aeronArchive, liveSubscription);
        }
    }

    // =========================================================================
    // PersistentSubscriptionState
    // =========================================================================

    static final class PersistentSubscriptionState implements EchoState
    {
        private final PersistentSubscription persistentSubscription;
        private final ControlledFragmentHandler controlledFragmentHandler;

        PersistentSubscriptionState(final Aeron aeron, final FragmentHandler fragmentHandler)
        {
            this.controlledFragmentHandler = (buffer, offset, length, header) ->
            {
                fragmentHandler.onFragment(buffer, offset, length, header);
                return ControlledFragmentHandler.Action.CONTINUE;
            };

            final long recordingId = Long.getLong(RECORDING_ID_PROP, 0);

            System.out.printf("%s connecting to archive: controlChannel=%s, controlStream=%s, " +
                    "controlResponseChannel=%s%n",
                PersistentSubscriptionState.class.getSimpleName(),
                System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP),
                System.getProperty(ARCHIVE_CONTROL_STREAM_PROP),
                System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP));

            final AeronArchive.Context archiveContext = new AeronArchive.Context()
                .controlRequestChannel(System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP))
                .controlRequestStreamId(Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0))
                .controlResponseChannel(System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP))
                .aeron(aeron);

            persistentSubscription = PersistentSubscription.create(
                new PersistentSubscription.Context()
                    .recordingId(recordingId)
                    .startPosition(0)
                    .replayStreamId(-5)
                    .listener(new PersistentSubscriptionListener()
                {
                    @Override
                    public void onLive()
                    {
                        System.out.println("onLive");
                    }

                    @Override
                    public void onError(Exception e)
                    {
                        e.printStackTrace();
                    }
                })
                    .replayChannel(System.getProperty(REPLAY_CHANNEL_PROP))
                    .liveChannel(destinationChannel())
                    .liveStreamId(destinationStreamId())
                    .aeronArchiveContext(archiveContext));

            System.out.printf("  PersistentSubscription created. recordingId=%d, liveChannel=%s, replayChannel=%s%n",
                recordingId, destinationChannel(), System.getProperty(REPLAY_CHANNEL_PROP));
        }

        public void awaitConnected()
        {
            // PersistentSubscription manages its own connection lifecycle internally.
        }

        public int poll()
        {
            return persistentSubscription.controlledPoll(controlledFragmentHandler, FRAGMENT_LIMIT);
        }

        public void close()
        {
            closeAll(persistentSubscription);
        }
    }
}