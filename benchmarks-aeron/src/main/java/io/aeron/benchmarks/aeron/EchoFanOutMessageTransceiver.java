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
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.HdrHistogram.ValueRecorder;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;
import io.aeron.benchmarks.PersistedHistogramSet;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.CloseHelper.closeAll;
import static io.aeron.benchmarks.aeron.AeronUtil.*;
import static io.aeron.benchmarks.aeron.RecoveringEchoNode.ARCHIVE_CONTROL_CHANNEL_PROP;
import static io.aeron.benchmarks.aeron.RecoveringEchoNode.ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP;
import static io.aeron.benchmarks.aeron.RecoveringEchoNode.ARCHIVE_CONTROL_STREAM_PROP;
import static io.aeron.benchmarks.aeron.RecoveringEchoNode.PROCESSING_TIME_OFFSET;

/**
 * Message transceiver for fan-out benchmarks: one publication, N subscriptions.
 * <p>
 * Sends each message once on a single publication. N echo nodes subscribe to that
 * publication and each echoes back on a dedicated reply channel. This transceiver
 * subscribes to all N reply channels and records per-receiver latency histograms
 * via {@link PersistedHistogramSet}.
 * <p>
 * Every message carries a processing time (nanoseconds) that the echo node spins
 * for after echoing. The {@link ControlStrategy} writes a normal processing time on
 * most messages, and periodically writes a large stall value to a target receiver
 * to trigger image loss and exercise replay-merge recovery under load.
 * <p>
 * If archive control channel properties are configured, starts a recording on the
 * outbound publication channel via {@code aeron-spy}.
 *
 * <h2>Wire layout</h2>
 * <pre>
 *   [TIMESTAMP_OFFSET..+7]         timestamp (long, little-endian)
 *   [RECEIVER_INDEX_OFFSET..+3]    receiver index (int, little-endian)
 *   [PROCESSING_TIME_OFFSET..+7]   processing time (long, nanoseconds, little-endian)
 *   [messageLength-8..end]         checksum (long, little-endian)
 * </pre>
 */
public final class EchoFanOutMessageTransceiver extends MessageTransceiver
{
    static final int MIN_MESSAGE_LENGTH = PROCESSING_TIME_OFFSET + SIZE_OF_LONG + SIZE_OF_LONG;

    // -------------------------------------------------------------------------
    // ControlStrategy
    // -------------------------------------------------------------------------

    /**
     * Owns all per-message decisions: which receiver to target and what processing time to encode.
     * <p>
     * System properties:
     * <ul>
     *   <li>{@code recovering.echo.control.processing.time.ns} — normal per-message processing time
     *                                                             in nanoseconds; 0 = no spin</li>
     *   <li>{@code recovering.echo.control.interval.us}         — how often to send a stall message,
     *                                                             in microseconds; 0 = disabled</li>
     *   <li>{@code recovering.echo.control.stall.ns}            — stall processing time in nanoseconds,
     *                                                             written to the target receiver periodically</li>
     *   <li>{@code recovering.echo.control.targets}            — comma-separated receiver indices to cycle
     *                                                            through for stalls; default = all receivers</li>
     * </ul>
     * Normal receiver selection is always round-robin across all receivers.
     * Each time a stall fires, one receiver from the target subset gets the stall processing time,
     * advancing the subset round-robin index for the next trigger.
     */
    static final class ControlStrategy
    {
        static final String PROCESSING_TIME_NS_PROP = "recovering.echo.control.processing.time.ns";
        static final String INTERVAL_US_PROP        = "recovering.echo.control.interval.us";
        static final String STALL_NS_PROP           = "recovering.echo.control.stall.ns";
        static final String TARGETS_PROP            = "recovering.echo.control.targets";

        private final long  normalProcessingTimeNs;
        private final long  intervalNs;
        private final long  stallNs;
        private final int[] targets;
        private final int   receiverCount;

        private int     normalRoundRobin = 0;  // advances every message
        private int     targetRoundRobin = 0;  // advances every stall trigger
        private long    nextStallNs      = Long.MAX_VALUE;
        private boolean stallPending     = false;
        private int     stallReceiver    = 0;

        ControlStrategy(final int receiverCount)
        {
            this.receiverCount          = receiverCount;
            this.normalProcessingTimeNs = Long.getLong(PROCESSING_TIME_NS_PROP, 0);
            this.intervalNs             = Long.getLong(INTERVAL_US_PROP, 0) * 1_000;
            this.stallNs                = Long.getLong(STALL_NS_PROP, 0);

            final String targetsProp = System.getProperty(TARGETS_PROP, "");
            if (targetsProp.isEmpty())
            {
                this.targets = new int[receiverCount];
                for (int i = 0; i < receiverCount; i++)
                {
                    this.targets[i] = i;
                }
            }
            else
            {
                final String[] parts = targetsProp.split(",");
                this.targets = new int[parts.length];
                for (int i = 0; i < parts.length; i++)
                {
                    this.targets[i] = Integer.parseInt(parts[i].trim());
                }
            }

            System.out.println("ControlStrategy:");
            System.out.println("  normalProcessingTimeNs: " + normalProcessingTimeNs);
            System.out.println("  intervalUs:             " + intervalNs / 1_000);
            System.out.println("  stallNs:                " + stallNs);
            System.out.println("  targets:                " + java.util.Arrays.toString(targets));
        }

        boolean isStallEnabled()
        {
            return intervalNs > 0 && stallNs > 0;
        }

        void start(final long nowNs)
        {
            if (isStallEnabled())
            {
                nextStallNs = nowNs + intervalNs;
            }
        }

        void reset(final long nowNs)
        {
            normalRoundRobin = 0;
            targetRoundRobin = 0;
            nextStallNs      = nowNs + intervalNs;
            stallPending     = false;
        }

        /**
         * Returns the receiver index for the next message.
         * Must be called exactly once per message, before {@link #write}.
         *
         * @param nowNs current time in nanoseconds from the transceiver clock
         */
        int nextReceiver(final long nowNs)
        {
            if (isStallEnabled() && !stallPending && nowNs >= nextStallNs)
            {
                stallPending     = true;
                stallReceiver    = targets[targetRoundRobin];
                targetRoundRobin = BitUtil.next(targetRoundRobin, targets.length);
                nextStallNs     += intervalNs;
            }

            final int receiver = stallPending ? stallReceiver : normalRoundRobin;
            normalRoundRobin = BitUtil.next(normalRoundRobin, receiverCount);
            return receiver;
        }

        /**
         * Writes all message fields. Encodes the stall processing time for the target receiver
         * when a stall is pending, otherwise the normal processing time.
         */
        void write(
            final MutableDirectBuffer buffer,
            final int offset,
            final int messageLength,
            final long timestamp,
            final long checksum,
            final int receiver)
        {
            buffer.putLong(offset + TIMESTAMP_OFFSET, timestamp, LITTLE_ENDIAN);
            buffer.putInt(offset + RECEIVER_INDEX_OFFSET, receiver, LITTLE_ENDIAN);

            if (stallPending && receiver == stallReceiver)
            {
                buffer.putLong(offset + PROCESSING_TIME_OFFSET, stallNs, LITTLE_ENDIAN);
                stallPending = false;
            }
            else
            {
                buffer.putLong(offset + PROCESSING_TIME_OFFSET, normalProcessingTimeNs, LITTLE_ENDIAN);
            }

            buffer.putLong(offset + messageLength - SIZE_OF_LONG, checksum, LITTLE_ENDIAN);
        }
    }

    // -------------------------------------------------------------------------
    // Transceiver fields
    // -------------------------------------------------------------------------

    private final BufferClaim bufferClaim = new BufferClaim();
    private final PersistedHistogramSet histogramSet;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;

    private Path logsDir;
    private ExclusivePublication publication;
    private Subscription[] subscriptions;
    private FragmentHandler[] fragmentHandlers;
    private int receiverCount;
    private ControlStrategy strategy;
    private AeronArchive aeronArchive;
    private long recordingSubscriptionId = -1;

    public EchoFanOutMessageTransceiver(final NanoClock nanoClock, final PersistedHistogramSet histogramSet)
    {
        this(nanoClock, histogramSet, launchEmbeddedMediaDriverIfConfigured(), connect(), true);
    }

    EchoFanOutMessageTransceiver(
        final NanoClock nanoClock,
        final PersistedHistogramSet histogramSet,
        final MediaDriver mediaDriver,
        final Aeron aeron,
        final boolean ownsAeronClient)
    {
        super(nanoClock, new DummyValueRecorder());
        this.histogramSet    = histogramSet;
        this.mediaDriver     = mediaDriver;
        this.aeron           = aeron;
        this.ownsAeronClient = ownsAeronClient;
    }

    public static class DummyValueRecorder implements ValueRecorder
    {
        @Override
        public void recordValue(final long value) throws ArrayIndexOutOfBoundsException {}

        @Override
        public void recordValueWithCount(final long value, final long count)
            throws ArrayIndexOutOfBoundsException {}

        @Override
        public void recordValueWithExpectedInterval(
            final long value, final long expectedIntervalBetweenValueSamples)
            throws ArrayIndexOutOfBoundsException {}

        @Override
        public void reset() {}
    }

    public void init(final Configuration configuration)
    {
        logsDir = configuration.logsDir();
        final int messageLength = configuration.messageLength();

        if (messageLength < MIN_MESSAGE_LENGTH)
        {
            throw new IllegalArgumentException(
                "messageLength=" + messageLength + " too small; minimum=" + MIN_MESSAGE_LENGTH);
        }

        validateMessageLength(messageLength);

        System.out.println("EchoFanOutMessageTransceiver.init()");
        System.out.println("  creating publication: channel=" + destinationChannel() +
            " stream=" + destinationStreamId());
        publication = aeron.addExclusivePublication(destinationChannel(), destinationStreamId());
        System.out.println("  publication created: sessionId=" + publication.sessionId());

        startArchiveRecording();

        final String[] srcChannels = sourceChannels();
        final int[] srcStreams     = sourceStreams();
        assertChannelsAndStreamsMatch(srcChannels, srcStreams, SOURCE_CHANNELS_PROP_NAME, SOURCE_STREAMS_PROP_NAME);

        receiverCount    = srcChannels.length;
        subscriptions    = new Subscription[receiverCount];
        fragmentHandlers = new FragmentHandler[receiverCount];
        System.out.println("  numReceivers: " + receiverCount);

        for (int i = 0; i < receiverCount; i++)
        {
            System.out.println("  creating subscription[" + i + "]: channel=" +
                srcChannels[i] + " stream=" + srcStreams[i]);
            subscriptions[i] = aeron.addSubscription(srcChannels[i], srcStreams[i]);
            System.out.println("  subscription[" + i + "] created");

            final String name = "receiver-" + i;
            final ValueRecorder recorder = histogramSet.create(name).valueRecorder();
            System.out.println("  value recorder created: " + name);

            fragmentHandlers[i] = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    final long timestamp = buffer.getLong(offset, LITTLE_ENDIAN);
                    final long now       = clock.nanoTime();
                    recorder.recordValue(now - timestamp);
                    receivedMessages++;
                });
        }

        long remainingConnectTimeoutNs = connectionTimeoutNs();

        System.out.println("  awaiting publication connection " +
            "(remaining " + remainingConnectTimeoutNs / 1_000_000 + "ms)...");
        long startNs = SystemNanoClock.INSTANCE.nanoTime();
        awaitConnected(
            () -> publication.isConnected() && publication.availableWindow() > 0,
            remainingConnectTimeoutNs,
            SystemNanoClock.INSTANCE);
        remainingConnectTimeoutNs -= SystemNanoClock.INSTANCE.nanoTime() - startNs;
        System.out.println("  publication connected (remaining " + remainingConnectTimeoutNs / 1_000_000 + "ms)");


        for (int i = 0; i < receiverCount; i++)
        {
            System.out.println("  awaiting subscription[" + i + "] " +
                "(remaining " + remainingConnectTimeoutNs / 1_000_000 + "ms): channel=" +
                subscriptions[i].channel() + " stream=" + subscriptions[i].streamId());
            startNs = SystemNanoClock.INSTANCE.nanoTime();
            final int idx = i;
            awaitConnected(
                () -> subscriptions[idx].isConnected(),
                remainingConnectTimeoutNs,
                SystemNanoClock.INSTANCE);
            remainingConnectTimeoutNs -= SystemNanoClock.INSTANCE.nanoTime() - startNs;
            System.out.println("  subscription[" + i + "] connected (remaining " +
                remainingConnectTimeoutNs / 1_000_000 + "ms)");
        }

        strategy = new ControlStrategy(receiverCount);
        strategy.start(SystemNanoClock.INSTANCE.nanoTime());

        System.out.println("  all connected");
    }

    private void startArchiveRecording()
    {
        final String controlChannel = System.getProperty(ARCHIVE_CONTROL_CHANNEL_PROP);
        if (null == controlChannel)
        {
            System.out.println("  archive control channel not configured, skipping recording");
            return;
        }

        final int controlStream      = Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0);
        final String responseChannel = System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP);

        System.out.printf("  connecting to archive: controlChannel=%s responseChannel=%s controlStream=%s%n",  controlChannel, responseChannel, controlStream);
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(controlChannel)
            .controlRequestStreamId(controlStream)
            .controlResponseChannel(responseChannel));

        final SourceLocation sourceLocation = sourceLocationForChannel(recordChannel());
        final int stream = destinationStreamId();

        System.out.println("  starting recording: channel=" + recordChannel() + " stream=" + stream);
        recordingSubscriptionId = aeronArchive.startRecording(recordChannel(), stream, sourceLocation);

        System.out.println("  awaiting active recording on channel=" + recordChannel() + " stream=" + stream + "...");
        awaitRecordingActive(recordChannel(), stream);
        System.out.println("  recording is active");
    }

    private void awaitRecordingActive(final String channel, final int stream)
    {
       awaitConnected(
           ()-> {
               final AtomicBoolean found = new AtomicBoolean(false);
               aeronArchive.listRecordingsForUri(
                   0,
                   1,
                   channel,
                   stream,
                   (controlSessionId, correlationId, recordingId,
                       startTimestamp, stopTimestamp, startPosition, stopPosition,
                       initialTermId, segmentFileLength, termBufferLength, mtuLength,
                       sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                       found.set(true));
               return found.get();
           },
           connectionTimeoutNs(),
           SystemNanoClock.INSTANCE
       );
    }

    public void destroy()
    {
        if (recordingSubscriptionId >= 0 && null != aeronArchive)
        {
            try
            {
                aeronArchive.stopRecording(recordingSubscriptionId);
                System.out.println("  recording stopped: subscriptionId=" + recordingSubscriptionId);
            }
            catch (final Exception ex)
            {
                System.err.println("  failed to stop recording: " + ex.getMessage());
            }
        }

        final String prefix = "fan-out-client-";
        dumpAeronStats(
            aeron.context().cncFile(),
            logsDir.resolve(prefix + "aeron-stat.txt"),
            logsDir.resolve(prefix + "errors.txt"));

        closeAll(aeronArchive);
        closeAll(subscriptions);
        closeAll(publication);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
    }

    public int send(final int numberOfMessages, final int messageLength, final long timestamp, final long checksum)
    {
        int count = 0;
        for (int i = 0; i < numberOfMessages; i++)
        {
            int retryCount = SEND_ATTEMPTS;
            long result;
            while ((result = publication.tryClaim(messageLength, bufferClaim)) < 0)
            {
                checkPublicationResult(result, idleStrategy());
                if (0 == --retryCount)
                {
                    return count;
                }
            }

            final int receiver = strategy.nextReceiver(clock.nanoTime());

            strategy.write(
                bufferClaim.buffer(),
                bufferClaim.offset(),
                messageLength,
                timestamp,
                checksum,
                receiver);

            bufferClaim.commit();
            count++;
        }

        return count;
    }

    public void receive()
    {
        for (int i = 0; i < receiverCount; i++)
        {
            subscriptions[i].poll(fragmentHandlers[i], FRAGMENT_LIMIT);
        }
    }

    public void reset()
    {
        super.reset();
        strategy.reset(SystemNanoClock.INSTANCE.nanoTime());
    }
}