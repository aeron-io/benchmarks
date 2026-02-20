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
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;
import io.aeron.benchmarks.PersistedHistogramSet;

import java.nio.file.Path;

import static io.aeron.Aeron.connect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.CloseHelper.closeAll;
import static io.aeron.benchmarks.aeron.AeronUtil.*;
import static io.aeron.benchmarks.aeron.StallingEchoNode.ARCHIVE_CONTROL_CHANNEL_PROP;
import static io.aeron.benchmarks.aeron.StallingEchoNode.ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP;
import static io.aeron.benchmarks.aeron.StallingEchoNode.ARCHIVE_CONTROL_STREAM_PROP;

/**
 * Message transceiver for fan-out benchmarks: one publication, N subscriptions.
 * <p>
 * Sends each message once on a single publication. N echo nodes subscribe to that
 * publication and each echoes back on a dedicated reply channel. This transceiver
 * subscribes to all N reply channels and records per-receiver latency histograms
 * via {@link PersistedHistogramSet}.
 * <p>
 * If archive control channel properties are configured, starts a recording on the
 * outbound publication channel via {@code aeron-spy} so the archive captures all
 * traffic for replay-merge recovery scenarios.
 */
public final class EchoFanOutMessageTransceiver extends MessageTransceiver
{
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MutableInteger receiverIndex = new MutableInteger();
    private final PersistedHistogramSet histogramSet;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;

    private Path logsDir;
    private ExclusivePublication publication;
    private Subscription[] subscriptions;
    private FragmentHandler[] fragmentHandlers;
    private int numReceivers;
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
        super(nanoClock, histogramSet.create("result").valueRecorder());
        this.histogramSet = histogramSet;
        this.mediaDriver = mediaDriver;
        this.aeron = aeron;
        this.ownsAeronClient = ownsAeronClient;
    }

    public void init(final Configuration configuration)
    {
        logsDir = configuration.logsDir();
        validateMessageLength(configuration.messageLength());

        System.out.println("EchoFanOutMessageTransceiver.init()");

        // Single outbound publication
        System.out.println("  creating publication: channel=" + destinationChannel() +
            " stream=" + destinationStreamId());
        publication = aeron.addExclusivePublication(destinationChannel(), destinationStreamId());
        System.out.println("  publication created: sessionId=" + publication.sessionId());

        // N inbound reply subscriptions
        final String[] srcChannels = sourceChannels();
        final int[] srcStreams = sourceStreams();
        assertChannelsAndStreamsMatch(srcChannels, srcStreams, SOURCE_CHANNELS_PROP_NAME, SOURCE_STREAMS_PROP_NAME);

        numReceivers = srcChannels.length;
        subscriptions = new Subscription[numReceivers];
        fragmentHandlers = new FragmentHandler[numReceivers];
        System.out.println("  numReceivers: " + numReceivers);

        for (int i = 0; i < numReceivers; i++)
        {
            System.out.println("  creating subscription[" + i + "]: channel=" +
                srcChannels[i] + " stream=" + srcStreams[i]);
            subscriptions[i] = aeron.addSubscription(srcChannels[i], srcStreams[i]);
            System.out.println("  subscription[" + i + "] created");

            String name = "receiver-" + i;
            final ValueRecorder recorder = histogramSet.create(name).valueRecorder();
            System.out.println("value recordered created: "+name);

            fragmentHandlers[i] = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    final long timestamp = buffer.getLong(offset, LITTLE_ENDIAN);
                    final long checksum = buffer.getLong(offset + length - SIZE_OF_LONG, LITTLE_ENDIAN);
                    final long now = clock.nanoTime();
                    recorder.recordValue(now - timestamp);
                    onMessageReceived(timestamp, checksum);
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

        startArchiveRecording();

        for (int i = 0; i < numReceivers; i++)
        {
            System.out.println("  awaiting subscription[" + i + "] " +
                "(remaining " + remainingConnectTimeoutNs / 1_000_000 + "ms): channel=" + subscriptions[i].channel() +
                " stream=" + subscriptions[i].streamId());
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

        final int controlStream = Integer.getInteger(ARCHIVE_CONTROL_STREAM_PROP, 0);
        final String responseChannel = System.getProperty(ARCHIVE_CONTROL_RESPONSE_CHANNEL_PROP);

        System.out.println("  connecting to archive: " + controlChannel);
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(controlChannel)
            .controlRequestStreamId(controlStream)
            .controlResponseChannel(responseChannel));

        final String spyChannel = "aeron-spy:" + destinationChannel();
        final int stream = destinationStreamId();

        System.out.println("  starting recording: channel=" + spyChannel + " stream=" + stream);
        recordingSubscriptionId = aeronArchive.startRecording(spyChannel, stream, SourceLocation.LOCAL);
        System.out.println("  recording started: subscriptionId=" + recordingSubscriptionId);
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
        AeronUtil.dumpAeronStats(
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
        return sendMessages(
            publication, bufferClaim, numberOfMessages, messageLength, timestamp, checksum, receiverIndex, 1);
    }

    public void receive()
    {
        for (int i = 0; i < numReceivers; i++)
        {
            subscriptions[i].poll(fragmentHandlers[i], FRAGMENT_LIMIT);
        }
    }

    public long expectedResponseMessages(final long iterations, final long messageRate)
    {
        return iterations * messageRate * numReceivers;
    }
}