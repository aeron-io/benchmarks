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
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import org.HdrHistogram.ValueRecorder;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;

import static io.aeron.ChannelUri.addSessionId;
import static io.aeron.archive.client.AeronArchive.connect;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationStreamId;
import static io.aeron.benchmarks.aeron.AeronUtil.findLastRecordingId;
import static io.aeron.benchmarks.aeron.AeronUtil.launchEmbeddedMediaDriverIfConfigured;
import static io.aeron.benchmarks.aeron.AeronUtil.recordChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.recordStream;
import static io.aeron.benchmarks.aeron.AeronUtil.replayFullRecording;
import static io.aeron.benchmarks.aeron.AeronUtil.sendMessages;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceStreamId;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.CloseHelper.closeAll;

public final class LiveReplayMessageTransceiver extends MessageTransceiver
{
    private final MediaDriver mediaDriver;
    private final AeronArchive aeronArchive;
    private final boolean ownsArchiveClient;

    private ExclusivePublication publication;
    private final BufferClaim bufferClaim = new BufferClaim();

    private Subscription subscription;
    private Image image;
    private final FragmentAssembler dataHandler = new FragmentAssembler(
        (buffer, offset, length, header) ->
        {
            final long timestamp = buffer.getLong(offset, LITTLE_ENDIAN);
            final long checksum = buffer.getLong(offset + length - SIZE_OF_LONG, LITTLE_ENDIAN);
            onMessageReceived(timestamp, checksum);
        });
    private final MutableInteger receiverIndex = new MutableInteger();
    private Path logsDir;
    private IdleStrategy idleStrategy;

    public LiveReplayMessageTransceiver(
        final NanoClock nanoClock,
        final ValueRecorder valueRecorder)
    {
        this(nanoClock, valueRecorder, launchEmbeddedMediaDriverIfConfigured(), connect(), true);
    }

    LiveReplayMessageTransceiver(
        final NanoClock nanoClock,
        final ValueRecorder valueRecorder,
        final MediaDriver mediaDriver,
        final AeronArchive aeronArchive,
        final boolean ownsArchiveClient)
    {
        super(nanoClock, valueRecorder);
        this.mediaDriver = mediaDriver;
        this.aeronArchive = aeronArchive;
        this.ownsArchiveClient = ownsArchiveClient;
    }

    public void init(final Configuration configuration)
    {
        logsDir = configuration.logsDir();
        final Aeron aeron = aeronArchive.context().aeron();

        publication = aeron.addExclusivePublication(destinationChannel(), destinationStreamId());

        final long connectionTimeoutNs = connectionTimeoutNs();
        final SystemNanoClock clock = SystemNanoClock.INSTANCE;
        awaitConnected(publication::isConnected, connectionTimeoutNs, clock);

        final long recordingId = findLastRecordingId(aeronArchive, recordChannel(), recordStream());

        final String replayChannel = sourceChannel();
        final int replayStreamId = sourceStreamId();
        final long replaySessionId = replayFullRecording(aeronArchive, recordingId, replayChannel, replayStreamId);

        final String channel = addSessionId(replayChannel, (int)replaySessionId);
        subscription = aeron.addSubscription(channel, replayStreamId);

        idleStrategy = configuration.idleStrategy();

        awaitConnected(subscription::isConnected, connectionTimeoutNs, clock);

        image = subscription.imageAtIndex(0);
    }

    public void destroy()
    {
        final String prefix = "live-replay-client-";
        AeronUtil.dumpAeronStats(
            aeronArchive.context().aeron().context().cncFile(),
            logsDir.resolve(prefix + "aeron-stat.txt"),
            logsDir.resolve(prefix + "errors.txt"));
        closeAll(publication, subscription);

        if (ownsArchiveClient)
        {
            closeAll(aeronArchive, mediaDriver);
        }
    }

    public int send(final int numberOfMessages, final int messageLength, final long timestamp, final long checksum)
    {
        return sendMessages(
            publication,
            bufferClaim,
            numberOfMessages,
            messageLength,
            timestamp,
            checksum,
            receiverIndex,
            1,
            idleStrategy);
    }

    public void receive()
    {
        final int fragments = image.poll(dataHandler, FRAGMENT_LIMIT);
        if (0 == fragments && image.isClosed())
        {
            throw new IllegalStateException("image closed unexpectedly");
        }
    }
}
