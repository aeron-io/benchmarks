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
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.ValueRecorder;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;

import static io.aeron.ChannelUri.addSessionId;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.AeronArchive.connect;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitRecordingStart;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationStreamId;
import static io.aeron.benchmarks.aeron.AeronUtil.launchArchivingMediaDriver;
import static io.aeron.benchmarks.aeron.AeronUtil.sendMessages;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceStreamId;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;
import static org.agrona.CloseHelper.closeAll;

/**
 * Implementation of the {@link MessageTransceiver} interface for benchmarking
 * live recording of the remote stream to local archive. Used together with the {@link EchoNode}.
 */
public final class LiveRecordingMessageTransceiver extends MessageTransceiver implements ControlledFragmentHandler
{
    private long recordingPosition = NULL_POSITION;
    private long recordingPositionConsumed = NULL_POSITION;
    private long recordingId;
    private final boolean ownsArchiveClient;

    private final ImageControlledFragmentAssembler messageHandler = new ImageControlledFragmentAssembler(this);
    private final ArchivingMediaDriver archivingMediaDriver;
    private final AeronArchive aeronArchive;
    private final MutableInteger receiverIndex = new MutableInteger();

    private ExclusivePublication publication;
    private final BufferClaim bufferClaim = new BufferClaim();

    private Subscription recordingEventsSubscription;
    private RecordingEventsAdapter recordingEventsAdapter;
    private Subscription subscription;
    private Image image;
    private Path logsDir;
    private IdleStrategy idleStrategy;

    public LiveRecordingMessageTransceiver(final NanoClock nanoClock, final ValueRecorder valueRecorder)
    {
        this(nanoClock, valueRecorder, launchArchivingMediaDriver(), connect(), true);
    }

    LiveRecordingMessageTransceiver(
        final NanoClock nanoClock,
        final ValueRecorder valueRecorder,
        final ArchivingMediaDriver archivingMediaDriver,
        final AeronArchive aeronArchive,
        final boolean ownsArchiveClient)
    {
        super(nanoClock, valueRecorder);
        this.archivingMediaDriver = archivingMediaDriver;
        this.aeronArchive = aeronArchive;
        this.ownsArchiveClient = ownsArchiveClient;
    }

    public void init(final Configuration configuration)
    {
        logsDir = configuration.logsDir();

        final AeronArchive.Context context = aeronArchive.context();
        final Aeron aeron = context.aeron();

        subscription = aeron.addSubscription(sourceChannel(), sourceStreamId());

        final String sendChannel = destinationChannel();
        final int sendStreamId = destinationStreamId();
        publication = aeron.addExclusivePublication(sendChannel, sendStreamId);

        recordingEventsSubscription = aeron.addSubscription(
            context.recordingEventsChannel(), context.recordingEventsStreamId());

        recordingEventsAdapter = new RecordingEventsAdapter(
            new LiveRecordingEventsListener(this), recordingEventsSubscription, FRAGMENT_LIMIT);

        idleStrategy = configuration.idleStrategy();

        awaitConnected(
            () -> recordingEventsSubscription.isConnected() && subscription.isConnected() && publication.isConnected(),
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        final int publicationSessionId = publication.sessionId();
        final String channel = addSessionId(sendChannel, publicationSessionId);
        aeronArchive.startRecording(channel, sendStreamId, LOCAL, true);
        recordingId = awaitRecordingStart(aeron, publicationSessionId, aeronArchive.archiveId());

        image = subscription.imageAtIndex(0);
    }

    public void destroy()
    {
        final String prefix = "live-recording-client-";
        AeronUtil.dumpArchiveErrors(
            archivingMediaDriver.archive.context().archiveDir(),
            logsDir.resolve(prefix + "archive-errors.txt"));
        AeronUtil.dumpAeronStats(
            aeronArchive.context().aeron().context().cncFile(),
            logsDir.resolve(prefix + "aeron-stat.txt"),
            logsDir.resolve(prefix + "errors.txt"));

        closeAll(publication, recordingEventsSubscription, subscription);

        if (ownsArchiveClient)
        {
            closeAll(aeronArchive, archivingMediaDriver);
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
        if (recordingPositionConsumed == recordingPosition)
        {
            recordingEventsAdapter.poll();
            if (recordingPositionConsumed == recordingPosition)
            {
                return; // no new recording events
            }
        }

        final int fragments = image.controlledPoll(messageHandler, FRAGMENT_LIMIT);
        if (0 == fragments && image.isClosed())
        {
            throw new IllegalStateException("image closed unexpectedly");
        }
    }

    public ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (recordingPositionConsumed == recordingPosition)
        {
            return ABORT;
        }

        final long timestamp = buffer.getLong(offset, LITTLE_ENDIAN);
        final long checksum = buffer.getLong(offset + length - SIZE_OF_LONG, LITTLE_ENDIAN);
        onMessageReceived(timestamp, checksum);
        recordingPositionConsumed += align(length, FRAME_ALIGNMENT);

        return CONTINUE;
    }

    static final class LiveRecordingEventsListener implements RecordingEventsListener
    {
        private final LiveRecordingMessageTransceiver messageTransceiver;

        LiveRecordingEventsListener(final LiveRecordingMessageTransceiver messageTransceiver)
        {
            this.messageTransceiver = messageTransceiver;
        }

        public void onStart(
            final long recordingId,
            final long startPosition,
            final int sessionId,
            final int streamId,
            final String channel,
            final String sourceIdentity)
        {
        }

        public void onProgress(final long recordingId, final long startPosition, final long position)
        {
            if (recordingId == messageTransceiver.recordingId)
            {
                if (NULL_POSITION == messageTransceiver.recordingPositionConsumed)
                {
                    messageTransceiver.recordingPositionConsumed = startPosition;
                }

                messageTransceiver.recordingPosition = position;
            }
        }

        public void onStop(final long recordingId, final long startPosition, final long stopPosition)
        {
            if (recordingId == messageTransceiver.recordingId)
            {
                messageTransceiver.recordingPosition = stopPosition;
            }
        }
    }
}
