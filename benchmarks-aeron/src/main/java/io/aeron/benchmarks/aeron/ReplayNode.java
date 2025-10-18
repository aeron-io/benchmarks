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
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.benchmarks.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.ChannelUri.addSessionId;
import static io.aeron.archive.client.AeronArchive.connect;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.RECEIVER_INDEX_OFFSET;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.checkPublicationResult;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.findLastRecordingId;
import static io.aeron.benchmarks.aeron.AeronUtil.idleStrategy;
import static io.aeron.benchmarks.aeron.AeronUtil.installSignalHandler;
import static io.aeron.benchmarks.aeron.AeronUtil.launchEmbeddedMediaDriverIfConfigured;
import static io.aeron.benchmarks.aeron.AeronUtil.receiverIndex;
import static io.aeron.benchmarks.aeron.AeronUtil.recordChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.recordStream;
import static io.aeron.benchmarks.aeron.AeronUtil.replayChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.replayFullRecording;
import static io.aeron.benchmarks.aeron.AeronUtil.replayStreamId;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceStreamId;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which echoes original messages back to the sender.
 */
public final class ReplayNode implements AutoCloseable, Runnable
{
    private final BufferClaim bufferClaim = new BufferClaim();
    private final FragmentHandler fragmentHandler;
    private final ExclusivePublication publication;
    private final Subscription subscription;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final AeronArchive aeronArchive;
    private final boolean ownsArchiveClient;
    private final int sessionId;
    private Image image;

    ReplayNode(final AtomicBoolean running)
    {
        this(running, launchEmbeddedMediaDriverIfConfigured(), connect(), true, receiverIndex());
    }

    ReplayNode(
        final AtomicBoolean running,
        final MediaDriver mediaDriver,
        final AeronArchive aeronArchive,
        final boolean ownsArchiveClient,
        final int receiverIndex)
    {
        this.running = running;
        this.mediaDriver = mediaDriver;
        this.aeronArchive = aeronArchive;
        this.ownsArchiveClient = ownsArchiveClient;

        final Aeron aeron = aeronArchive.context().aeron();

        publication = aeron.addExclusivePublication(sourceChannel(), sourceStreamId());

        final ChannelUri uri = ChannelUri.parse(recordChannel());
        final String alias = uri.get(CommonContext.ALIAS_PARAM_NAME);
        final long recordingId = findLastRecordingId(
            aeronArchive,
            null != alias ? CommonContext.ALIAS_PARAM_NAME + "=" + alias : "aeron:" + uri.media(),
            recordStream());

        final String replayChannel = replayChannel();
        final int replayStreamId = replayStreamId();
        final long replaySessionId = replayFullRecording(aeronArchive, recordingId, replayChannel, replayStreamId);
        this.sessionId = (int)replaySessionId;

        subscription = aeron.addSubscription(addSessionId(replayChannel, sessionId), replayStreamId);

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

    public void run()
    {
        awaitConnected(
            () -> subscription.isConnected() && publication.availableWindow() > 0,
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        image = subscription.imageBySessionId(sessionId);

        final IdleStrategy idleStrategy = idleStrategy();

        final AtomicBoolean running = this.running;

        while (true)
        {
            final int fragments = image.poll(fragmentHandler, FRAGMENT_LIMIT);
            if (0 == fragments)
            {
                if (!running.get())
                {
                    return; // Abort execution
                }

                if (image.isClosed())
                {
                    return;  // Abort execution
                }
            }

            idleStrategy.idle(fragments);
        }
    }

    public void close()
    {
        closeAll(subscription);
        closeAll(publication);

        if (ownsArchiveClient)
        {
            closeAll(aeronArchive, mediaDriver);
        }
    }

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir = Configuration.resolveLogsDir();
        final int receiverIndex = receiverIndex();

        final AtomicBoolean running = new AtomicBoolean(true);
        final Object signalHandler = installSignalHandler(() -> running.set(false));
        try
        {
            try (ReplayNode server = new ReplayNode(running))
            {
                // wait for all background threads to be started before pinning the main thread to a dedicated core
                Thread.currentThread().setName("replay-" + receiverIndex);

                server.run();

                final String prefix = "replay-node-" + receiverIndex + "-";
                AeronUtil.dumpAeronStats(
                    server.aeronArchive.context().aeron().context().cncFile(),
                    outputDir.resolve(prefix + "aeron-stat.txt"),
                    outputDir.resolve(prefix + "errors.txt"));
            }
        }
        finally
        {
            AeronUtil.close(signalHandler);
        }
    }
}
