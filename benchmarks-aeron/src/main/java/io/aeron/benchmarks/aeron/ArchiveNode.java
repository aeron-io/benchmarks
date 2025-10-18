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
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.benchmarks.Configuration;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.ChannelUri.addSessionId;
import static io.aeron.archive.client.AeronArchive.connect;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitRecordingStart;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationStreamId;
import static io.aeron.benchmarks.aeron.AeronUtil.installSignalHandler;
import static io.aeron.benchmarks.aeron.AeronUtil.launchArchivingMediaDriver;
import static io.aeron.benchmarks.aeron.AeronUtil.pipeMessages;
import static io.aeron.benchmarks.aeron.AeronUtil.recordChannel;
import static io.aeron.benchmarks.aeron.AeronUtil.recordStream;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which archives received messages and replays persisted messages back to the sender.
 * Counterpart for the {@link LiveReplayMessageTransceiver}.
 */
public final class ArchiveNode implements AutoCloseable, Runnable
{
    private final AtomicBoolean running;
    private final ArchivingMediaDriver archivingMediaDriver;
    private final AeronArchive aeronArchive;
    private final boolean ownsArchiveClient;
    private final ExclusivePublication publication;
    private final Subscription subscription;

    ArchiveNode(final AtomicBoolean running)
    {
        this(running, launchArchivingMediaDriver(), connect(), true);
    }

    ArchiveNode(
        final AtomicBoolean running,
        final ArchivingMediaDriver archivingMediaDriver,
        final AeronArchive aeronArchive,
        final boolean ownsArchiveClient)
    {
        this.running = running;
        this.archivingMediaDriver = archivingMediaDriver;
        this.aeronArchive = aeronArchive;
        this.ownsArchiveClient = ownsArchiveClient;

        final Aeron aeron = aeronArchive.context().aeron();

        subscription = aeron.addSubscription(destinationChannel(), destinationStreamId());

        final String recordChannel = recordChannel();
        final int recordStreamId = recordStream();
        publication = aeron.addExclusivePublication(recordChannel, recordStreamId);

        final int publicationSessionId = publication.sessionId();
        final String channel = addSessionId(recordChannel, publicationSessionId);
        aeronArchive.startRecording(channel, recordStreamId, LOCAL, true);
    }

    public void run()
    {
        awaitConnected(
            () -> subscription.isConnected() && publication.availableWindow() > 0,
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        awaitRecordingStart(aeronArchive.context().aeron(), publication.sessionId(), aeronArchive.archiveId());

        pipeMessages(subscription, publication, running);
    }

    public void close()
    {
        closeAll(publication, subscription);

        if (ownsArchiveClient)
        {
            closeAll(aeronArchive, archivingMediaDriver);
        }
    }

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir = Configuration.resolveLogsDir();

        final AtomicBoolean running = new AtomicBoolean(true);
        final Object signalHandler = installSignalHandler(() -> running.set(false));
        try
        {
            try (ArchiveNode server = new ArchiveNode(running))
            {
                // wait for all background threads to be started before pinning the main thread to a dedicated core
                Thread.currentThread().setName("archive-node");

                server.run();

                final String prefix = "archive-node-";
                AeronUtil.dumpArchiveErrors(
                    server.archivingMediaDriver.archive.context().archiveDir(),
                    outputDir.resolve(prefix + "archive-errors.txt"));
                AeronUtil.dumpAeronStats(
                    server.archivingMediaDriver.archive.context().aeron().context().cncFile(),
                    outputDir.resolve(prefix + "aeron-stat.txt"),
                    outputDir.resolve(prefix + "errors.txt")
                );
            }
        }
        finally
        {
            AeronUtil.close(signalHandler);
        }
    }
}
