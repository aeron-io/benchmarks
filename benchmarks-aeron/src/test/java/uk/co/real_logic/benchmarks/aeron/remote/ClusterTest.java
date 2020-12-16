/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.benchmarks.aeron.remote;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.NoOpLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import uk.co.real_logic.benchmarks.remote.Configuration;
import uk.co.real_logic.benchmarks.remote.MessageTransceiver;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static io.aeron.driver.Configuration.DIR_DELETE_ON_SHUTDOWN_PROP_NAME;
import static io.aeron.driver.Configuration.DIR_DELETE_ON_START_PROP_NAME;
import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.EMBEDDED_MEDIA_DRIVER_PROP_NAME;
import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.launchArchivingMediaDriver;

class ClusterTest
{
    @BeforeEach
    void before()
    {
        setProperty(EMBEDDED_MEDIA_DRIVER_PROP_NAME, "true");
        setProperty(AeronArchive.Configuration.RECORDING_EVENTS_ENABLED_PROP_NAME, "false");
        setProperty(DIR_DELETE_ON_START_PROP_NAME, "true");
        setProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME, "true");
        setProperty(Archive.Configuration.ARCHIVE_DIR_DELETE_ON_START_PROP_NAME, "true");
    }

    @AfterEach
    void after()
    {
        clearProperty(EMBEDDED_MEDIA_DRIVER_PROP_NAME);
        clearProperty(DIR_DELETE_ON_START_PROP_NAME);
        clearProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME);
        clearProperty(Archive.Configuration.ARCHIVE_DIR_DELETE_ON_START_PROP_NAME);
        clearProperty(AERON_DIR_PROP_NAME);
        clearProperty(Archive.Configuration.ARCHIVE_DIR_PROP_NAME);
        clearProperty(AeronArchive.Configuration.RECORDING_EVENTS_ENABLED_PROP_NAME);
        clearProperty(AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME);
        clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME);
    }

    @Timeout(30)
    @Test
    void messageLength32bytes(final @TempDir Path tempDir) throws Exception
    {
        test(10_000, 32, 10, tempDir);
    }

    @Timeout(30)
    @Test
    void messageLength192bytes(final @TempDir Path tempDir) throws Exception
    {
        test(1000, 192, 5, tempDir);
    }

    @Timeout(30)
    @Test
    void messageLength1344bytes(final @TempDir Path tempDir) throws Exception
    {
        test(100, 1344, 1, tempDir);
    }

    @SuppressWarnings("MethodLength")
    protected final void test(
        final int messages,
        final int messageLength,
        final int burstSize,
        final Path tempDir) throws Exception
    {
        final String aeronDirectoryName = tempDir.resolve("driver").toString();
        setProperty(AERON_DIR_PROP_NAME, aeronDirectoryName);
        setProperty(Archive.Configuration.ARCHIVE_DIR_PROP_NAME, tempDir.resolve("archive").toString());
        setProperty(AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME,
            "aeron:udp?endpoint=localhost:8010|term-length=64k");
        setProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME, "aeron:ipc?term-length=64k");

        final Configuration configuration = new Configuration.Builder()
            .numberOfMessages(messages)
            .messageLength(messageLength)
            .messageTransceiverClass(ClusterMessageTransceiver.class)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("aeron")
            .build();

        final AtomicReference<Throwable> error = new AtomicReference<>();
        final LongArrayList receivedTimestamps = new LongArrayList(messages, Long.MIN_VALUE);
        final LongArrayList sentTimestamps = new LongArrayList(messages, Long.MIN_VALUE);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(AeronArchive.Configuration.controlChannel())
            .controlRequestStreamId(AeronArchive.Configuration.controlStreamId())
            .controlResponseChannel("aeron:udp?endpoint=localhost:0|term-length=64k")
            .aeronDirectoryName(aeronDirectoryName);

        final String clusterDirectoryName = tempDir.resolve("consensus-module").toString();

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .clusterMemberId(0)
            .clusterMembers("0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:8010")
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel("aeron:udp?term-length=64k|control-mode=manual|control=localhost:20002")
            .errorHandler(AeronUtil.rethrowingErrorHandler("consensus-module"))
            .archiveContext(aeronArchiveContext.clone())
            .aeronDirectoryName(aeronDirectoryName)
            .clusterDirectoryName(clusterDirectoryName);

        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context()
            .clusteredService(new EchoClusteredService())
            .errorHandler(AeronUtil.rethrowingErrorHandler("service-container"))
            .archiveContext(aeronArchiveContext.clone())
            .aeronDirectoryName(aeronDirectoryName)
            .clusterDirectoryName(clusterDirectoryName);

        final AeronCluster.Context aeronClusterContext = new AeronCluster.Context()
            .ingressChannel("aeron:udp?term-length=64k")
            .ingressEndpoints("0=localhost:20000")
            .egressChannel("aeron:udp?endpoint=localhost:0|term-length=64k");

        try (ArchivingMediaDriver driver = launchArchivingMediaDriver();
            ConsensusModule consensusModule = ConsensusModule.launch(consensusModuleContext);
            ClusteredServiceContainer clusteredServiceContainer =
                ClusteredServiceContainer.launch(serviceContainerContext))
        {
            final MessageTransceiver messageTransceiver = new ClusterMessageTransceiver(
                null,
                aeronClusterContext,
                (timestamp, checksum) ->
                {
                    assertEquals(-timestamp, checksum);
                    receivedTimestamps.addLong(timestamp);
                });

            messageTransceiver.init(configuration);
            try
            {
                int sent = 0;
                long timestamp = 1_000;
                while (sent < messages || receivedTimestamps.size() < messages)
                {
                    if (Thread.interrupted())
                    {
                        throw new IllegalStateException("run cancelled!");
                    }

                    if (sent < messages)
                    {
                        int sentBatch = 0;
                        do
                        {
                            sentBatch +=
                                messageTransceiver.send(burstSize - sentBatch, messageLength, timestamp, -timestamp);
                            messageTransceiver.receive();
                        }
                        while (sentBatch < burstSize);

                        for (int i = 0; i < burstSize; i++)
                        {
                            sentTimestamps.add(timestamp);
                        }

                        sent += burstSize;
                        timestamp++;
                    }

                    if (receivedTimestamps.size() < messages)
                    {
                        messageTransceiver.receive();
                    }

                    if (null != error.get())
                    {
                        rethrowUnchecked(error.get());
                    }
                }
            }
            finally
            {
                messageTransceiver.destroy();
            }
        }

        if (null != error.get())
        {
            rethrowUnchecked(error.get());
        }

        assertEquals(sentTimestamps, receivedTimestamps);
    }

}
