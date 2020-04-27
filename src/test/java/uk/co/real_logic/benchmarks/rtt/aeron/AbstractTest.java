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
package uk.co.real_logic.benchmarks.rtt.aeron;

import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import uk.co.real_logic.benchmarks.rtt.Configuration;
import uk.co.real_logic.benchmarks.rtt.MessageRecorder;
import uk.co.real_logic.benchmarks.rtt.MessageTransceiver;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.co.real_logic.benchmarks.rtt.Configuration.MIN_MESSAGE_LENGTH;
import static uk.co.real_logic.benchmarks.rtt.aeron.AeronUtil.EMBEDDED_MEDIA_DRIVER_PROP_NAME;

abstract class AbstractTest<DRIVER extends AutoCloseable,
    CLIENT extends AutoCloseable,
    MESSAGE_TRANSCEIVER extends MessageTransceiver,
    NODE extends AutoCloseable & Runnable>
{
    @BeforeEach
    void before()
    {
        setProperty(EMBEDDED_MEDIA_DRIVER_PROP_NAME, "true");
    }

    @AfterEach
    void after()
    {
        clearProperty(EMBEDDED_MEDIA_DRIVER_PROP_NAME);
    }

    @Timeout(30)
    @Test
    void messageLength16bytes(final @TempDir Path tempDir) throws Exception
    {
        test(10_000, MIN_MESSAGE_LENGTH, 10, tempDir);
    }

    @Timeout(30)
    @Test
    void messageLength200bytes(final @TempDir Path tempDir) throws Exception
    {
        test(1000, 200, 5, tempDir);
    }

    @Timeout(30)
    @Test
    void messageLength32KB(final @TempDir Path tempDir) throws Exception
    {
        test(100, 32 * 1024, 1, tempDir);
    }

    @SuppressWarnings("MethodLength")
    private void test(
        final int messages, final int messageLength, final int burstSize, final Path tempDir) throws Exception
    {
        final Configuration configuration = new Configuration.Builder()
            .numberOfMessages(messages)
            .messageLength(messageLength)
            .messageTransceiverClass(messageTransceiverClass())
            .outputDirectory(tempDir)
            .build();

        final AtomicReference<Throwable> error = new AtomicReference<>();
        final LongArrayList receivedTimestamps = new LongArrayList(messages, Long.MIN_VALUE);
        final LongArrayList sentTimestamps = new LongArrayList(messages, Long.MIN_VALUE);

        final DRIVER driver = createDriver();
        final CLIENT client = connectToDriver();
        try
        {
            final AtomicBoolean running = new AtomicBoolean(true);
            final CountDownLatch publisherStarted = new CountDownLatch(1);

            final Thread nodeThread = new Thread(
                () ->
                {
                    publisherStarted.countDown();

                    try (NODE node = createNode(running, driver, client))
                    {
                        node.run();
                    }
                    catch (final Throwable t)
                    {
                        error.set(t);
                    }
                });
            nodeThread.setName("remote-node");
            nodeThread.setDaemon(true);
            nodeThread.start();

            final MessageTransceiver messageTransceiver =
                createMessageTransceiver(driver, client, (timestamp, checksum) ->
                {
                    assertEquals(-timestamp, checksum);
                    receivedTimestamps.addLong(timestamp);
                });

            publisherStarted.await();

            messageTransceiver.init(configuration);
            try
            {
                Thread.currentThread().setName("message-transceiver");
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
                running.set(false);
                nodeThread.join();
                messageTransceiver.destroy();
            }
        }
        finally
        {
            closeAll(client, driver);

            if (driver instanceof MediaDriver)
            {
                ((MediaDriver)driver).context().deleteAeronDirectory();
            }
            else
            {
                final ArchivingMediaDriver archivingMediaDriver = (ArchivingMediaDriver)driver;
                archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
                archivingMediaDriver.archive().context().deleteDirectory();
            }
        }

        if (null != error.get())
        {
            rethrowUnchecked(error.get());
        }

        assertEquals(sentTimestamps, receivedTimestamps);
    }

    abstract NODE createNode(AtomicBoolean running, DRIVER driver, CLIENT client);

    abstract DRIVER createDriver();

    abstract CLIENT connectToDriver();

    abstract Class<MESSAGE_TRANSCEIVER> messageTransceiverClass();

    abstract MESSAGE_TRANSCEIVER createMessageTransceiver(
        DRIVER driver, CLIENT client, MessageRecorder messageRecorder);
}
