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
package io.aeron.benchmarks;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static io.aeron.benchmarks.MessageTransceiver.CHECKSUM;
import static io.aeron.benchmarks.PersistedHistogram.FILE_EXTENSION;
import static io.aeron.benchmarks.PersistedHistogram.HISTORY_FILE_EXTENSION;
import static io.aeron.benchmarks.PersistedHistogram.Status.OK;

@Timeout(10)
class LoadTestRigTest
{
    private final IdleStrategy idleStrategy = mock(IdleStrategy.class);
    private final NanoClock clock = mock(NanoClock.class);
    private final PrintStream out = mock(PrintStream.class);
    private final Histogram histogram = mock(Histogram.class);
    private final SinglePersistedHistogram persistedHistogram = mock(SinglePersistedHistogram.class);
    private final ProgressReporter progressReporter = mock(ProgressReporter.class);
    private final MessageTransceiver messageTransceiver = spy(
        new MessageTransceiver(clock, histogram)
        {
            public void init(final Configuration configuration)
            {
            }

            public void destroy()
            {
            }

            public int send(
                final int numberOfMessages, final int messageLength, final long timestamp, final long checksum)
            {
                return numberOfMessages;
            }

            public void receive()
            {
                onMessageReceived(1, CHECKSUM);
            }
        });

    private Configuration configuration;

    @BeforeEach
    void before(final @TempDir Path tempDir)
    {
        configuration = new Configuration.Builder()
            .warmupIterations(0)
            .iterations(1)
            .messageRate(1)
            .messageTransceiverClass(InMemoryMessageTransceiver.class)
            .idleStrategy(idleStrategy)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("test")
            .build();
    }

    @Test
    void constructorThrowsNullPointerExceptionIfConfigurationIsNull()
    {
        assertThrows(NullPointerException.class, () -> new LoadTestRig(null));
    }

    @Test
    void runPerformsWarmupBeforeMeasurement() throws Exception
    {
        final long nanoTime = SECONDS.toNanos(123);
        final NanoClock clock = () -> nanoTime;

        configuration = new Configuration.Builder()
            .warmupIterations(1)
            .warmupMessageRate(1)
            .iterations(1)
            .messageRate(1)
            .outputTimeUnit(NANOSECONDS)
            .messageTransceiverClass(configuration.messageTransceiverClass())
            .idleStrategy(idleStrategy)
            .outputDirectory(configuration.outputDirectory())
            .outputFileNamePrefix("test")
            .build();

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out,
            clock,
            persistedHistogram,
            progressReporter);

        loadTestRig.run();

        final InOrder inOrder = inOrder(messageTransceiver, out, persistedHistogram, progressReporter);
        inOrder.verify(out)
            .printf("%nStarting latency benchmark using the following configuration:%n%s%n", configuration);
        inOrder.verify(messageTransceiver).init(configuration);
        inOrder.verify(out).printf("%nRunning warmup for %,d iterations of %,d messages each, with %,d bytes payload" +
            " and a burst size of %,d...%n",
            configuration.warmupIterations(),
            configuration.warmupMessageRate(),
            configuration.messageLength(),
            configuration.batchSize());
        inOrder.verify(messageTransceiver).send(1, configuration.messageLength(), nanoTime, CHECKSUM);
        inOrder.verify(progressReporter).reportProgress(nanoTime, nanoTime, 1, 1);
        inOrder.verify(messageTransceiver).reset();
        inOrder.verify(persistedHistogram).reset();
        inOrder.verify(progressReporter).reset();
        inOrder.verify(out).printf("%nRunning measurement for %,d iterations of %,d messages each, with %,d bytes" +
            " payload and a burst size of %,d...%n",
            configuration.iterations(),
            configuration.messageRate(),
            configuration.messageLength(),
            configuration.batchSize());
        inOrder.verify(messageTransceiver).send(1, configuration.messageLength(), nanoTime, CHECKSUM);
        inOrder.verify(progressReporter).reportProgress(nanoTime, nanoTime, 1, 1);
        inOrder.verify(progressReporter).reset();
        inOrder.verify(out).printf("%nHistogram of RTT latencies in NANOSECONDS.%n");
        inOrder.verify(persistedHistogram).outputPercentileDistribution(out, 1.0);
        inOrder.verify(persistedHistogram).saveToFile(
            configuration.outputDirectory(), configuration.outputFileNamePrefix(), OK);
        inOrder.verify(messageTransceiver).destroy();
        inOrder.verify(persistedHistogram).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void runWarnsAboutMissedTargetRate() throws Exception
    {
        when(clock.nanoTime()).thenReturn(1L, 9_000_000_000L);

        configuration = new Configuration.Builder()
            .warmupIterations(0)
            .iterations(3)
            .messageRate(5)
            .batchSize(2)
            .messageTransceiverClass(configuration.messageTransceiverClass())
            .idleStrategy(idleStrategy)
            .outputDirectory(configuration.outputDirectory())
            .outputFileNamePrefix("test")
            .build();

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out,
            clock,
            persistedHistogram,
            progressReporter);

        loadTestRig.run();

        verify(out).printf("%nHistogram of RTT latencies in MICROSECONDS.%n");
        verify(persistedHistogram).outputPercentileDistribution(out, 1000.0);
        verify(out).printf("%n*** WARNING: Target message rate not achieved: expected to send %,d messages in " +
            "total but managed to send only %,d messages (loss %.4f%%)!%n", 15L, 2L, 86.66666666666667d);
    }

    @Test
    void receiveShouldKeepReceivingMessagesUpToTheSentMessagesLimit()
    {
        configuration = new Configuration.Builder()
            .warmupIterations(0)
            .iterations(1)
            .messageRate(3)
            .batchSize(200)
            .outputTimeUnit(MINUTES)
            .messageTransceiverClass(configuration.messageTransceiverClass())
            .idleStrategy(idleStrategy)
            .outputDirectory(configuration.outputDirectory())
            .outputFileNamePrefix("test")
            .build();

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out, clock,
            persistedHistogram,
            progressReporter);

        loadTestRig.send(1, 3);

        verify(messageTransceiver).send(anyInt(), anyInt(), anyLong(), anyLong());
        verify(messageTransceiver, times(3)).receive();
        verify(messageTransceiver, times(4)).receivedMessages();
        verify(messageTransceiver, times(3)).onMessageReceived(anyLong(), anyLong());
        verify(idleStrategy, times(4)).reset();
        verifyNoMoreInteractions(messageTransceiver, idleStrategy);
    }

    @Test
    void sendStopsWhenTotalNumberOfMessagesIsReached(final @TempDir Path tempDir)
    {
        when(clock.nanoTime()).thenReturn(
            MILLISECONDS.toNanos(1000),
            MILLISECONDS.toNanos(1750),
            MILLISECONDS.toNanos(2400),
            MILLISECONDS.toNanos(2950));

        final Configuration configuration = new Configuration.Builder()
            .messageRate(1)
            .idleStrategy(idleStrategy)
            .batchSize(4)
            .messageLength(24)
            .messageTransceiverClass(InMemoryMessageTransceiver.class)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("test")
            .build();

        doAnswer(
            invocation ->
            {
                final MessageTransceiver mt = (MessageTransceiver)invocation.getMock();
                mt.onMessageReceived(1, CHECKSUM);
                mt.onMessageReceived(1, CHECKSUM);
                return null;
            }).when(messageTransceiver).receive();

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out,
            clock,
            persistedHistogram,
            progressReporter);

        final LoadTestRig.SendResult result = loadTestRig.send(2, 9);

        assertEquals(18, result.sentMessages);
        assertEquals(18, result.receivedMessages);
        verify(clock, times(24)).nanoTime();
        verify(idleStrategy, times(10)).reset();
        verify(messageTransceiver).send(4, 24, 1000000000L, CHECKSUM);
        verify(messageTransceiver).send(4, 24, 1444444444L, CHECKSUM);
        verify(messageTransceiver).send(4, 24, 1888888888L, CHECKSUM);
        verify(messageTransceiver).send(4, 24, 2333333332L, CHECKSUM);
        verify(messageTransceiver).send(2, 24, 2777777776L, CHECKSUM);
        verify(messageTransceiver, times(9)).receive();
        verify(messageTransceiver, times(10)).receivedMessages();
        verify(messageTransceiver, times(18)).onMessageReceived(anyLong(), anyLong());
        verify(progressReporter).reportProgress(1000000000L, 2400000000L, 8, 2);
        verify(progressReporter).reportProgress(1000000000L, 2950000000L, 18, 2);
        verifyNoMoreInteractions(out, clock, idleStrategy, messageTransceiver);
    }

    @Test
    void sendStopsIfTimeElapsesBeforeTargetNumberOfMessagesIsReached(final @TempDir Path tempDir)
    {
        when(messageTransceiver.send(anyInt(), anyInt(), anyLong(), anyLong())).thenReturn(15, 10, 5, 30);
        when(clock.nanoTime()).thenReturn(
            MILLISECONDS.toNanos(500),
            MILLISECONDS.toNanos(501),
            MILLISECONDS.toNanos(777),
            MILLISECONDS.toNanos(778),
            MILLISECONDS.toNanos(6750),
            MILLISECONDS.toNanos(6751),
            MILLISECONDS.toNanos(9200),
            MILLISECONDS.toNanos(9201),
            MILLISECONDS.toNanos(12000),
            MILLISECONDS.toNanos(12001)
        );

        final Configuration configuration = new Configuration.Builder()
            .messageRate(1)
            .idleStrategy(idleStrategy)
            .batchSize(30)
            .messageLength(100)
            .messageTransceiverClass(InMemoryMessageTransceiver.class)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("test")
            .build();

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out, clock,
            persistedHistogram,
            progressReporter);

        final LoadTestRig.SendResult result = loadTestRig.send(10, 100);

        assertEquals(120, result.sentMessages);
        assertEquals(120, result.receivedMessages);
        verify(clock, times(128)).nanoTime();
        verify(idleStrategy, times(119)).reset();
        verify(messageTransceiver).send(30, 100, MILLISECONDS.toNanos(500), CHECKSUM);
        verify(messageTransceiver).send(15, 100, MILLISECONDS.toNanos(500), CHECKSUM);
        verify(messageTransceiver).send(5, 100, MILLISECONDS.toNanos(500), CHECKSUM);
        verify(messageTransceiver).send(30, 100, MILLISECONDS.toNanos(800), CHECKSUM);
        verify(messageTransceiver).send(30, 100, MILLISECONDS.toNanos(1100), CHECKSUM);
        verify(messageTransceiver).send(30, 100, MILLISECONDS.toNanos(1400), CHECKSUM);
        verify(messageTransceiver, times(120)).receive();
        verify(messageTransceiver, times(119)).receivedMessages();
        verify(messageTransceiver, times(120)).onMessageReceived(anyLong(), anyLong());
        verify(progressReporter).reportProgress(500000000L, 6751000000L, 30, 10);
        verify(progressReporter).reportProgress(500000000L, 9200000000L, 60, 10);
        verify(progressReporter).reportProgress(500000000L, 9201000000L, 90, 10);
        verifyNoMoreInteractions(out, clock, idleStrategy, messageTransceiver);
    }

    @Test
    void endToEndTest(final @TempDir Path tempDir) throws Exception
    {
        final Configuration configuration = new Configuration.Builder()
            .warmupIterations(2)
            .warmupMessageRate(100)
            .iterations(5)
            .messageRate(777)
            .messageLength(32)
            .batchSize(9)
            .messageTransceiverClass(InMemoryMessageTransceiver.class)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("test")
            .trackHistory(false)
            .build();
        final LoadTestRig testRig = new LoadTestRig(configuration);

        final long startTimeNs = System.nanoTime();
        testRig.run();
        final long durationNs = System.nanoTime() - startTimeNs;

        final long maxDurationNs = SECONDS.toNanos(
            configuration.warmupIterations() +
            configuration.iterations() +
            1 /* init + report + destroy */);
        assertTrue(durationNs <= maxDurationNs,
            () -> "Too long: duration=" + durationNs + " vs maxDurationNs=" + maxDurationNs);
        final File[] files = tempDir.toFile().listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertEquals(1, Stream.of(files).filter((f) -> f.getName().endsWith(FILE_EXTENSION)).count());
        assertEquals(
            tempDir.resolve("logs").toFile(),
            Stream.of(files).filter(File::isDirectory).findFirst().orElse(null));
    }

    @Test
    void endToEndTestWithHistory(final @TempDir Path tempDir) throws Exception
    {
        final Configuration configuration = new Configuration.Builder()
            .warmupIterations(1)
            .warmupMessageRate(999)
            .iterations(3)
            .messageRate(3040)
            .messageLength(32)
            .batchSize(13)
            .messageTransceiverClass(InMemoryMessageTransceiver.class)
            .outputDirectory(tempDir)
            .outputFileNamePrefix("test")
            .trackHistory(true)
            .build();
        final LoadTestRig testRig = new LoadTestRig(configuration);

        testRig.run();

        final File[] files = tempDir.toFile().listFiles();
        assertNotNull(files);
        assertEquals(3, files.length);
        assertEquals(1, Stream.of(files).filter((f) -> f.getName().endsWith(FILE_EXTENSION)).count());
        assertEquals(1, Stream.of(files).filter((f) -> f.getName().endsWith(HISTORY_FILE_EXTENSION)).count());
        assertEquals(
            tempDir.resolve("logs").toFile(),
            Stream.of(files).filter(File::isDirectory).findFirst().orElse(null));
    }

    @Test
    void shouldCallDestroyOnMessageTransceiverIfInitFails() throws Exception
    {
        final IllegalStateException initException = new IllegalStateException("Init failed");
        doThrow(initException).when(messageTransceiver).init(configuration);

        final LoadTestRig loadTestRig = new LoadTestRig(
            configuration,
            messageTransceiver,
            out,
            mock(NanoClock.class),
            mock(PersistedHistogram.class),
            progressReporter);

        final IllegalStateException exception = assertThrows(IllegalStateException.class, loadTestRig::run);
        assertSame(initException, exception);
        verify(messageTransceiver).destroy();
    }
}
