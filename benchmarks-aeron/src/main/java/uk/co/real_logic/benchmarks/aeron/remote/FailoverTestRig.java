/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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

import org.HdrHistogram.ValueRecorder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.benchmarks.remote.Configuration;
import uk.co.real_logic.benchmarks.remote.PersistedHistogram;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;
import static uk.co.real_logic.benchmarks.aeron.remote.FailoverConstants.ECHO_MESSAGE_LENGTH;
import static uk.co.real_logic.benchmarks.aeron.remote.FailoverConstants.LEADER_STEP_DOWN_FLAG;
import static uk.co.real_logic.benchmarks.remote.PersistedHistogram.*;
import static uk.co.real_logic.benchmarks.remote.PersistedHistogram.Status.OK;
import static uk.co.real_logic.benchmarks.util.PropertiesUtil.loadPropertiesFiles;
import static uk.co.real_logic.benchmarks.util.PropertiesUtil.mergeWithSystemProperties;

public final class FailoverTestRig implements FailoverListener
{
    private final Configuration configuration;
    private final FailoverTransceiver transceiver;
    private final PrintStream out;
    private final NanoClock clock;
    private final PersistedHistogram persistedHistogram;
    private final ValueRecorder valueRecorder;

    private final long[] generationTimestamps;
    private final long[] ackTimestamps;
    private int freePosition;
    private int sendPosition;
    private int ackPosition;

    private final int failoverSequence;
    private boolean failoverRequested;
    private boolean synced = true;

    public FailoverTestRig(final Configuration configuration)
    {
        this(configuration, new ClusterFailoverTransceiver());
    }

    public FailoverTestRig(final Configuration configuration, final FailoverTransceiver transceiver)
    {
        this(configuration, transceiver, System.out, SystemNanoClock.INSTANCE, newPersistedHistogram(configuration));
    }

    public FailoverTestRig(
        final Configuration configuration,
        final FailoverTransceiver transceiver,
        final PrintStream out,
        final NanoClock clock,
        final PersistedHistogram persistedHistogram)
    {
        this.configuration = validate(requireNonNull(configuration));
        this.transceiver = requireNonNull(transceiver);
        this.out = requireNonNull(out);
        this.clock = requireNonNull(clock);
        this.persistedHistogram = requireNonNull(persistedHistogram);
        this.valueRecorder = persistedHistogram.valueRecorder();

        final int totalMessages = configuration.warmupIterations() * configuration.warmupMessageRate() +
            configuration.iterations() * configuration.messageRate();
        generationTimestamps = new long[totalMessages];
        ackTimestamps = new long[totalMessages];

        failoverSequence = configuration.warmupIterations() * configuration.warmupMessageRate() +
            configuration.messageRate(); // 1s after warmup
    }

    private Configuration validate(final Configuration configuration)
    {
        if (configuration.batchSize() != 1)
        {
            throw new IllegalArgumentException("batchSize must be 1, but was " + configuration.batchSize());
        }
        if (configuration.messageLength() != ECHO_MESSAGE_LENGTH)
        {
            throw new IllegalArgumentException("messageLength must be " + ECHO_MESSAGE_LENGTH + ", but was " +
                configuration.messageLength());
        }

        return configuration;
    }

    public void run() throws Exception
    {
        out.printf("%nStarting failover benchmark using the following configuration:%n%s%n", configuration);

        try
        {
            transceiver.init(configuration, this);

            if (configuration.warmupIterations() > 0)
            {
                out.printf("%nRunning warmup for %,d iterations of %,d messages each, with %,d bytes payload and a" +
                    " burst size of %,d...%n",
                    configuration.warmupIterations(),
                    configuration.warmupMessageRate(),
                    configuration.messageLength(),
                    configuration.batchSize());
                runTest(configuration.warmupIterations(), configuration.warmupMessageRate());

                persistedHistogram.reset();
            }

            out.printf("%nRunning measurement for %,d iterations of %,d messages each, with %,d bytes payload and a" +
                " burst size of %,d...%n",
                configuration.iterations(),
                configuration.messageRate(),
                configuration.messageLength(),
                configuration.batchSize());
            runTest(configuration.iterations(), configuration.messageRate());

            out.printf("%nHistogram of RTT latencies in microseconds.%n");
            final PersistedHistogram histogram = persistedHistogram;
            histogram.outputPercentileDistribution(out, 1000.0);

            final PersistedHistogram.Status status = OK;
            final Path histogramPath = histogram.saveToFile(
                configuration.outputDirectory(),
                configuration.outputFileNamePrefix(),
                status);
            saveRawDataToFile(
                configuration.warmupIterations() * configuration.warmupMessageRate(),
                histogramPath);
            if (configuration.trackHistory())
            {
                histogram.saveHistoryToCsvFile(
                    configuration.outputDirectory(),
                    configuration.outputFileNamePrefix(),
                    status,
                    50.0, 99.0, 99.99, 100.0);
            }
        }
        finally
        {
            CloseHelper.closeAll(transceiver, persistedHistogram);
        }
    }

    private void saveRawDataToFile(final int startIndex, final Path histogramPath) throws IOException
    {
        final Path dir = histogramPath.getParent();
        final String histogramFileName = histogramPath.getFileName().toString();
        final Path path = dir.resolve(histogramFileName.replace(FILE_EXTENSION, "-raw.csv"));

        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8))
        {
            writer.write("GenerationTimestamp,AckTimestamp");
            writer.newLine();

            for (int i = startIndex; i < generationTimestamps.length; i++)
            {
                final long generationTimestamp = generationTimestamps[i];
                final long ackTimestamp = ackTimestamps[i];

                writer.write(generationTimestamp + "," + ackTimestamp);
                writer.newLine();
            }
        }
    }

    private void runTest(final int durationSeconds, final int messageRate)
    {
        final NanoClock clock = this.clock;
        final IdleStrategy idleStrategy = configuration.idleStrategy();

        final int targetMessageCount = Math.multiplyExact(durationSeconds, messageRate);
        final long periodNs = TimeUnit.SECONDS.toNanos(1) / messageRate;
        int generatedMessages = 0;
        long fallingBehindCount = 0;
        long nextMessageAt = clock.nanoTime() + TimeUnit.MICROSECONDS.toNanos(100);
        final long deadline = nextMessageAt + TimeUnit.SECONDS.toNanos(durationSeconds + 3);

        while (true)
        {
            final boolean moreToGenerate = generatedMessages < targetMessageCount;
            final boolean moreToSendOrReceive = sendPosition < freePosition || ackPosition < freePosition;

            if (!moreToGenerate && !moreToSendOrReceive)
            {
                break;
            }

            int workCount = 0;
            final long now = clock.nanoTime();

            if (moreToGenerate && now - nextMessageAt >= 0)
            {
                generationTimestamps[freePosition++] = now;

                workCount += trySend();

                generatedMessages++;
                nextMessageAt += periodNs;

                if (now - nextMessageAt >= 0)
                {
                    fallingBehindCount++;
                }
            }

            workCount += transceiver.receive();

            workCount += trySend();

            if (now - deadline >= 0)
            {
                throw new RuntimeException("Timed out");
            }

            idleStrategy.idle(workCount);
        }

        out.println("Stats: fallingBehindCount=" + fallingBehindCount);
    }

    private int trySend()
    {
        if (!synced || sendPosition >= freePosition)
        {
            return 0;
        }

        final int sequence = sendPosition;
        final long timestamp = generationTimestamps[sendPosition];
        final int flags = (sequence == failoverSequence && !failoverRequested) ? LEADER_STEP_DOWN_FLAG : 0;

        if (transceiver.trySendEcho(sequence, timestamp, flags))
        {
            sendPosition++;

            if (flags != 0)
            {
                failoverRequested = true;
            }

            return 1;
        }

        return 0;
    }

    public void onConnected(final long sessionId, final int leaderMemberId)
    {
        out.println("Established session " + sessionId + " with leader node " + leaderMemberId);
    }

    public void onEchoMessage(final int sequence, final long timestamp, final int flags)
    {
        final long now = clock.nanoTime();

        final int expectedSequence = ackPosition;
        if (sequence != expectedSequence)
        {
            throw new IllegalStateException("expected " + expectedSequence + ", but got " + sequence);
        }

        ackTimestamps[ackPosition] = now;

        ackPosition++;

        final long latencyNs = now - timestamp;
        valueRecorder.recordValue(latencyNs);
    }

    public void onSyncMessage(final int expectedSequence)
    {
        final int diff = sendPosition - expectedSequence;
        sendPosition = expectedSequence;
        synced = true;

        out.println("Synced, will resume sending from " + expectedSequence + ", had to rewind " + diff);
    }

    public void onNewLeader(final int leaderMemberId)
    {
        final long lastAckAt = ackPosition > 0 ? ackTimestamps[ackPosition - 1] : 0;
        final long failoverDurationMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - lastAckAt);
        out.println("Connected to new leader " + leaderMemberId +
            ", approximate failover duration was " + failoverDurationMs + "ms, syncing...");

        synced = false;

        final int expectedSequence = ackPosition;
        transceiver.sendSync(expectedSequence);
    }

    public static void main(final String[] args) throws Exception
    {
        Thread.currentThread().setName("load-test-rig");
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));

        final Configuration configuration = Configuration.fromSystemProperties();

        new FailoverTestRig(configuration).run();
    }
}
