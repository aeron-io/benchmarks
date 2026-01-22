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
package io.aeron.benchmarks.aeron;

import org.HdrHistogram.ValueRecorder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.PersistedHistogram;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.aeron.benchmarks.Configuration.outputScaleRatio;
import static java.util.Objects.requireNonNull;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;
import static io.aeron.benchmarks.aeron.FailoverConstants.ECHO_MESSAGE_LENGTH;
import static io.aeron.benchmarks.PersistedHistogram.FILE_EXTENSION;
import static io.aeron.benchmarks.PersistedHistogram.Status.OK;
import static io.aeron.benchmarks.PersistedHistogram.newPersistedHistogram;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;

public final class FailoverTestRig implements FailoverListener
{
    private final Configuration configuration;
    private final FailoverConfiguration failoverConfiguration;
    private final FailoverTransceiver transceiver;
    private final PrintStream out;
    private final NanoClock clock;
    private final PersistedHistogram persistedHistogram;
    private final ValueRecorder valueRecorder;
    private final FailoverControlClient controlClient;

    private final long[] generationTimestamps;
    private final long[] ackTimestamps;
    private int freePosition;
    private int sendPosition;
    private int ackPosition;

    private long followerRestartAt;
    private long followerRestartRequestedAt;
    private boolean followerRestartRequested;
    private long failoverAt;
    private long failoverRequestedAt;
    private boolean failoverRequested;
    private long restartAt;
    private long restartRequestedAt;
    private boolean restartRequested;
    private boolean synced = true;
    private int leaderMemberId;

    public FailoverTestRig(final Configuration configuration, final FailoverConfiguration failoverConfiguration)
    {
        this(configuration, failoverConfiguration, new ClusterFailoverTransceiver());
    }

    public FailoverTestRig(
        final Configuration configuration,
        final FailoverConfiguration failoverConfiguration,
        final FailoverTransceiver transceiver)
    {
        this(configuration,
            failoverConfiguration,
            transceiver,
            System.out,
            SystemNanoClock.INSTANCE,
            newPersistedHistogram(configuration));
    }

    public FailoverTestRig(
        final Configuration configuration,
        final FailoverConfiguration failoverConfiguration,
        final FailoverTransceiver transceiver,
        final PrintStream out,
        final NanoClock clock,
        final PersistedHistogram persistedHistogram)
    {
        this.configuration = validate(requireNonNull(configuration));
        this.failoverConfiguration = requireNonNull(failoverConfiguration);
        this.transceiver = requireNonNull(transceiver);
        this.out = requireNonNull(out);
        this.clock = requireNonNull(clock);
        this.persistedHistogram = requireNonNull(persistedHistogram);
        this.valueRecorder = persistedHistogram.valueRecorder();

        final int totalMessages = configuration.warmupIterations() * configuration.warmupMessageRate() +
            configuration.iterations() * configuration.messageRate();
        generationTimestamps = new long[totalMessages];
        ackTimestamps = new long[totalMessages];

        controlClient = new FailoverControlClient(failoverConfiguration.controlEndpoints());

        followerRestartAt = failoverAt = restartAt = clock.nanoTime() + TimeUnit.DAYS.toNanos(1);
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
                out.printf(
                    "%n%s Running warmup for %,d iterations of %,d messages each, with %,d bytes payload and" +
                    " a burst size of %,d...%n",
                    Instant.now(),
                    configuration.warmupIterations(),
                    configuration.warmupMessageRate(),
                    configuration.messageLength(),
                    configuration.batchSize());
                runTest(configuration.warmupIterations(), configuration.warmupMessageRate());

                persistedHistogram.reset();
            }

            out.printf(
                "%n%s Running measurement for %,d iterations of %,d messages each, with %,d bytes payload and" +
                " a burst size of %,d...%n",
                Instant.now(),
                configuration.iterations(),
                configuration.messageRate(),
                configuration.messageLength(),
                configuration.batchSize());
            followerRestartAt = clock.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            failoverAt = clock.nanoTime() + failoverConfiguration.failoverDelayNs();
            runTest(configuration.iterations(), configuration.messageRate());

            out.printf("%nHistogram of RTT latencies in " + configuration.outputTimeUnit() + ".%n");
            final PersistedHistogram histogram = persistedHistogram;
            histogram.outputPercentileDistribution(out, outputScaleRatio(configuration.outputTimeUnit()));

            final PersistedHistogram.Status status = OK;
            final Path histogramPath = histogram.saveToFile(
                configuration.outputDirectory(),
                configuration.outputFileNamePrefix(),
                status);
            saveRawDataToFile(
                configuration.warmupIterations() * configuration.warmupMessageRate(),
                histogramPath);
        }
        finally
        {
            CloseHelper.closeAll(transceiver, persistedHistogram, controlClient);
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

            final long t0 = generationTimestamps[startIndex];
            writeAnnotation("follower restart", followerRestartRequestedAt, t0, writer);
            writeAnnotation("failover", failoverRequestedAt, t0, writer);
            writeAnnotation("restart", restartRequestedAt, t0, writer);
        }
    }

    private void writeAnnotation(final String name, final long timestamp, final long t0, final BufferedWriter writer)
        throws IOException
    {
        final double relativeTimeSeconds = (timestamp - t0) / 1e9;
        writer.write(String.format("#annotation:%d,\"%s\",%f%n", timestamp, name, relativeTimeSeconds));
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

            if (now - followerRestartAt >= 0 && !followerRestartRequested)
            {
                final int nodeIdToRestart = leaderMemberId == 0 ? 1 : 0;
                followerRestartRequestedAt = clock.nanoTime();
                controlClient.sendCycleNodeCommand(nodeIdToRestart);
                followerRestartRequested = true;
                workCount++;
            }

            if (now - failoverAt >= 0 && !failoverRequested)
            {
                failoverRequestedAt = clock.nanoTime();
                controlClient.sendStepDownCommand();
                failoverRequested = true;
                workCount++;
            }

            if (now - restartAt >= 0 && !restartRequested)
            {
                restartRequestedAt = clock.nanoTime();
                controlClient.sendRestartCommand();
                restartRequested = true;
                workCount++;
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

        if (transceiver.trySendEcho(sequence, timestamp))
        {
            sendPosition++;

            return 1;
        }

        return 0;
    }

    public void onConnected(final long sessionId, final int leaderMemberId)
    {
        this.leaderMemberId = leaderMemberId;
        out.println("Established session " + sessionId + " with leader node " + leaderMemberId);
    }

    public void onEchoMessage(final int sequence, final long timestamp)
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

        if (failoverRequested && !restartRequested)
        {
            restartAt = clock.nanoTime() + TimeUnit.SECONDS.toNanos(6);
        }
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
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));

        final Configuration configuration = Configuration.fromSystemProperties();
        final FailoverConfiguration failoverConfiguration = FailoverConfiguration.fromSystemProperties();

        final FailoverTestRig failoverTestRig = new FailoverTestRig(configuration, failoverConfiguration);

        // wait for all background threads to be started before pinning the main thread to a dedicated core
        Thread.currentThread().setName("load-test-rig");
        failoverTestRig.run();
    }
}
