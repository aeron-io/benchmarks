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
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.SingleWriterRecorder;
import org.HdrHistogram.ValueRecorder;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemEpochClock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;

/**
 * A persistent histogram that periodically logs a histogram of values. Primarily so that potential latency spikes
 * can be correlated over time.
 * <p>
 * All instances share a single background daemon thread via {@link BackgroundLogger}.
 * <p>
 * {@link #saveToFile} is the terminal operation — it flushes, writes output, deregisters from the shared logger,
 * and closes the underlying log writer.
 * <p>
 * {@link #close} releases resources without writing anything. Safe to call after {@link #saveToFile}.
 */
public class LoggingPersistedHistogram implements PersistedHistogram
{
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final double[] CSV_PERCENTILES = {50.0, 99.0, 99.9, 99.99, 99.999, 100.0};
    static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final EpochClock EPOCH_CLOCK = SystemEpochClock.INSTANCE;
    private static final long POLL_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);


    private final SingleWriterRecorder recorder;
    private final HistogramState state;
    private final String namePrefix;

    public LoggingPersistedHistogram(
        final Path outputDirectory,
        final String namePrefix,
        final SingleWriterRecorder recorder)
    {
        requireNonNull(outputDirectory);
        requireNonNull(recorder);

        final String prefix = namePrefix.trim();
        if (prefix.isEmpty())
        {
            throw new IllegalArgumentException("Name prefix cannot be blank!");
        }

        this.namePrefix = namePrefix;
        this.recorder = recorder;
        this.state = new HistogramState(
            new File(outputDirectory.toFile(), prefix + FILE_EXTENSION),
            recorder);
        BackgroundLogger.INSTANCE.syncRegister(state);
    }

    public void outputPercentileDistribution(final PrintStream printStream, final double outputValueUnitScalingRatio)
    {
        final Histogram histogram = BackgroundLogger.INSTANCE.syncAggregate(state);
        histogram.outputPercentileDistribution(printStream, outputValueUnitScalingRatio);
    }

    public Path saveToFile(final Path outputDirectory, final String namePrefix, final Status status) throws IOException
    {
        requireNonNull(outputDirectory);

        final String prefix = namePrefix.trim();
        if (prefix.isEmpty())
        {
            throw new IllegalArgumentException("Name prefix cannot be blank!");
        }

        BackgroundLogger.INSTANCE.syncDeregister(state);

        state.flush();
        state.closeWriter();

        Path result = state.file.toPath();

        Path newPath = result;
        if (!namePrefix.equals(this.namePrefix))
        {
            newPath = outputDirectory.resolve(namePrefix + FILE_EXTENSION);
        }
        if (status == Status.FAIL)
        {
            newPath = state.file.toPath().resolveSibling(newPath + FAILED_FILE_SUFFIX);
        }
        if (newPath != result)
        {
            Files.move(state.file.toPath(), newPath, StandardCopyOption.REPLACE_EXISTING);
            result = newPath;
        }

        final Path csvPath = result.resolveSibling(
            PersistedHistogram.fileName(status, prefix, HISTORY_FILE_EXTENSION));

        try (PrintStream csvOutput = new PrintStream(csvPath.toFile(), StandardCharsets.US_ASCII);
            HistogramLogReader reader = new HistogramLogReader(result.toFile()))
        {
            csvOutput.print("timestamp (ms)");
            for (final double percentile : CSV_PERCENTILES)
            {
                csvOutput.print(",");
                csvOutput.print(percentile);
            }
            csvOutput.println();

            while (reader.hasNext())
            {
                final Histogram interval = (Histogram)reader.nextIntervalHistogram();
                if (interval == null)
                {
                    continue;
                }

                final long midPointTimestamp = interval.getStartTimeStamp() +
                    ((interval.getEndTimeStamp() - interval.getStartTimeStamp()) / 2);
                csvOutput.print(midPointTimestamp);

                for (final double percentile : CSV_PERCENTILES)
                {
                    csvOutput.print(",");
                    csvOutput.print(interval.getValueAtPercentile(percentile));
                }
                csvOutput.println();
            }
        }

        return result;
    }

    public ValueRecorder valueRecorder()
    {
        return recorder;
    }

    public void reset()
    {
        BackgroundLogger.INSTANCE.syncReset(state);
    }

    /**
     * Releases resources without writing anything. Safe to call after {@link #saveToFile} — will be a no-op.
     */
    public void close()
    {
        BackgroundLogger.INSTANCE.syncDeregister(state);

        state.closeWriter();
    }

    // -------------------------------------------------------------------------
    // Per-histogram state — pure data container
    // -------------------------------------------------------------------------

    static final class HistogramState
    {
        final File file;
        final SingleWriterRecorder recorder;

        volatile boolean deregistered = false;

        // Accessed only by the background thread
        Histogram recycled;
        Histogram aggregate = new Histogram(SIGNIFICANT_DIGITS);
        PrintStream logStream;
        HistogramLogWriter writer;
        long lastLogTimeMs;

        HistogramState(final File file, final SingleWriterRecorder recorder)
        {
            this.file = file;
            this.recorder = recorder;
        }

        void openWriter()
        {
            try
            {
                final long nowMs = EPOCH_CLOCK.time();
                this.logStream = new PrintStream(new FileOutputStream(file), false, StandardCharsets.US_ASCII);
                this.writer = new HistogramLogWriter(logStream);
                this.writer.outputLogFormatVersion();
                this.writer.outputStartTime(nowMs);
                this.lastLogTimeMs = nowMs;
            }
            catch (final IOException ex)
            {
                throw new IllegalStateException("Failed to open histogram log: " + file, ex);
            }
        }

        void closeWriter()
        {
            if (writer != null)
            {
                writer.close();
                writer = null;
            }
            if (logStream != null)
            {
                logStream.close();
                logStream = null;
            }
        }

        void flush()
        {
            recycled = recorder.getIntervalHistogram(recycled);
            if (recycled.getTotalCount() > 0)
            {
                aggregate.add(recycled);
                writer.outputIntervalHistogram(recycled);
                logStream.flush();
            }
        }

        void poll()
        {
            final long nowMs = EPOCH_CLOCK.time();
            if (nowMs < lastLogTimeMs + BackgroundLogger.LOGGING_INTERVAL_MS)
            {
                return;
            }

            recycled = recorder.getIntervalHistogram(recycled);
            if (recycled.getTotalCount() > 0)
            {
                aggregate.add(recycled);
                writer.outputIntervalHistogram(recycled);
                logStream.flush();
                lastLogTimeMs = nowMs;
            }
        }

        void reset()
        {
            recorder.reset();
            aggregate.reset();
            recycled = null;
            closeWriter();
            openWriter();
        }
    }

    static final class Request
    {
        enum Type
        { REGISTER, RESET, AGGREGATE, DEREGISTER }

        final Type type;
        final HistogramState state;
        private final Object sync = new Object();
        private Object result;
        private boolean completed;

        Request(final Type type, final HistogramState state)
        {
            this.type = type;
            this.state = state;
        }

        void complete(final Object value)
        {
            synchronized (sync)
            {
                this.result = value;
                this.completed = true;
                sync.notifyAll();
            }
        }

        Object await()
        {
            final long deadline = System.currentTimeMillis() + LoggingPersistedHistogram.TIMEOUT_MS;
            synchronized (sync)
            {
                while (!completed)
                {
                    final long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0)
                    {
                        throw new IllegalStateException("Request timed out");
                    }
                    try
                    {
                        sync.wait(remaining);
                    }
                    catch (final InterruptedException ex)
                    {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                return result;
            }
        }
    }

    static final class BackgroundLogger
    {
        static final BackgroundLogger INSTANCE = new BackgroundLogger();

        static final long LOGGING_INTERVAL_MS = 1_000;

        private final List<HistogramState> states = new ArrayList<>();
        private final ManyToOneConcurrentArrayQueue<Request> requests = new ManyToOneConcurrentArrayQueue<>(64);

        private BackgroundLogger()
        {
            final Thread thread = new Thread(this::run, "LoggingPersistedHistogram.BackgroundLogger");
            thread.setDaemon(true);
            thread.start();
        }

        void syncRegister(final HistogramState state)
        {
            final Request request = new Request(Request.Type.REGISTER, state);
            offer(request);
            request.await();
        }

        void syncDeregister(final HistogramState state)
        {
            if (state.deregistered)
            {
                return;
            }
            final Request request = new Request(Request.Type.DEREGISTER, state);
            offer(request);
            request.await();
        }

        void syncReset(final HistogramState state)
        {
            final Request request = new Request(Request.Type.RESET, state);
            offer(request);
            request.await();
        }

        Histogram syncAggregate(final HistogramState state)
        {
            final Request request = new Request(Request.Type.AGGREGATE, state);
            offer(request);
            return (Histogram)request.await();
        }

        private void offer(final Request request)
        {
            if (!requests.offer(request))
            {
                throw new IllegalStateException("Request queue is full, failed to enqueue " + request.type);
            }
        }

        private void run()
        {
            long nextWakeNs = System.nanoTime() + POLL_INTERVAL_NS;

            while (true)
            {
                processRequests();

                final int size = states.size();
                for (int i = 0; i < size; i++)
                {
                    states.get(i).poll();
                }

                final long remainingNs = nextWakeNs - System.nanoTime();
                if (remainingNs > 0)
                {
                    LockSupport.parkNanos(remainingNs);
                }

                nextWakeNs += POLL_INTERVAL_NS;
            }
        }

        private void processRequests()
        {
            Request request;
            while ((request = requests.poll()) != null)
            {
                switch (request.type)
                {
                    case REGISTER:
                        request.state.openWriter();
                        states.add(request.state);
                        request.complete(null);
                        break;
                    case RESET:
                        request.state.reset();
                        request.complete(null);
                        break;
                    case AGGREGATE:
                        request.state.flush();
                        request.complete(request.state.aggregate.copy());
                        break;
                    case DEREGISTER:
                        request.state.deregistered = true;
                        states.remove(request.state);
                        request.complete(null);
                        break;
                }
            }
        }
    }
}