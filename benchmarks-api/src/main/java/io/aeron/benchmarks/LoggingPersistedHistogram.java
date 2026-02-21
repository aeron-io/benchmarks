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
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemEpochClock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

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

    private final SingleWriterRecorder recorder;
    private final HistogramState state;

    public LoggingPersistedHistogram(
        final Path outputDirectory,
        final SingleWriterRecorder recorder)
    {
        this(outputDirectory, "loadtestrig", recorder);
    }

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

        this.recorder = recorder;
        this.state = new HistogramState(
            new File(outputDirectory.toFile(), prefix + ".hdr"),
            recorder);
        BackgroundLogger.INSTANCE.register(state);
    }

    public void outputPercentileDistribution(final PrintStream printStream, final double outputValueUnitScalingRatio)
    {
        final Histogram histogram = state.syncAggregate();
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

        // Terminal operation: deregister, flush last interval, close writer
        final Histogram finalAggregate = BackgroundLogger.INSTANCE.deregister(state);

        final Path csvPath = outputDirectory.resolve(
            PersistedHistogram.fileName(status, prefix, HISTORY_FILE_EXTENSION));

        try (PrintStream csvOutput = new PrintStream(csvPath.toFile(), StandardCharsets.US_ASCII);
             HistogramLogReader reader = new HistogramLogReader(state.file))
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

        return PersistedHistogram.saveHistogramToFile(finalAggregate, outputDirectory, prefix, status);
    }

    public ValueRecorder valueRecorder()
    {
        return recorder;
    }

    public void reset()
    {
        state.syncReset();
    }

    /**
     * Releases resources without writing anything. Safe to call after {@link #saveToFile} — will be a no-op.
     */
    public void close()
    {
        BackgroundLogger.INSTANCE.abandon(state);
    }

    // -------------------------------------------------------------------------
    // Per-histogram state — owned by the histogram, visited by the shared thread
    // -------------------------------------------------------------------------

    static final class HistogramState
    {
        private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

        final File file;
        final SingleWriterRecorder recorder;
        final OneToOneConcurrentArrayQueue<Request> requests = new OneToOneConcurrentArrayQueue<>(16);
        final EpochClock epochClock = SystemEpochClock.INSTANCE;

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
                this.logStream = new PrintStream(new FileOutputStream(file), false, StandardCharsets.US_ASCII);
                this.writer = new HistogramLogWriter(logStream);
                this.writer.outputLogFormatVersion();
                this.writer.outputStartTime(epochClock.time());
                this.lastLogTimeMs = epochClock.time();
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

        void reset()
        {
            recorder.reset();
            aggregate.reset();
            recycled = null;
            closeWriter();
            openWriter();
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
            final long nowMs = epochClock.time();
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

        Histogram syncAggregate()
        {
            final Request request = new Request(Request.Type.AGGREGATE);
            requests.offer(request);
            return (Histogram)request.await(TIMEOUT_MS);
        }

        void syncReset()
        {
            final Request request = new Request(Request.Type.RESET);
            requests.offer(request);
            request.await(TIMEOUT_MS);
        }
    }

    // -------------------------------------------------------------------------
    // Lightweight request — one per operation, completed by background thread
    // -------------------------------------------------------------------------

    static final class Request
    {
        enum Type
        { RESET, AGGREGATE }

        final Type type;
        private final Object sync = new Object();
        private Object result;
        private boolean completed;

        Request(final Type type)
        {
            this.type = type;
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

        Object await(final long timeoutMs)
        {
            final long deadline = System.currentTimeMillis() + timeoutMs;
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

    // -------------------------------------------------------------------------
    // Singleton background thread — serves all registered HistogramState instances
    // -------------------------------------------------------------------------

    static final class BackgroundLogger
    {
        static final BackgroundLogger INSTANCE = new BackgroundLogger();

        static final long POLL_INTERVAL_MS = 100;
        static final long LOGGING_INTERVAL_MS = 1_000;

        private final Set<HistogramState> states = new CopyOnWriteArraySet<>();

        private BackgroundLogger()
        {
            final Thread thread = new Thread(this::run, "LoggingPersistedHistogram.BackgroundLogger");
            thread.setDaemon(true);
            thread.start();
        }

        void register(final HistogramState state)
        {
            state.openWriter();
            states.add(state);
        }

        /**
         * Deregisters the state, flushes the final interval, closes the writer, and returns the aggregate histogram.
         * Idempotent — safe to call even if already deregistered via {@link #abandon}.
         */
        Histogram deregister(final HistogramState state)
        {
            if (state.deregistered)
            {
                return state.aggregate.copy();
            }
            state.deregistered = true;
            states.remove(state);
            state.flush();
            final Histogram aggregate = state.aggregate.copy();
            state.closeWriter();
            return aggregate;
        }

        /**
         * Deregisters the state and releases resources without writing anything.
         * Idempotent — safe to call after {@link #deregister}.
         */
        void abandon(final HistogramState state)
        {
            if (state.deregistered)
            {
                return;
            }
            state.deregistered = true;
            states.remove(state);
            state.closeWriter();
        }

        private void run()
        {
            while (true)
            {
                for (final HistogramState state : states)
                {
                    processRequests(state);
                    state.poll();
                }

                try
                {
                    Thread.sleep(POLL_INTERVAL_MS);
                }
                catch (final InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private void processRequests(final HistogramState state)
        {
            Request request;
            while ((request = state.requests.poll()) != null)
            {
                switch (request.type)
                {
                    case RESET:
                        state.reset();
                        request.complete(null);
                        break;
                    case AGGREGATE:
                        state.flush();
                        request.complete(state.aggregate.copy());
                        break;
                }
            }
        }
    }
}