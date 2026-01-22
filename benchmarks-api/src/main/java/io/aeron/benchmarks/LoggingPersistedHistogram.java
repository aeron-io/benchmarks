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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A persistent histogram that periodically logs a histogram of values. Primarily so that potential latency spikes
 * can be correlated over time.
 */
public class LoggingPersistedHistogram implements PersistedHistogram
{
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final double[] CSV_PERCENTILES = {50.0, 99.0, 99.9, 99.99, 99.999, 100.0};

    private final SingleWriterRecorder recorder;
    private final BackgroundLogger backgroundLogger;
    private final File loadTestRigHgrmFile;

    public LoggingPersistedHistogram(final Path outputDirectory, final SingleWriterRecorder recorder) throws IOException
    {
        this.recorder = recorder;
        this.loadTestRigHgrmFile = new File(outputDirectory.toFile(), "loadtestrig.hgrm");
        this.backgroundLogger = new BackgroundLogger(loadTestRigHgrmFile, recorder);
        this.backgroundLogger.start();
    }

    public void outputPercentileDistribution(final PrintStream printStream, final double outputValueUnitScalingRatio)
    {
        final Histogram histogram = backgroundLogger.syncAggregate();
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

        final Histogram finalAggregate = backgroundLogger.syncAggregate();

        final Path csvPath = outputDirectory.resolve(
            PersistedHistogram.fileName(status, prefix, HISTORY_FILE_EXTENSION));

        try (PrintStream csvOutput = new PrintStream(csvPath.toFile(), StandardCharsets.US_ASCII);
            HistogramLogReader reader = new HistogramLogReader(loadTestRigHgrmFile))
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
        backgroundLogger.syncReset();
    }

    public void close()
    {
        backgroundLogger.syncStop();
    }

    private static class Request
    {
        private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

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

        Object await()
        {
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;

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

    private static class BackgroundLogger
    {
        private static final long POLL_INTERVAL_MS = 100;
        private static final long LOGGING_INTERVAL_MS = 1_000;

        private final EpochClock epochClock = SystemEpochClock.INSTANCE;
        private final Thread thread;
        private final File file;
        private final SingleWriterRecorder recorder;
        private final OneToOneConcurrentArrayQueue<Request> requests = new OneToOneConcurrentArrayQueue<>(16);

        private volatile boolean stopped = false;
        private HistogramLogWriter writer;
        private Histogram recycled;
        private Histogram aggregate = new Histogram(SIGNIFICANT_DIGITS);
        private long lastLogTimeMs;

        BackgroundLogger(final File file, final SingleWriterRecorder recorder)
        {
            this.file = file;
            this.recorder = recorder;
            this.thread = new Thread(this::run);
            this.thread.setName("LoggingPersistedHistogram.BackgroundLogger");
            this.thread.setDaemon(true);
        }

        void start()
        {
            lastLogTimeMs = epochClock.time();
            thread.start();
        }

        void syncReset()
        {
            if (stopped)
            {
                return;
            }

            final Request request = new Request(Request.Type.RESET);
            requests.offer(request);

            if (stopped && requests.remove(request))
            {
                return;
            }

            request.await();
        }

        Histogram syncAggregate()
        {
            if (stopped)
            {
                throw new IllegalStateException("BackgroundLogger terminated");
            }

            final Request request = new Request(Request.Type.AGGREGATE);
            requests.offer(request);

            if (stopped && requests.remove(request))
            {
                throw new IllegalStateException("BackgroundLogger terminated");
            }

            return (Histogram)request.await();
        }

        void syncStop()
        {
            stopped = true;
            try
            {
                thread.join();
            }
            catch (final InterruptedException ex)
            {
                Thread.currentThread().interrupt();
            }
        }

        private void run()
        {
            openWriter();
            try
            {
                while (!stopped)
                {
                    processRequests();
                    poll();

                    try
                    {
                        Thread.sleep(POLL_INTERVAL_MS);
                    }
                    catch (final InterruptedException ex)
                    {
                        break;
                    }
                }
            }
            finally
            {
                flush();
                closeWriter();
                rejectPendingRequests();
            }
        }

        private void processRequests()
        {
            Request request;
            while ((request = requests.poll()) != null)
            {
                switch (request.type)
                {
                    case RESET:
                        reset();
                        request.complete(null);
                        break;
                    case AGGREGATE:
                        flush();
                        request.complete(aggregate.copy());
                        break;
                }
            }
        }

        private void rejectPendingRequests()
        {
            Request request;
            while ((request = requests.poll()) != null)
            {
                request.complete(null);
            }
        }

        private void reset()
        {
            recorder.reset();
            aggregate.reset();
            recycled = null;
            closeWriter();
            openWriter();
            lastLogTimeMs = epochClock.time();
        }

        private void flush()
        {
            recycled = recorder.getIntervalHistogram(recycled);
            if (recycled.getTotalCount() > 0)
            {
                aggregate.add(recycled);
                writer.outputIntervalHistogram(recycled);
            }
        }

        private void poll()
        {
            final long nowMs = epochClock.time();
            if (nowMs < lastLogTimeMs + LOGGING_INTERVAL_MS)
            {
                return;
            }

            recycled = recorder.getIntervalHistogram(recycled);
            if (recycled.getTotalCount() > 0)
            {
                aggregate.add(recycled);
                writer.outputIntervalHistogram(recycled);
                lastLogTimeMs = nowMs;
            }
        }

        private void openWriter()
        {
            try
            {
                this.writer = new HistogramLogWriter(file);
                this.writer.outputLogFormatVersion();
                this.writer.outputStartTime(epochClock.time());
            }
            catch (final IOException ex)
            {
                throw new IllegalStateException("Failed to open histogram log", ex);
            }
        }

        private void closeWriter()
        {
            if (writer != null)
            {
                writer.close();
                writer = null;
            }
        }
    }
}