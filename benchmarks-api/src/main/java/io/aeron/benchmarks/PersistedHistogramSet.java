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
import org.HdrHistogram.SingleWriterRecorder;
import org.HdrHistogram.ValueRecorder;
import org.agrona.CloseHelper;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * A set that creates and tracks named {@link PersistedHistogram} instances.
 * <p>
 * Used by {@link MessageTransceiver} implementations that need multiple histograms
 * (e.g. one per receiver in a fan-out benchmark). The {@link LoadTestRig} uses this
 * factory to save all histograms at the end of a run.
 */
public final class PersistedHistogramSet implements AutoCloseable
{
    private final Configuration configuration;
    private final Map<String, PersistedHistogram> histograms = new LinkedHashMap<>();

    public PersistedHistogramSet(final Configuration configuration)
    {
        this.configuration = configuration;
    }

    /**
     * Wrap a pre-existing {@link PersistedHistogram} in a factory.
     * Used for backward compatibility with constructors that receive a single histogram.
     *
     * @param configuration     the Configuration
     * @param histogram        the existing histogram.
     * @return a factory containing the histogram under the name "result".
     */
    public static PersistedHistogramSet wrap(final Configuration configuration, final PersistedHistogram histogram)
    {
        final PersistedHistogramSet factory = new PersistedHistogramSet(configuration);
        factory.histograms.put(configuration.outputFileNamePrefix(), histogram);
        return factory;
    }

    /**
     * Create a named {@link ValueRecorder} backed by a new {@link PersistedHistogram}.
     *
     * @param name the name used as the file prefix when saving. Must be unique.
     * @return a {@link ValueRecorder} that records into the named histogram.
     * @throws IllegalArgumentException if a histogram with the same name already exists.
     */
    public PersistedHistogram create(final String name)
    {
        if (histograms.containsKey(name))
        {
            throw new IllegalArgumentException("Histogram already exists: " + name);
        }

        final PersistedHistogram result;
        final int numberOfSignificantValueDigits = 3;
        if (configuration.trackHistory())
        {
            final SingleWriterRecorder recorder = new SingleWriterRecorder(numberOfSignificantValueDigits);
            result = new LoggingPersistedHistogram(configuration.outputDirectory(), name, recorder);
        }
        else
        {
            final Histogram histogram = new Histogram(HOURS.toNanos(1), numberOfSignificantValueDigits);
            result = new SinglePersistedHistogram(histogram);
        }

        histograms.put(name, result);
        return result;
    }

    /**
     * Reset all histograms (e.g. after warmup).
     */
    public void reset()
    {
        for (final PersistedHistogram histogram : histograms.values())
        {
            histogram.reset();
        }
    }

    /**
     * Print percentile distributions for all histograms.
     *
     * @param out        the output stream.
     */
    public void outputPercentileDistributions(final PrintStream out)
    {
        final double scaleRatio = Configuration.outputScaleRatio(configuration.outputTimeUnit());

        for (final Map.Entry<String, PersistedHistogram> entry : histograms.entrySet())
        {
            final String name = entry.getKey();
            final PersistedHistogram histogram = entry.getValue();
            out.printf("%nHistogram [%s]:%n", name);
            histogram.outputPercentileDistribution(out, scaleRatio);
        }
    }

    /**
     * Save all histograms to files.
     *
     * @param status          the benchmark status.
     */
    public void saveAll(final PersistedHistogram.Status status) throws IOException
    {
        final Path outputDirectory = configuration.outputDirectory();
        for (final Map.Entry<String, PersistedHistogram> entry : histograms.entrySet())
        {
            final PersistedHistogram histogram = entry.getValue();
            final String name = entry.getKey();
            histogram.saveToFile(outputDirectory, name, status);
        }
    }

    public void close() throws Exception
    {
        for (final PersistedHistogram histogram : histograms.values())
        {
            CloseHelper.close(histogram);
        }
    }
}