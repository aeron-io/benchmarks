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

import org.HdrHistogram.ValueRecorder;
import org.agrona.CloseHelper;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.aeron.benchmarks.PersistedHistogram.newPersistedHistogram;

/**
 * A set that creates and tracks named {@link PersistedHistogram} instances.
 * <p>
 * Used by {@link MessageTransceiver} implementations that need multiple histograms
 * (e.g. one per receiver in a fan-out benchmark). The {@link LoadTestRig} uses this
 * factory to save all histograms at the end of a run.
 */
public final class PersistedHistogramSet
{
    private final Configuration configuration;
    private final Map<String, PersistedHistogram> histograms = new LinkedHashMap<>();

    public PersistedHistogramSet(final Configuration configuration)
    {
        this.configuration = configuration;
    }

    private PersistedHistogramSet()
    {
        this.configuration = null;
    }

    /**
     * Wrap a pre-existing {@link PersistedHistogram} in a factory.
     * Used for backward compatibility with constructors that receive a single histogram.
     *
     * @param histogram the existing histogram.
     * @return a factory containing the histogram under the name "result".
     */
    public static PersistedHistogramSet wrap(final PersistedHistogram histogram)
    {
        final PersistedHistogramSet factory = new PersistedHistogramSet();
        factory.histograms.put("result", histogram);
        return factory;
    }

    /**
     * Create a named {@link ValueRecorder} backed by a new {@link PersistedHistogram}.
     *
     * @param name the name used as the file prefix when saving. Must be unique.
     * @return a {@link ValueRecorder} that records into the named histogram.
     * @throws IllegalArgumentException if a histogram with the same name already exists.
     * @throws IllegalStateException if factory was created via {@link #wrap} (no configuration available).
     */
    public ValueRecorder create(final String name)
    {
        if (null == configuration)
        {
            throw new IllegalStateException("Cannot create new histograms on a wrapped factory");
        }

        if (histograms.containsKey(name))
        {
            throw new IllegalArgumentException("Histogram already exists: " + name);
        }

        final PersistedHistogram histogram = newPersistedHistogram(configuration);
        histograms.put(name, histogram);
        return histogram.valueRecorder();
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
     * @param scaleRatio the scale ratio for the output time unit.
     */
    public void outputPercentileDistributions(final PrintStream out, final double scaleRatio)
    {
        for (final Map.Entry<String, PersistedHistogram> entry : histograms.entrySet())
        {
            out.printf("%nHistogram [%s]:%n", entry.getKey());
            entry.getValue().outputPercentileDistribution(out, scaleRatio);
        }
    }

    /**
     * Save all histograms to files.
     *
     * @param outputDirectory the output directory.
     * @param status          the benchmark status.
     */
    public void saveAll(final Path outputDirectory, final PersistedHistogram.Status status) throws IOException
    {
        for (final Map.Entry<String, PersistedHistogram> entry : histograms.entrySet())
        {
            PersistedHistogram histogram = entry.getValue();
            String name = entry.getKey();
            histogram.saveToFile(outputDirectory, name, status);
        }
    }

    /**
     * Close all histograms.
     */
    public void closeAll()
    {
        for (final PersistedHistogram histogram : histograms.values())
        {
            CloseHelper.close(histogram);
        }
    }
}