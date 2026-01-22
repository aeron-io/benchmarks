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
import org.HdrHistogram.ValueRecorder;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * PersistedHistogram that contains a single histogram instance.
 */
public final class SinglePersistedHistogram implements PersistedHistogram
{
    private final Histogram histogram;

    /**
     * A single instance persisted histogram with the supplied histogram.
     *
     * @param histogram to record values.
     */
    public SinglePersistedHistogram(final Histogram histogram)
    {
        this.histogram = histogram;
    }

    /**
     * Produce textual representation of the value distribution of histogram data by percentile. The distribution is
     * output with exponentially increasing resolution, with each exponentially decreasing half-distance containing
     * five (5) percentile reporting tick points.
     *
     * @param printStream                 stream into which the distribution will be output.
     * @param outputValueUnitScalingRatio the scaling factor by which to divide histogram recorded values units in
     *                                    output.
     */
    public void outputPercentileDistribution(final PrintStream printStream, final double outputValueUnitScalingRatio)
    {
        histogram.outputPercentileDistribution(printStream, outputValueUnitScalingRatio);
    }

    /**
     * Save histogram into a file on disc. Uses provided {@code namePrefix} and appending an <em>index</em> to ensure
     * unique name, i.e. {@code namePrefix_index.hdr}. For example the first file with a given prefix
     * will be stored under index zero (e.g. {@code my-file_0.hdr}) and the fifth under index number four
     * (e.g. {@code my-file_4.hdr}).
     *
     * @param outputDirectory output directory where files should be stored.
     * @param namePrefix      name prefix to use when creating a file.
     * @param status
     * @return created file.
     * @throws NullPointerException     if {@code null == outputDirectory || null == namePrefix}.
     * @throws IllegalArgumentException if {@code namePrefix} is blank.
     * @throws IOException              if IO error occurs.
     */
    public Path saveToFile(final Path outputDirectory, final String namePrefix, final Status status) throws IOException
    {
        requireNonNull(outputDirectory);

        final String prefix = namePrefix.trim();
        if (prefix.isEmpty())
        {
            throw new IllegalArgumentException("Name prefix cannot be blank!");
        }

        return PersistedHistogram.saveHistogramToFile(histogram, outputDirectory, prefix, status);
    }

    /**
     * {@inheritDoc}
     */
    public ValueRecorder valueRecorder()
    {
        return histogram;
    }

    /**
     * {@inheritDoc}
     */
    public void reset()
    {
        histogram.reset();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }
}
