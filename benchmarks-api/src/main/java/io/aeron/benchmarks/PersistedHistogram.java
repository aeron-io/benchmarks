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
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.SingleWriterRecorder;
import org.HdrHistogram.ValueRecorder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static java.util.concurrent.TimeUnit.HOURS;

public interface PersistedHistogram extends AutoCloseable
{
    /**
     * File extension used to persist histogram values on disc.
     */
    String FILE_EXTENSION = ".hdr";

    /**
     * File extension used to persist history of the histogram values on disc.
     */
    String HISTORY_FILE_EXTENSION = ".hdr.csv";

    /**
     * File suffix for aggregated histogram.
     */
    String AGGREGATE_FILE_SUFFIX = "-combined" + FILE_EXTENSION;

    /**
     * File name suffix for the report file, i.e. the plottable results.
     */
    String REPORT_FILE_SUFFIX = "-report.hgrm";

    /**
     * File name suffix for failed benchmark results.
     */
    String FAILED_FILE_SUFFIX = ".FAIL";

    enum Status
    {
        OK,
        FAIL
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
    void outputPercentileDistribution(PrintStream printStream, double outputValueUnitScalingRatio);

    /**
     * Save histogram into a file on disc. Uses provided {@code namePrefix} and appending an <em>index</em> to ensure
     * unique name, i.e. {@code namePrefix_index.hdr}. For example the first file with a given prefix
     * will be stored under index zero (e.g. {@code my-file_0.hdr}) and the fifth under index number four
     * (e.g. {@code my-file_4.hdr}).
     *
     * @param outputDirectory output directory where files should be stored.
     * @param namePrefix      name prefix to use when creating a file.
     * @param status          of the execution.
     * @return created file.
     * @throws NullPointerException     if {@code null == outputDirectory || null == namePrefix}.
     * @throws IllegalArgumentException if {@code namePrefix} is blank.
     * @throws IOException              if IO error occurs.
     */
    Path saveToFile(Path outputDirectory, String namePrefix, Status status) throws IOException;

    /**
     * Provide a value recorder to be used for measurements. Values recorded through this interface will be persisted
     * by this PersistedHistogram.
     *
     * @return the value recorder.
     */
    ValueRecorder valueRecorder();

    /**
     * Reset the histogram recording, generally between warmup and real runs.
     */
    void reset();

    /**
     * {@inheritDoc}
     */
    void close();

    static Path saveHistogramToFile(
        final Histogram histogram, final Path outputDirectory, final String prefix, final Status status)
        throws IOException
    {
        return saveToFile(histogram, outputDirectory.resolve(fileName(status, prefix, FILE_EXTENSION)));
    }

    static String fileName(final Status status, final String fileNamePrefix, final String fileExtension)
    {
        final String name = fileNamePrefix + fileExtension;
        if (Status.FAIL == status)
        {
            return name + FAILED_FILE_SUFFIX;
        }
        return name;
    }

    static boolean isHdrFile(final String fileName, final String fileExtension)
    {
        final int failedSuffix = fileName.lastIndexOf(FAILED_FILE_SUFFIX);
        final int lengthWithoutSuffix = failedSuffix > 0 ? failedSuffix : fileName.length();
        return fileName.startsWith(fileExtension, lengthWithoutSuffix - fileExtension.length()) &&
            !fileName.startsWith(
                AGGREGATE_FILE_SUFFIX, lengthWithoutSuffix - AGGREGATE_FILE_SUFFIX.length());
    }

    static Path saveToFile(final Histogram histogram, final Path file)
        throws FileNotFoundException
    {
        final HistogramLogWriter logWriter = new HistogramLogWriter(file.toFile());
        try
        {
            logWriter.outputIntervalHistogram(
                histogram.getStartTimeStamp() / 1000.0,
                histogram.getEndTimeStamp() / 1000.0,
                histogram,
                1.0);
        }
        finally
        {
            logWriter.close();
        }

        return file;
    }

    @SuppressWarnings("checkstyle:indentation")
    static PersistedHistogram newPersistedHistogram(final Configuration configuration)
    {
        final int numberOfSignificantValueDigits = 3;
        if (configuration.trackHistory())
        {
            return new LoggingPersistedHistogram(
                configuration.outputDirectory(),
                configuration.outputFileNamePrefix(),
                new SingleWriterRecorder(numberOfSignificantValueDigits));
        }
        else
        {
            return new SinglePersistedHistogram(new Histogram(HOURS.toNanos(1), numberOfSignificantValueDigits));
        }
    }
}
