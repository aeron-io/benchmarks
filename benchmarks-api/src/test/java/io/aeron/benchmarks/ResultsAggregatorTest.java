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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static java.nio.file.Files.*;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Arrays.sort;
import static org.junit.jupiter.api.Assertions.*;
import static io.aeron.benchmarks.PersistedHistogram.*;

class ResultsAggregatorTest
{
    @TempDir
    Path tempDir;

    @Test
    void throwsNullPointerExceptionIfDirectoryIsNull()
    {
        assertThrows(NullPointerException.class, () -> new ResultsAggregator(null, 1));
    }

    @Test
    void throwsIllegalArgumentExceptionIfDirectoryDoesNotExist()
    {
        final Path dir = tempDir.resolve("non-existing");

        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> new ResultsAggregator(dir, 1));

        assertEquals("directory does not exist: " + dir, exception.getMessage());
    }

    @Test
    void throwsIllegalArgumentExceptionIfDirectoryIsNotADirectory() throws IOException
    {
        final Path file = createFile(tempDir.resolve("one.txt"));

        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> new ResultsAggregator(file, 1));

        assertEquals(file + " is not a directory!", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(doubles = { -1.0, 0.0, Double.NaN })
    void throwsIllegalArgumentExceptionIfOutputValueUnitScalingRatioIsInvalid(final double outputValueUnitScalingRatio)
    {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new ResultsAggregator(tempDir, outputValueUnitScalingRatio));

        assertEquals("report output value scale ratio must a positive number, got: " + outputValueUnitScalingRatio,
            exception.getMessage());
    }

    @Test
    void emptyDirectory() throws IOException
    {
        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1);

        aggregator.run();

        assertArrayEquals(new File[0], tempDir.toFile().listFiles());
    }

    @Test
    void directoryContainsNonHdrFiles() throws IOException
    {
        final Path otherFile = createFile(tempDir.resolve("other.txt"));

        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1);

        aggregator.run();

        assertArrayEquals(new File[]{ otherFile.toFile() }, tempDir.toFile().listFiles());
    }

    @Test
    void multipleHistogramFiles() throws IOException
    {
        saveToDisk("my.hdr", createHistogram(10, 25, 100, 555, 777, 999));
        saveToDisk("my.hdr.FAIL", createHistogram(2, 4, 555555, 1232343));
        saveToDisk("my-combined.hdr.FAIL", createHistogram(3, 4, 11, 1, 1, 22));
        saveToDisk("other.hdr", createHistogram(1, 45, 200));
        write(tempDir.resolve("other-report.hgrm"), new byte[]{ 0, -128, 127 }, CREATE_NEW);
        saveToDisk("hidden-4.ccc", createHistogram(0, 0, 1, 2, 3, 4, 5, 6));
        saveToDisk("hidden-6.ccc", createHistogram(0, 0, 6, 6, 0));

        final double reportOutputScalingRatio = 250.0;
        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, reportOutputScalingRatio);

        aggregator.run();

        final String[] aggregateFiles = tempDir.toFile().list((dir, name) ->
            name.endsWith(AGGREGATE_FILE_SUFFIX) || name.endsWith(AGGREGATE_FILE_SUFFIX + FAILED_FILE_SUFFIX));
        assertNotNull(aggregateFiles);
        sort(aggregateFiles);
        assertArrayEquals(
            new String[]{ "my-combined.hdr.FAIL", "other-combined.hdr" }, aggregateFiles);
        final Histogram myAggregate = createHistogram(2, 25, 100, 555, 777, 999, 555555, 1232343);
        assertEquals(myAggregate, loadFromDisk("my-combined.hdr.FAIL"));
        final Histogram otherAggregate = createHistogram(1, 45, 200);
        assertEquals(otherAggregate, loadFromDisk("other-combined.hdr"));

        final String[] reportFiles = tempDir.toFile().list((dir, name) ->
            name.endsWith(REPORT_FILE_SUFFIX) || name.endsWith(REPORT_FILE_SUFFIX + FAILED_FILE_SUFFIX));
        assertNotNull(reportFiles);
        sort(reportFiles);
        assertArrayEquals(new String[]{ "my-report.hgrm.FAIL", "other-report.hgrm" }, reportFiles);
        assertArrayEquals(outputPercentileDistribution(myAggregate, reportOutputScalingRatio),
            readAllBytes(tempDir.resolve("my-report.hgrm.FAIL")));
        assertArrayEquals(outputPercentileDistribution(otherAggregate, reportOutputScalingRatio),
            readAllBytes(tempDir.resolve("other-report.hgrm")));
    }

    private byte[] outputPercentileDistribution(final Histogram histogram, final double outputValueUnitScalingRatio)
    {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (PrintStream printStream = new PrintStream(bos))
        {
            histogram.outputPercentileDistribution(printStream, outputValueUnitScalingRatio);
        }

        return bos.toByteArray();
    }

    private Histogram createHistogram(final long startTimeMs, final long endTimeMs, final long... values)
    {
        final Histogram histogram = new Histogram(3);
        histogram.setStartTimeStamp(startTimeMs);
        histogram.setEndTimeStamp(endTimeMs);

        for (final long value : values)
        {
            histogram.recordValue(value);
        }

        return histogram;
    }

    private void saveToDisk(final String fileName, final Histogram histogram) throws FileNotFoundException
    {
        final HistogramLogWriter logWriter = new HistogramLogWriter(tempDir.resolve(fileName).toFile());
        try
        {
            logWriter.outputIntervalHistogram(
                histogram.getStartTimeStamp() / 1000.0, histogram.getEndTimeStamp() / 1000.0, histogram, 1);
        }
        finally
        {
            logWriter.close();
        }
    }

    private Histogram loadFromDisk(final String fileName) throws FileNotFoundException
    {
        try (HistogramLogReader logReader = new HistogramLogReader(tempDir.resolve(fileName).toFile()))
        {
            return (Histogram)logReader.nextIntervalHistogram();
        }
    }

    @Test
    void emptyHdrFiles() throws IOException
    {
        createDirectories(tempDir.resolve("run-0"));
        createDirectories(tempDir.resolve("run-1"));
        saveEmptyToDisk("run-0/my.hdr");
        saveEmptyToDisk("run-1/my.hdr");

        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1000.0);

        aggregator.run();

        assertTrue(exists(tempDir.resolve("my-combined.hdr")));
        assertFalse(exists(tempDir.resolve("my-report.hgrm")));
    }

    @Test
    void emptyFailedHdrFiles() throws IOException
    {
        createDirectories(tempDir.resolve("run-0"));
        createDirectories(tempDir.resolve("run-1"));
        saveEmptyToDisk("run-0/my.hdr");
        saveEmptyToDisk("run-1/my.hdr.FAIL");

        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1000.0);

        aggregator.run();

        assertTrue(exists(tempDir.resolve("my-combined.hdr.FAIL")));
        assertFalse(exists(tempDir.resolve("my-report.hgrm.FAIL")));
    }

    private void saveEmptyToDisk(final String fileName) throws FileNotFoundException
    {
        final HistogramLogWriter logWriter = new HistogramLogWriter(tempDir.resolve(fileName).toFile());
        logWriter.close();
    }
}
