/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.benchmarks.rtt;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Arrays.sort;
import static org.junit.jupiter.api.Assertions.*;
import static uk.co.real_logic.benchmarks.rtt.RttHistogram.AGGREGATE_FILE_SUFFIX;

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

        assertEquals("Directory " + dir + " does not exist!", exception.getMessage());
    }

    @Test
    void throwsIllegalArgumentExceptionIfDirectoryIsNotADirectory() throws IOException
    {
        final Path file = Files.createFile(tempDir.resolve("one.txt"));

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

        assertEquals("Output value scale ratio must a positive number, got: " + outputValueUnitScalingRatio,
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
        final Path otherFile = Files.createFile(tempDir.resolve("other.txt"));

        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1);

        aggregator.run();

        assertArrayEquals(new File[]{ otherFile.toFile() }, tempDir.toFile().listFiles());
    }

    @Test
    void multipleHistogramFiles() throws IOException
    {
        saveToDisc("my-5.hdr", createHistogram(10, 25, 100, 555, 777, 999));
        saveToDisc("my-0.hdr", createHistogram(2, 4, 555555, 1232343));
        saveToDisc("my-combined.hdr", createHistogram(3, 4, 11, 1, 1, 22));
        saveToDisc("other-78.hdr", createHistogram(1, 45, 200));
        saveToDisc("hidden-4.ccc", createHistogram(0, 0, 1, 2, 3, 4, 5, 6));
        saveToDisc("hidden-6.ccc", createHistogram(0, 0, 6, 6, 0));

        final ResultsAggregator aggregator = new ResultsAggregator(tempDir, 1);

        aggregator.run();

        final String[] aggregateFiles = tempDir.toFile().list((dir, name) -> name.endsWith(AGGREGATE_FILE_SUFFIX));
        sort(aggregateFiles);
        assertArrayEquals(new String[]{ "my-combined.hdr", "other-combined.hdr" },
            aggregateFiles);
        assertEquals(createHistogram(2, 25, 100, 555, 777, 999, 555555, 1232343),
            loadFromDisc("my-combined.hdr"));
        assertEquals(createHistogram(1, 45, 200), loadFromDisc("other-combined.hdr"));
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

    private void saveToDisc(final String fileName, final Histogram histogram) throws FileNotFoundException
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

    private Histogram loadFromDisc(final String fileName) throws FileNotFoundException
    {
        final HistogramLogReader logReader = new HistogramLogReader(tempDir.resolve(fileName).toFile());
        try
        {
            return (Histogram)logReader.nextIntervalHistogram();
        }
        finally
        {
            logReader.close();
        }
    }

}