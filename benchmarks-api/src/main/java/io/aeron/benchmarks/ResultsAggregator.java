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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.lang.Double.*;
import static java.nio.file.Files.*;
import static java.util.stream.Collectors.groupingBy;
import static io.aeron.benchmarks.PersistedHistogram.*;

public final class ResultsAggregator
{
    private final Path directory;
    private final double reportOutputScalingRatio;

    public ResultsAggregator(final Path directory, final double reportOutputScalingRatio)
    {
        if (!exists(directory))
        {
            throw new IllegalArgumentException("directory does not exist: " + directory.toAbsolutePath());
        }

        if (!isDirectory(directory))
        {
            throw new IllegalArgumentException(directory.toAbsolutePath() + " is not a directory!");
        }

        if (isNaN(reportOutputScalingRatio) || compare(reportOutputScalingRatio, 0.0) <= 0)
        {
            throw new IllegalArgumentException(
                "report output value scale ratio must a positive number, got: " + reportOutputScalingRatio);
        }

        this.directory = directory;
        this.reportOutputScalingRatio = reportOutputScalingRatio;
    }

    public void run() throws IOException
    {
        try (Stream<Path> stream = walk(directory))
        {
            final Map<String, List<Path>> byPrefix = stream
                .filter((path) ->
                {
                    if (!isRegularFile(path))
                    {
                        return false;
                    }
                    final String fileName = path.getFileName().toString();
                    return isHdrFile(fileName, FILE_EXTENSION);
                })
                .collect(groupingBy((file) ->
                {
                    String fileName = file.getFileName().toString();
                    if (fileName.endsWith(FAILED_FILE_SUFFIX))
                    {
                        fileName = fileName.substring(0, fileName.length() - FAILED_FILE_SUFFIX.length());
                    }
                    fileName = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());
                    if (fileName.endsWith(AGGREGATE_FILE_SUFFIX))
                    {
                        fileName = fileName.substring(0, fileName.length() - AGGREGATE_FILE_SUFFIX.length());
                    }
                    return fileName;
                }));

            for (final Entry<String, List<Path>> e : byPrefix.entrySet())
            {
                final Histogram aggregate = aggregateHistograms(e);
                final String filePrefix = e.getKey();
                String suffix = "";
                for (final Path p : e.getValue())
                {
                    final String fileName = p.getFileName().toString();
                    if (fileName.endsWith(FAILED_FILE_SUFFIX))
                    {
                        suffix = FAILED_FILE_SUFFIX;
                        break;
                    }
                }
                if (null == aggregate)
                {
                    createFile(directory.resolve(filePrefix + AGGREGATE_FILE_SUFFIX + suffix));
                }
                else
                {
                    saveToFile(aggregate, directory.resolve(filePrefix + AGGREGATE_FILE_SUFFIX + suffix));
                    createReportFile(aggregate, directory.resolve(filePrefix + REPORT_FILE_SUFFIX + suffix));
                }
            }
        }
    }

    private Histogram aggregateHistograms(final Entry<String, List<Path>> entry) throws FileNotFoundException
    {
        Histogram aggregate = null;

        for (final Path file : entry.getValue())
        {
            try (HistogramLogReader logReader = new HistogramLogReader(file.toFile()))
            {
                while (logReader.hasNext())
                {
                    final Histogram histogram = (Histogram)logReader.nextIntervalHistogram();
                    if (null == aggregate)
                    {
                        aggregate = histogram;
                    }
                    else
                    {
                        aggregate.add(histogram);
                    }
                }
            }
        }

        return aggregate;
    }

    private void createReportFile(final Histogram aggregate, final Path reportFile) throws IOException
    {
        try (FileOutputStream fos = new FileOutputStream(reportFile.toFile(), false);
            PrintStream printStream = new PrintStream(fos))
        {
            aggregate.outputPercentileDistribution(printStream, reportOutputScalingRatio);
        }
    }

    public static void main(final String[] args) throws IOException
    {
        if (args.length < 1 || args.length > 2)
        {
            printHelp();
            System.exit(-1);
        }

        final Path directory = Paths.get(args[0]);
        final ResultsAggregator resultsAggregator = new ResultsAggregator(
            directory, args.length == 2 ? parseDouble(args[1]) : 1000.0);

        resultsAggregator.run();
    }

    private static void printHelp()
    {
        System.out.println("Usage: <input-dir> [reportOutputScalingRatio] - aggregates multiple histogram files");
        System.out.println("  from the `input-dir` into a single file grouping them by a common prefix.");
        System.out.println("  For each aggregate file it also produces a report file which can be plotted using");
        System.out.println("  http://hdrhistogram.github.io/HdrHistogram/plotFiles.html.");
        System.out.println("  For example if the `input-dir` contains files `my-0.hdr`, `my-6.hdr` and `other-3.hdr`");
        System.out.println("  the result of executing the aggregator will be four new files in the same directory,");
        System.out.println("  i.e. `my-combined.hdr` (combination of the `my-0.hdr` and `my-6.hdr`) and the report");
        System.out.println("  file `my-report.hgrm`; `other-combined.hdr` (contains the `other-0.hdr` histogram) and");
        System.out.println("  `other-report.hgrm`.");
        System.out.println();
        System.out.println("  Input arguments:");
        System.out.println("  `input-dir` - is the directory containing results files to be aggregated");
        System.out.println("  `reportOutputScalingRatio` - is the scaling factor by which to divide histogram");
        System.out.println("  recorded values units to produce the report file.");
        System.out.println("  Default value is 1000.0, i.e. the output will be in microseconds.");
    }
}
