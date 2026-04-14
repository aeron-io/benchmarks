/*
 * Copyright 2015-2026 Real Logic Limited.
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
package io.aeron.benchmarks.aeron;

import io.aeron.benchmarks.Configuration;
import org.HdrHistogram.ValueRecorder;
import org.agrona.concurrent.NanoClock;

import java.io.File;
import java.io.IOException;

public class ClusterWithOperationMessageTransceiver extends ClusterMessageTransceiver
{
    public static final String BENCHMARKS_CLUSTER_SCRIPT_NAME_PROP_NAME = "io.aeron.benchmarks.cluster.script.name";
    public static final String BENCHMARKS_CLUSTER_SCRIPT_FREQUENCY_PROP_NAME =
        "io.aeron.benchmarks.cluster.script.frequency.ms";

    private OperationScript operationScript;
    private Thread thread;

    public ClusterWithOperationMessageTransceiver(final NanoClock nanoClock, final ValueRecorder valueRecorder)
    {
        super(nanoClock, valueRecorder);
        System.out.println("running our ClusterWithOperationMessageTransceiver");
    }

    public void init(final Configuration configuration) throws Exception
    {
        super.init(configuration);

        final String scriptName = System.getProperty(BENCHMARKS_CLUSTER_SCRIPT_NAME_PROP_NAME);
        final Long scriptFrequency = Long.getLong(BENCHMARKS_CLUSTER_SCRIPT_FREQUENCY_PROP_NAME, 5_000L);

        if (null == scriptName)
        {
            throw new IllegalArgumentException(BENCHMARKS_CLUSTER_SCRIPT_NAME_PROP_NAME + " not specified");
        }

        System.out.println("Attempt to load script: " + scriptName);
        final File scriptFile = new File(scriptName);
        if (!scriptFile.exists() && !scriptFile.isFile())
        {
            throw new IllegalArgumentException(scriptName + " is not a valid script");
        }

        this.operationScript = new OperationScript(scriptFile, scriptFrequency);
        this.thread = new Thread(operationScript);
        thread.start();
    }

    public void destroy()
    {
        super.destroy();
        if (null != thread)
        {
            thread.interrupt();
        }
    }

    private record OperationScript(File scriptFile, long scriptFrequencyMs) implements Runnable
    {
        private static final long BREATHING_ROOM_MS = 100;

        @Override
        public void run()
        {
            System.out.println("Running script at frequency " + scriptFrequencyMs + "ms");

            final long scriptFrequencyNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(scriptFrequencyMs);

            long nextScheduledTime = System.nanoTime() + scriptFrequencyNanos;

            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    final long now = System.nanoTime();
                    final long waitNanos = nextScheduledTime - now;

                    if (waitNanos > 0)
                    {
                        java.util.concurrent.TimeUnit.NANOSECONDS.sleep(waitNanos);
                    }
                    else
                    {
                        System.out.println("!!  Missed our deadline to run script - skip to next time");
                        nextScheduledTime += scriptFrequencyNanos;
                        continue;
                    }

                    System.out.println("Running script: " + scriptFile.getAbsolutePath());

                    final ProcessBuilder pb = new ProcessBuilder()
                        .command(scriptFile.getAbsolutePath());
                    pb.inheritIO();
                    pb.start().waitFor();

                    System.out.println("Completed script");

                    // Schedule relative to when this run was supposed to start.
                    nextScheduledTime += java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(scriptFrequencyMs);
                }
                catch (final IOException e)
                {
                    e.printStackTrace(System.err);
                    return;
                }
                catch (final InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
