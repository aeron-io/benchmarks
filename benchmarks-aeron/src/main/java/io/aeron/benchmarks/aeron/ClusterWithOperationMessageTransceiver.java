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

        this.operationScript = new OperationScript(scriptFile);
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

    private record OperationScript(File scriptFile) implements Runnable
    {
        public void run()
        {
            System.out.println("starting thread OperationScript");
            while (!Thread.currentThread().isInterrupted())
            {
                try
                {
                    //noinspection BusyWait
                    Thread.sleep(5_000);
                    System.out.println("Running script: " + scriptFile.getAbsolutePath());
                    final ProcessBuilder pb = new ProcessBuilder().command(scriptFile.getAbsolutePath());
                    pb.inheritIO();
                    pb.start().waitFor();
                    System.out.println("Completed script");

                }
                catch (final IOException e)
                {
                    e.printStackTrace(System.err);
                    return;
                }
                catch (final InterruptedException ignore)
                {
                    // must return here to end the thread
                    return;
                }
            }
        }
    }
}
