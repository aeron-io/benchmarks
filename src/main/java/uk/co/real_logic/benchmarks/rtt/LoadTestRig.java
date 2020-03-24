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
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * {@code LoadTestRig} class is the core of the RTT benchmark. It is responsible for running benchmark against provided
 * {@link MessagePump} instance using given {@link Configuration}.
 */
public final class LoadTestRig
{
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);
    private final Configuration configuration;
    private final MessagePump messagePump;
    private final NanoClock clock;
    private final PrintStream out;
    private final Histogram histogram;
    private final MessageRecorder messageRecorder;

    public LoadTestRig(final Configuration configuration, final Class<? extends MessagePump> messagePumpClass)
    {
        this.configuration = requireNonNull(configuration);
        requireNonNull(messagePumpClass);
        this.clock = SystemNanoClock.INSTANCE;
        this.out = System.out;
        histogram = new Histogram(
            max(configuration.iterations(), configuration.warmUpIterations()) * NANOS_PER_SECOND, 3);
        messageRecorder = new MessageRecorder()
        {
            private long time;

            public void record(final long timestamp)
            {
                long time = this.time;
                if (0 == time)
                {
                    this.time = time = clock.nanoTime();
                }
                histogram.recordValue(time - timestamp);
            }

            public void reset()
            {
                time = 0;
            }
        };
        try
        {
            this.messagePump = messagePumpClass.getConstructor(MessageRecorder.class)
                .newInstance(messageRecorder);
        }
        catch (final ReflectiveOperationException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            throw new Error();
        }
    }

    LoadTestRig(
        final Configuration configuration,
        final NanoClock clock,
        final PrintStream out,
        final Histogram histogram,
        final MessageRecorder messageRecorder,
        final MessagePump messagePump)
    {
        this.configuration = configuration;
        this.clock = clock;
        this.out = out;
        this.histogram = histogram;
        this.messageRecorder = messageRecorder;
        this.messagePump = messagePump;
    }

    /**
     * Run the benchmark and print histogram of the RTT values at the end.
     *
     * @throws Exception in case of any error from the {@link MessagePump}
     */
    public void run() throws Exception
    {
        messagePump.init(configuration);
        try
        {
            final AtomicLong sentMessages = new AtomicLong();

            // Warm up
            if (configuration.warmUpIterations() > 0)
            {
                out.printf("Running warm up for %,d iterations of %,d messages with burst size of %,d...%n",
                    configuration.warmUpIterations(),
                    configuration.warmUpNumberOfMessages(),
                    configuration.batchSize());
                doRun(configuration.warmUpIterations(), configuration.warmUpNumberOfMessages(), sentMessages);

                histogram.reset();
                sentMessages.set(0);
            }

            // Measurement
            out.printf("%nRunning measurement for %,d iterations of %,d messages with burst size of %,d...%n",
                configuration.iterations(),
                configuration.numberOfMessages(),
                configuration.batchSize());
            doRun(configuration.iterations(), configuration.numberOfMessages(), sentMessages);

            out.printf("%nHistogram of RTT latencies in microseconds.%n");
            histogram.outputPercentileDistribution(out, 1000.0);
        }
        finally
        {
            messagePump.destroy();
        }
    }

    private void doRun(
        final int iterations,
        final int messages,
        final AtomicLong sentMessages)
    {
        final CompletableFuture<?> receiverTask = runAsync(() -> receive(sentMessages));
        sentMessages.set(send(iterations, messages));
        receiverTask.join();
    }

    void receive(final AtomicLong sentMessages)
    {
        final MessageRecorder messageRecorder = this.messageRecorder;
        final MessagePump messagePump = this.messagePump;
        final IdleStrategy idleStrategy = configuration.receiverIdleStrategy();

        long sent = 0;
        long received = 0;
        while (true)
        {
            messageRecorder.reset();
            final int count = messagePump.receive();
            if (count > 0)
            {
                received += count;
                idleStrategy.reset();
            }
            else
            {
                idleStrategy.idle();
            }
            if (0 == sent)
            {
                sent = sentMessages.get();
            }
            if (0 != sent && received >= sent)
            {
                break;
            }
        }
    }

    long send(final int iterations, final int numberOfMessages)
    {
        final MessagePump messagePump = this.messagePump;
        final NanoClock clock = this.clock;
        final int burstSize = configuration.batchSize();
        final int messageSize = configuration.messageLength();
        final IdleStrategy idleStrategy = configuration.senderIdleStrategy();
        final long sendInterval = NANOS_PER_SECOND * burstSize / numberOfMessages;
        final long totalNumberOfMessages = (long)iterations * numberOfMessages;

        final long startTime = clock.nanoTime();
        final long endTime = startTime + iterations * NANOS_PER_SECOND;
        long sentMessages = 0;
        long timestamp = startTime;
        long now = startTime;
        long nextReportTime = startTime + NANOS_PER_SECOND;

        while (true)
        {
            final int batchSize = (int)min(totalNumberOfMessages - sentMessages, burstSize);
            int sent = messagePump.send(batchSize, messageSize, timestamp);
            if (sent < batchSize)
            {
                idleStrategy.reset();
                do
                {
                    idleStrategy.idle();
                }
                while ((sent += messagePump.send(batchSize - sent, messageSize, timestamp)) < batchSize);
            }
            sentMessages += batchSize;
            if (totalNumberOfMessages == sentMessages)
            {
                reportProgress(startTime, now, sentMessages);
                break;
            }

            timestamp += sendInterval;

            if (now < timestamp)
            {
                idleStrategy.reset();
                while ((now = clock.nanoTime()) < timestamp && now < endTime)
                {
                    idleStrategy.idle();
                }
                if (now >= endTime)
                {
                    break;
                }
            }

            if (now >= nextReportTime)
            {
                final int elapsedSeconds = reportProgress(startTime, now, sentMessages);
                nextReportTime = startTime + (elapsedSeconds + 1) * NANOS_PER_SECOND;
            }
        }

        return sentMessages;
    }

    private int reportProgress(final long startTime, final long now, final long sentMessages)
    {
        final int elapsedSeconds = (int)round((double)(now - startTime) / NANOS_PER_SECOND);
        final long sendRate = 0 == elapsedSeconds ? sentMessages : sentMessages / elapsedSeconds;
        out.format("Send rate %,d msg/sec%n", sendRate);
        return elapsedSeconds;
    }

    public static void main(final String[] args) throws Exception
    {
        SystemUtil.loadPropertiesFiles(args);

        final Configuration configuration = Configuration.fromSystemProperties();
        new LoadTestRig(configuration, configuration.messagePumpClass()).run();
    }
}
