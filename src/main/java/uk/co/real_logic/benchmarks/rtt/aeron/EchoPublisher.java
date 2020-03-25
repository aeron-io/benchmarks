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
package uk.co.real_logic.benchmarks.rtt.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.SystemUtil;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Publication.*;
import static org.agrona.CloseHelper.closeAll;

public final class EchoPublisher implements AutoCloseable
{
    private final ExclusivePublication publication;
    private final Subscription subscription;
    private final AtomicBoolean running;
    private final MessagePumpDriver driver;
    private final boolean ownsDriver;

    EchoPublisher(final AtomicBoolean running)
    {
        this(running, new MessagePumpDriver(), true);
    }

    EchoPublisher(final AtomicBoolean running, final MessagePumpDriver driver)
    {
        this(running, driver, false);
    }

    private EchoPublisher(final AtomicBoolean running, final MessagePumpDriver driver, final boolean ownsDriver)
    {
        this.running = running;
        this.driver = driver;
        this.ownsDriver = ownsDriver;

        final Aeron aeron = driver.aeron();
        final MessagePumpConfiguration configuration = driver.configuration();

        publication = aeron.addExclusivePublication(configuration.receiverChannel, configuration.receiverStreamId);

        subscription = aeron.addSubscription(configuration.senderChannel, configuration.senderStreamId);

        while (!subscription.isConnected() || !publication.isConnected())
        {
            Thread.yield();
        }
    }

    public void run()
    {
        final AtomicBoolean running = this.running;
        final Subscription subscription = this.subscription;
        final ExclusivePublication publication = this.publication;
        final int frameCountLimit = driver.configuration().frameCountLimit;

        final FragmentHandler dataHandler =
            (buffer, offset, length, header) ->
            {
                long result;
                while ((result = publication.offer(buffer, offset, length)) < 0L)
                {
                    checkResult(result);
                }
            };

        while (running.get())
        {
            subscription.poll(dataHandler, frameCountLimit);
        }
    }

    public void close()
    {
        closeAll(subscription, publication);

        if (ownsDriver)
        {
            driver.close();
        }
    }

    public static void main(final String[] args)
    {
        SystemUtil.loadPropertiesFiles(args);

        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler for graceful shutdown.
        SigInt.register(() -> running.set(false));

        try (EchoPublisher server = new EchoPublisher(running))
        {
            server.run();
        }
    }

    private static void checkResult(final long result)
    {
        if (result == CLOSED ||
            result == NOT_CONNECTED ||
            result == MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("Publication error: " + result);
        }
    }
}
