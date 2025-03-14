/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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
package uk.co.real_logic.benchmarks.aeron.remote;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public final class FailoverControlServer implements AutoCloseable
{
    private final DatagramChannel channel;
    private final Thread thread;
    private final int memberId;
    private final AtomicReference<Cluster.Role> roleRef;
    private final Component<ConsensusModule> consensusModule;
    private final Component<ClusteredServiceContainer> clusteredServiceContainer;
    private final ErrorHandler errorHandler;

    private boolean closed;

    public FailoverControlServer(
        final String hostname,
        final int port,
        final int memberId,
        final AtomicReference<Cluster.Role> roleRef,
        final Component<ConsensusModule> consensusModule,
        final Component<ClusteredServiceContainer> clusteredServiceContainer,
        final ErrorHandler errorHandler)
    {
        this.memberId = memberId;
        this.roleRef = roleRef;
        this.consensusModule = consensusModule;
        this.clusteredServiceContainer = clusteredServiceContainer;
        this.errorHandler = errorHandler;

        try
        {
            channel = createChannel(hostname, port);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }

        thread = new Thread(this::run, getClass().getSimpleName());
    }

    private static DatagramChannel createChannel(final String hostname, final int port) throws IOException
    {
        final DatagramChannel channel = DatagramChannel.open();

        try
        {
            channel.bind(new InetSocketAddress(hostname, port));
        }
        catch (final IOException e)
        {
            try
            {
                channel.close();
            }
            catch (final IOException ce)
            {
                e.addSuppressed(ce);
            }

            throw e;
        }

        return channel;
    }

    public void start()
    {
        thread.start();
    }

    public void close()
    {
        thread.interrupt();
        try
        {
            thread.join(3000);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        try
        {
            channel.close();
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException("failed to close channel", e);
        }
    }

    public InetSocketAddress getLocalAddress()
    {
        try
        {
            return (InetSocketAddress)channel.getLocalAddress();
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private void run()
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(2048);

        while (!Thread.currentThread().isInterrupted())
        {
            try
            {
                byteBuffer.clear();
                channel.receive(byteBuffer);
                byteBuffer.flip();

                final int command = byteBuffer.getInt();
                if (command == FailoverConstants.LEADER_STEP_DOWN_COMMAND && roleRef.get() == Cluster.Role.LEADER)
                {
                    CloseHelper.closeAll(consensusModule, clusteredServiceContainer);
                    closed = true;
                }
                else if (command == FailoverConstants.RESTART_COMMAND && closed)
                {
                    consensusModule.start();
                    clusteredServiceContainer.start();
                    closed = false;
                }
                else if (command == FailoverConstants.CYCLE_NODE_COMMAND)
                {
                    final int nodeId = byteBuffer.getInt();
                    if (nodeId == memberId)
                    {
                        CloseHelper.closeAll(consensusModule, clusteredServiceContainer);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000)); // 2 timer intervals
                        consensusModule.start();
                        clusteredServiceContainer.start();
                    }
                }
            }
            catch (final AsynchronousCloseException e)
            {
                break;
            }
            catch (final IOException e)
            {
                errorHandler.onError(e);
            }
        }
    }
}
