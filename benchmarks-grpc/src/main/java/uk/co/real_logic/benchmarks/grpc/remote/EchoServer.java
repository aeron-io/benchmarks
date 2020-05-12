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
package uk.co.real_logic.benchmarks.grpc.remote;

import io.grpc.Server;
import org.agrona.SystemUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.IOException;
import java.net.SocketAddress;

import static io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forAddress;

public class EchoServer implements AutoCloseable
{
    private final Server server;

    public EchoServer(final SocketAddress serverAddress)
    {
        server = forAddress(serverAddress).addService(new EchoService()).build();
    }

    public void start() throws IOException
    {
        server.start();
        System.out.println("Server started, listening on: " + server.getListenSockets());
    }

    public void close() throws Exception
    {
        System.out.println("Shutting down server...");
        server.shutdownNow();
        server.awaitTermination();
    }

    public static void main(final String[] args) throws Exception
    {
        SystemUtil.loadPropertiesFiles(args);

        try (EchoServer server = new EchoServer(GrpcConfig.getServerAddress()))
        {
            server.start();

            new ShutdownSignalBarrier().await();
        }
    }

}
