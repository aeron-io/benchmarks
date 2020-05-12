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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import uk.co.real_logic.benchmarks.remote.Configuration;
import uk.co.real_logic.benchmarks.remote.MessageRecorder;
import uk.co.real_logic.benchmarks.remote.MessageTransceiver;
import uk.co.real_logic.benchmarks.grpc.remote.EchoBenchmarksGrpc.EchoBenchmarksBlockingStub;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static uk.co.real_logic.benchmarks.remote.Configuration.MIN_MESSAGE_LENGTH;
import static uk.co.real_logic.benchmarks.grpc.remote.GrpcConfig.getServerChannel;

public class BlockingMessageTrasceiver extends MessageTransceiver
{
    private ManagedChannel serverChannel;
    private EchoBenchmarksBlockingStub blockingClient;
    private EchoMessage.Builder messageBuilder;
    private ByteString payload;

    public BlockingMessageTrasceiver(final MessageRecorder messageRecorder)
    {
        super(messageRecorder);
    }

    public void init(final Configuration configuration) throws Exception
    {
        serverChannel = getServerChannel();
        blockingClient = EchoBenchmarksGrpc.newBlockingStub(serverChannel);

        messageBuilder = EchoMessage.newBuilder();
        final int payloadLength = configuration.messageLength() - MIN_MESSAGE_LENGTH - 4 /* array length field */;
        if (payloadLength <= 0)
        {
            payload = ByteString.EMPTY;
        }
        else
        {
            final byte[] bytes = new byte[payloadLength];
            ThreadLocalRandom.current().nextBytes(bytes);
            payload = copyFrom(bytes);
        }
    }

    public void destroy() throws Exception
    {
        blockingClient = null;
        serverChannel.shutdown().awaitTermination(1, MINUTES);
    }

    public int send(final int numberOfMessages, final int length, final long timestamp, final long checksum)
    {
        final EchoBenchmarksBlockingStub blockingClient = this.blockingClient;
        final EchoMessage.Builder messageBuilder = this.messageBuilder;
        final ByteString payload = this.payload;

        for (int i = 0; i < numberOfMessages; i++)
        {
            final EchoMessage request = messageBuilder
                .setTimestamp(timestamp)
                .setPayload(payload)
                .setChecksum(checksum)
                .build();

            final EchoMessage response = blockingClient.echo(request);

            onMessageReceived(response.getTimestamp(), response.getChecksum());
        }

        return numberOfMessages;
    }

    public void receive()
    {
    }
}
