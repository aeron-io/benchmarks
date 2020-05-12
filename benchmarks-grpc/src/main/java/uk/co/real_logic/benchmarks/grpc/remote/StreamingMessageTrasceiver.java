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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.agrona.LangUtil;
import uk.co.real_logic.benchmarks.remote.Configuration;
import uk.co.real_logic.benchmarks.remote.MessageRecorder;
import uk.co.real_logic.benchmarks.remote.MessageTransceiver;
import uk.co.real_logic.benchmarks.grpc.remote.EchoBenchmarksGrpc.EchoBenchmarksStub;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static uk.co.real_logic.benchmarks.grpc.remote.GrpcConfig.getServerChannel;
import static uk.co.real_logic.benchmarks.remote.Configuration.MIN_MESSAGE_LENGTH;

public class StreamingMessageTrasceiver extends MessageTransceiver
{
    private ManagedChannel serverChannel;
    private EchoBenchmarksStub asyncClient;
    private ClientCallStreamObserver<EchoMessage> requestObserver;
    private EchoMessage.Builder messageBuilder;
    private ByteString payload;

    public StreamingMessageTrasceiver(final MessageRecorder messageRecorder)
    {
        super(messageRecorder);
    }

    public void init(final Configuration configuration) throws Exception
    {
        serverChannel = getServerChannel();
        asyncClient = EchoBenchmarksGrpc.newStub(serverChannel);

        final StreamObserver<EchoMessage> responseObserver = new StreamObserver<EchoMessage>()
        {
            public void onNext(final EchoMessage response)
            {
                onMessageReceived(response.getTimestamp(), response.getChecksum());
            }

            public void onError(final Throwable t)
            {
                t.printStackTrace();
                LangUtil.rethrowUnchecked(t);
            }

            public void onCompleted()
            {
            }
        };

        requestObserver = (ClientCallStreamObserver<EchoMessage>)asyncClient.echoStream(responseObserver);

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
        requestObserver.onCompleted();

        serverChannel.shutdown().awaitTermination(1, MINUTES);
    }

    public int send(final int numberOfMessages, final int length, final long timestamp, final long checksum)
    {
        final ClientCallStreamObserver<EchoMessage> requestObserver = this.requestObserver;
        final EchoMessage.Builder messageBuilder = this.messageBuilder;
        final ByteString payload = this.payload;
        int count = 0;

        for (int i = 0; i < numberOfMessages && requestObserver.isReady(); i++)
        {
            final EchoMessage request = messageBuilder
                .setTimestamp(timestamp)
                .setPayload(payload)
                .setChecksum(checksum)
                .build();

            requestObserver.onNext(request);
            count++;
        }

        return count;
    }

    public void receive()
    {
    }
}
