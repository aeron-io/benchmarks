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
package io.aeron.benchmarks.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.HdrHistogram.ValueRecorder;
import org.agrona.LangUtil;
import org.agrona.concurrent.NanoClock;
import io.aeron.benchmarks.grpc.EchoBenchmarksGrpc.EchoBenchmarksStub;
import io.aeron.benchmarks.Configuration;
import io.aeron.benchmarks.MessageTransceiver;

import java.util.concurrent.ThreadLocalRandom;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.concurrent.TimeUnit.MINUTES;
import static io.aeron.benchmarks.grpc.GrpcConfig.getServerChannel;
import static io.aeron.benchmarks.Configuration.MIN_MESSAGE_LENGTH;

public class StreamingMessageTransceiver extends MessageTransceiver
{
    private ManagedChannel serverChannel;
    private ClientCallStreamObserver<EchoMessage> requestObserver;
    private EchoMessage.Builder messageBuilder;
    private ByteString payload;

    public StreamingMessageTransceiver(final NanoClock clock, final ValueRecorder valueRecorder)
    {
        super(clock, valueRecorder);
    }

    public void init(final Configuration configuration)
    {
        serverChannel = getServerChannel();

        ConnectivityState state;
        while (ConnectivityState.READY != (state = serverChannel.getState(true)))
        {
            if (ConnectivityState.SHUTDOWN == state)
            {
                throw new IllegalStateException("gRPC shutdown before connect");
            }
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new IllegalStateException("Interrupted while yielding...");
            }
        }

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

        final EchoBenchmarksStub asyncClient = EchoBenchmarksGrpc.newStub(serverChannel);
        requestObserver = (ClientCallStreamObserver<EchoMessage>)asyncClient.echoStream(responseObserver);

        messageBuilder = EchoMessage.newBuilder();
        final int payloadLength = configuration.messageLength() - MIN_MESSAGE_LENGTH;
        if (payloadLength == 0)
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
        try
        {
            if (null != requestObserver)
            {
                requestObserver.onCompleted();
            }
        }
        catch (final Throwable t)
        {
            System.err.println("*** gRPC cleanup failed");
            t.printStackTrace();
        }
        finally
        {
            if (null != serverChannel)
            {
                serverChannel.shutdown();
                serverChannel.awaitTermination(1, MINUTES);
            }
        }
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
