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
package io.aeron.benchmarks.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.BitUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import static io.aeron.benchmarks.aeron.AeronUtil.RECEIVER_INDEX_OFFSET;
import static io.aeron.benchmarks.aeron.AeronUtil.SEND_ATTEMPTS;
import static io.aeron.benchmarks.aeron.AeronUtil.TIMESTAMP_OFFSET;
import static io.aeron.benchmarks.aeron.AeronUtil.checkPublicationResult;
import static io.aeron.benchmarks.aeron.AeronUtil.useTryClaim;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;

public abstract class MessageSender
{
    final ExclusivePublication publication;
    final IdleStrategy idleStrategy;
    private final int numReceivers;
    private int receiverIndex;

    public MessageSender(
        final ExclusivePublication publication, final IdleStrategy idleStrategy, final int numReceivers)
    {
        this.publication = publication;
        this.idleStrategy = idleStrategy;
        this.numReceivers = numReceivers;
    }

    public abstract int send(
        int numberOfMessages, int messageLength, long timestamp, long checksum);

    final void preparePayload(
        final MutableDirectBuffer buffer,
        final int offset,
        final int messageLength,
        final long timestamp,
        final long checksum)
    {
        buffer.putLong(offset + TIMESTAMP_OFFSET, timestamp, LITTLE_ENDIAN);

        // set receiverIndex to ensure only one reply will be received
        buffer.putInt(offset + RECEIVER_INDEX_OFFSET, receiverIndex, LITTLE_ENDIAN);
        receiverIndex = BitUtil.next(receiverIndex, numReceivers);

        buffer.putLong(offset + messageLength - SIZE_OF_LONG, checksum, LITTLE_ENDIAN);
    }

    public static MessageSender create(
        final ExclusivePublication publication,
        final IdleStrategy idleStrategy,
        final int numReceivers)
    {
        if (useTryClaim())
        {
            return new TryClaim(publication, idleStrategy, numReceivers);
        }
        else
        {
            return new Offer(publication, idleStrategy, numReceivers);
        }
    }

    static final class Offer extends MessageSender
    {
        private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);

        Offer(final ExclusivePublication publication, final IdleStrategy idleStrategy, final int numReceivers)
        {
            super(publication, idleStrategy, numReceivers);
        }

        public int send(
            final int numberOfMessages,
            final int messageLength,
            final long timestamp,
            final long checksum)
        {
            final ExclusivePublication publication = this.publication;
            final ExpandableArrayBuffer buffer = this.buffer;

            int count = 0;
            for (int i = 0; i < numberOfMessages; i++)
            {
                preparePayload(buffer, 0, messageLength, timestamp, checksum);

                int retryCount = SEND_ATTEMPTS;
                long result;
                while ((result = publication.offer(buffer, 0, messageLength)) < 0)
                {
                    if (checkPublicationResult(result, idleStrategy))
                    {
                        continue;
                    }

                    if (0 == --retryCount)
                    {
                        return count;
                    }
                }

                count++;
            }

            return count;
        }
    }

    static final class TryClaim extends MessageSender
    {
        private final BufferClaim bufferClaim = new BufferClaim();

        TryClaim(final ExclusivePublication publication, final IdleStrategy idleStrategy, final int numReceivers)
        {
            super(publication, idleStrategy, numReceivers);
        }

        public int send(
            final int numberOfMessages,
            final int messageLength,
            final long timestamp,
            final long checksum)
        {
            final ExclusivePublication publication = this.publication;
            final BufferClaim bufferClaim = this.bufferClaim;
            int count = 0;
            for (int i = 0; i < numberOfMessages; i++)
            {
                int retryCount = SEND_ATTEMPTS;
                long result;
                while ((result = publication.tryClaim(messageLength, bufferClaim)) < 0)
                {
                    if (checkPublicationResult(result, idleStrategy))
                    {
                        continue;
                    }

                    if (0 == --retryCount)
                    {
                        return count;
                    }
                }

                preparePayload(bufferClaim.buffer(), bufferClaim.offset(), messageLength, timestamp, checksum);
                bufferClaim.commit();

                count++;
            }

            return count;
        }
    }
}
