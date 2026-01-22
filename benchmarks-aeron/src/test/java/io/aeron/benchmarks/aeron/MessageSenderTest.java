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
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

import static io.aeron.benchmarks.aeron.AeronUtil.MIN_MESSAGE_LENGTH;
import static io.aeron.benchmarks.aeron.AeronUtil.RECEIVER_INDEX_OFFSET;
import static io.aeron.benchmarks.aeron.AeronUtil.TIMESTAMP_OFFSET;
import static io.aeron.benchmarks.aeron.AeronUtil.USE_TRY_CLAIM_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.useTryClaim;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class MessageSenderTest
{
    private final ExclusivePublication publication = mock(ExclusivePublication.class);
    private final IdleStrategy idleStrategy = mock(IdleStrategy.class);

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldCreateSenderBasedOnSystemProperty(final boolean useTryClaim)
    {
        System.setProperty(USE_TRY_CLAIM_PROP_NAME, Boolean.toString(useTryClaim));
        try
        {
            assertEquals(useTryClaim, useTryClaim());

            final MessageSender messageSender = MessageSender.create(publication, idleStrategy, 1);
            assertNotNull(messageSender);
            assertEquals(
                useTryClaim ? MessageSender.TryClaim.class : MessageSender.Offer.class,
                messageSender.getClass());

            assertNotSame(messageSender, MessageSender.create(publication, idleStrategy, 1));
        }
        finally
        {
            System.clearProperty(USE_TRY_CLAIM_PROP_NAME);
            assertTrue(useTryClaim());
        }
    }

    @Test
    void preparePayloadShouldIncrementReceiverIndex()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final MessageSender.Offer messageSender = new MessageSender.Offer(publication, idleStrategy, 3);

        messageSender.preparePayload(buffer, 4, MIN_MESSAGE_LENGTH, 879, Long.MIN_VALUE);
        messageSender.preparePayload(buffer, 40, 30, Long.MAX_VALUE, 42);
        messageSender.preparePayload(buffer, 100, 100, 100, 2000);
        messageSender.preparePayload(buffer, 224, 32, Long.MIN_VALUE, 0);

        assertEquals(879, buffer.getLong(4 + TIMESTAMP_OFFSET, LITTLE_ENDIAN));
        assertEquals(0, buffer.getLong(4 + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN));
        assertEquals(Long.MIN_VALUE, buffer.getLong(4 + MIN_MESSAGE_LENGTH - SIZE_OF_LONG, LITTLE_ENDIAN));

        assertEquals(Long.MAX_VALUE, buffer.getLong(40 + TIMESTAMP_OFFSET, LITTLE_ENDIAN));
        assertEquals(1, buffer.getLong(40 + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN));
        assertEquals(42, buffer.getLong(40 + 30 - SIZE_OF_LONG, LITTLE_ENDIAN));

        assertEquals(100, buffer.getLong(100 + TIMESTAMP_OFFSET, LITTLE_ENDIAN));
        assertEquals(2, buffer.getLong(100 + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN));
        assertEquals(2000, buffer.getLong(100 + 100 - SIZE_OF_LONG, LITTLE_ENDIAN));

        assertEquals(Long.MIN_VALUE, buffer.getLong(224 + TIMESTAMP_OFFSET, LITTLE_ENDIAN));
        assertEquals(0, buffer.getLong(224 + RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN));
        assertEquals(0, buffer.getLong(224 + 32 - SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void shouldSendMultipleMessagesUsingOffer()
    {
        final int numberOfMessages = 5;
        final int messageLength = 288;
        final long timestamp = 42;
        final long checksum = 0x26348233434L;

        final MessageSender.Offer messageSender = new MessageSender.Offer(publication, idleStrategy, 3);
        final MutableInteger callCount = new MutableInteger();
        doAnswer((Answer<Long>)invocation ->
        {
            final int count = callCount.getAndIncrement();
            if (count > 3)
            {
                return Publication.BACK_PRESSURED;
            }

            if (2 == count)
            {
                return Publication.ADMIN_ACTION;
            }

            final MutableDirectBuffer buffer = invocation.getArgument(0);
            final int offset = invocation.getArgument(1);
            final int length = invocation.getArgument(2);

            assertEquals(0, offset);
            assertEquals(messageLength, length);

            assertEquals(timestamp, buffer.getLong(TIMESTAMP_OFFSET, LITTLE_ENDIAN));
            assertEquals(min(count, 2), buffer.getLong(RECEIVER_INDEX_OFFSET, LITTLE_ENDIAN));
            assertEquals(checksum, buffer.getLong(length - SIZE_OF_LONG, LITTLE_ENDIAN));

            return 1L;
        })
            .when(publication)
            .offer(any(MutableDirectBuffer.class), anyInt(), anyInt());

        assertEquals(3, messageSender.send(numberOfMessages, messageLength, timestamp, checksum));
    }

    @Test
    void shouldSendMultipleMessagesUsingTryClaim()
    {
        final int numberOfMessages = 9;
        final int messageLength = 1024;
        final long timestamp = Long.MAX_VALUE - 120000000L;
        final long checksum = 42;

        final MessageSender.TryClaim messageSender = new MessageSender.TryClaim(publication, idleStrategy, 5);
        final MutableInteger callCount = new MutableInteger();
        final int initialOffset = 64;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[2048]);
        doAnswer((Answer<Long>)invocation ->
        {
            final int count = callCount.getAndIncrement();
            if (count > 4)
            {
                return Publication.BACK_PRESSURED;
            }

            if (3 == count)
            {
                return Publication.ADMIN_ACTION;
            }

            final int length = invocation.getArgument(0);
            assertEquals(messageLength, length);

            final BufferClaim bufferClaim = invocation.getArgument(1);
            bufferClaim.wrap(buffer, initialOffset, length + DataHeaderFlyweight.HEADER_LENGTH);

            return 1L;
        })
            .when(publication)
            .tryClaim(anyInt(), any(BufferClaim.class));

        assertEquals(4, messageSender.send(numberOfMessages, messageLength, timestamp, checksum));
    }
}
