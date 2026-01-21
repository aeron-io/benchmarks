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
import io.aeron.Image;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.benchmarks.aeron.AeronUtil.checkPublicationResult;

public final class EchoClusteredService implements ClusteredService
{
    private static final boolean USE_TRY_CLAIM = Boolean.getBoolean("io.aeron.benchmarks.useTryClaim");

    private final BufferClaim bufferClaim = new BufferClaim();
    private IdleStrategy idleStrategy;
    private final long snapshotSize;

    public EchoClusteredService(final long snapshotSize)
    {
        this.snapshotSize = snapshotSize;
    }

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        idleStrategy = cluster.idleStrategy();
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (null == session)
        {
            return;
        }

        final IdleStrategy idleStrategy = this.idleStrategy;
        idleStrategy.reset();

        if (USE_TRY_CLAIM)
        {
            final BufferClaim bufferClaim = this.bufferClaim;

            long result;
            while ((result = session.tryClaim(length, bufferClaim)) <= 0)
            {
                checkPublicationResult(result);
                idleStrategy.idle();
            }

            // FIXME: This is not required with the latest master
            if (ClientSession.MOCKED_OFFER == result)
            {
                bufferClaim.commit();
                return;
            }

            final MutableDirectBuffer dstBuffer = bufferClaim.buffer();
            final int msgOffset = bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH;
            dstBuffer.putBytes(msgOffset, buffer, offset, length);
            bufferClaim.flags(header.flags()).commit();
        }
        else
        {
            long result;
            while ((result = session.offer(buffer, offset, length)) <= 0)
            {
                checkPublicationResult(result);
                idleStrategy.idle();
            }
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[snapshotPublication.maxPayloadLength()]);
        buffer.setMemory(0, buffer.capacity(), (byte)'x');

        for (long written = 0; written < snapshotSize;)
        {
            final long remaining = snapshotSize - written;
            final int toWrite = (int)Math.min(buffer.capacity(), remaining);

            idleStrategy.reset();
            while (0 > snapshotPublication.offer(buffer, 0, toWrite))
            {
                idleStrategy.idle();
            }

            written += toWrite;
        }
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
    }
}
