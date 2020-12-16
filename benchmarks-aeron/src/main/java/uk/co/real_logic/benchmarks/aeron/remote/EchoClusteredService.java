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
package uk.co.real_logic.benchmarks.aeron.remote;

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

import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.checkPublicationResult;

public final class EchoClusteredService implements ClusteredService
{
    private final BufferClaim bufferClaim = new BufferClaim();
    private IdleStrategy idleStrategy;

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
            return; // skip non-client calls
        }

        final IdleStrategy idleStrategy = this.idleStrategy;
        final BufferClaim bufferClaim = this.bufferClaim;

        idleStrategy.reset();
        long result;
        while ((result = session.tryClaim(length, bufferClaim)) <= 0)
        {
            checkPublicationResult(result);
            idleStrategy.idle();
        }

        final MutableDirectBuffer dstBuffer = bufferClaim.buffer();
        final int msgOffset = bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH;

        dstBuffer.putBytes(msgOffset, buffer, offset, length);

        bufferClaim.flags(header.flags()).commit();
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
    }
}
