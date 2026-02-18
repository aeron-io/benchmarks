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

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.benchmarks.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemNanoClock;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.DESTINATION_CHANNELS_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.DESTINATION_STREAMS_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.FRAGMENT_LIMIT;
import static io.aeron.benchmarks.aeron.AeronUtil.SOURCE_CHANNELS_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.SOURCE_STREAMS_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.assertChannelsAndStreamsMatch;
import static io.aeron.benchmarks.aeron.AeronUtil.awaitConnected;
import static io.aeron.benchmarks.aeron.AeronUtil.checkPublicationResult;
import static io.aeron.benchmarks.aeron.AeronUtil.connectionTimeoutNs;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationChannels;
import static io.aeron.benchmarks.aeron.AeronUtil.destinationStreams;
import static io.aeron.benchmarks.aeron.AeronUtil.idleStrategy;
import static io.aeron.benchmarks.aeron.AeronUtil.launchEmbeddedMediaDriverIfConfigured;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceChannels;
import static io.aeron.benchmarks.aeron.AeronUtil.sourceStreams;
import static org.agrona.CloseHelper.closeAll;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

/**
 * Remote node which echoes original messages back to multiple senders.
 * Each sender has a dedicated request/reply channel pair.
 * <p>
 * Configuration properties:
 * <ul>
 *     <li>{@code io.aeron.benchmarks.aeron.destination.channels} - comma-separated list of destination channels</li>
 *     <li>{@code io.aeron.benchmarks.aeron.destination.streams} - comma-separated list of destination stream IDs</li>
 *     <li>{@code io.aeron.benchmarks.aeron.source.channels} - comma-separated list of source channels</li>
 *     <li>{@code io.aeron.benchmarks.aeron.source.streams} - comma-separated list of source stream IDs</li>
 * </ul>
 * <p>
 * Example configuration for 2 producers:
 * <pre>
 * io.aeron.benchmarks.aeron.destination.channels=aeron:udp?endpoint=localhost:13333,aeron:udp?endpoint=localhost:13335
 * io.aeron.benchmarks.aeron.destination.streams=77777,77779
 * io.aeron.benchmarks.aeron.source.channels=aeron:udp?endpoint=localhost:13334,aeron:udp?endpoint=localhost:13336
 * io.aeron.benchmarks.aeron.source.streams=55555,55557
 * </pre>
 * <p>
 * Each producer runs a standard {@link EchoMessageTransceiver} configured with the singular channel/stream properties
 * matching one pair from the arrays above.
 */
public final class MultiEchoNode implements AutoCloseable, Runnable
{
    private final FragmentHandler[] fragmentHandlers;
    private final ExclusivePublication[] publications;
    private final Subscription[] subscriptions;
    private final Image[] images;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;

    MultiEchoNode(final AtomicBoolean running)
    {
        this(running, launchEmbeddedMediaDriverIfConfigured(), connect(), true);
    }

    MultiEchoNode(
        final AtomicBoolean running,
        final MediaDriver mediaDriver,
        final Aeron aeron,
        final boolean ownsAeronClient)
    {
        this.running = running;
        this.mediaDriver = mediaDriver;
        this.aeron = aeron;
        this.ownsAeronClient = ownsAeronClient;

        final String[] destChannels = destinationChannels();
        final int[] destStreams = destinationStreams();
        assertChannelsAndStreamsMatch(
            destChannels, destStreams, DESTINATION_CHANNELS_PROP_NAME, DESTINATION_STREAMS_PROP_NAME);

        final String[] srcChannels = sourceChannels();
        final int[] srcStreams = sourceStreams();
        assertChannelsAndStreamsMatch(
            srcChannels, srcStreams, SOURCE_CHANNELS_PROP_NAME, SOURCE_STREAMS_PROP_NAME);

        if (destChannels.length != srcChannels.length)
        {
            throw new IllegalArgumentException(
                "Number of destination channels (" + destChannels.length +
                    ") does not match number of source channels (" + srcChannels.length + ")");
        }

        final int numChannelPairs = destChannels.length;
        fragmentHandlers = new FragmentHandler[numChannelPairs];
        publications = new ExclusivePublication[numChannelPairs];
        subscriptions = new Subscription[numChannelPairs];
        images = new Image[numChannelPairs];
        final BufferClaim bufferClaim = new BufferClaim();

        for (int i = 0; i < numChannelPairs; i++)
        {
            final ExclusivePublication publication =
                aeron.addExclusivePublication(srcChannels[i], srcStreams[i]);
            publications[i] = publication;
            subscriptions[i] = aeron.addSubscription(destChannels[i], destStreams[i]);
            fragmentHandlers[i] =
                (buffer, offset, length, header) ->
                {
                    long result;
                    while ((result = publication.tryClaim(length, bufferClaim)) <= 0)
                    {
                        checkPublicationResult(result);
                    }

                    bufferClaim
                        .flags(header.flags())
                        .putBytes(buffer, offset, length)
                        .commit();
                };
        }
    }

    public void run()
    {
        final FragmentHandler[] fragmentHandlers = this.fragmentHandlers;
        final Image[] images = this.images;
        final IdleStrategy idleStrategy = idleStrategy();
        final AtomicBoolean running = this.running;

        awaitConnected(
            () -> allConnected(subscriptions) && allConnected(publications),
            connectionTimeoutNs(),
            SystemNanoClock.INSTANCE);

        reloadImages(subscriptions, images);
        final int numImages = images.length;
        int pollIndex = 0;

        while (true)
        {
            if (++pollIndex >= numImages)
            {
                pollIndex = 0;
            }

            int fragments = 0;
            for (int i = pollIndex; i < numImages && fragments < FRAGMENT_LIMIT; i++)
            {
                fragments += images[i].poll(fragmentHandlers[i], FRAGMENT_LIMIT - fragments);
            }

            for (int i = 0; i < pollIndex && fragments < FRAGMENT_LIMIT; i++)
            {
                fragments += images[i].poll(fragmentHandlers[i], FRAGMENT_LIMIT - fragments);
            }

            if (0 == fragments)
            {
                if (!running.get())
                {
                    return;
                }

                for (final Image image : images)
                {
                    if (image.isClosed())
                    {
                        return;
                    }
                }
            }

            idleStrategy.idle(fragments);
        }
    }

    public void close()
    {
        closeAll(subscriptions);
        closeAll(publications);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
    }

    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path outputDir = Configuration.resolveLogsDir();
        final int receiverIndex = AeronUtil.receiverIndex();

        final AtomicBoolean running = new AtomicBoolean(true);
        try (ShutdownSignalBarrier shutdownSignalBarrier = new ShutdownSignalBarrier(() -> running.set(false));
            MultiEchoNode node = new MultiEchoNode(running))
        {
            Thread.currentThread().setName("echo-" + receiverIndex);

            node.run();

            final String prefix = "echo-node-" + receiverIndex + "-";
            AeronUtil.dumpAeronStats(
                node.aeron.context().cncFile(),
                outputDir.resolve(prefix + "aeron-stat.txt"),
                outputDir.resolve(prefix + "errors.txt"));
        }
    }

    private static void reloadImages(final Subscription[] subscriptions, final Image[] images)
    {
        for (int i = 0; i < subscriptions.length; i++)
        {
            images[i] = subscriptions[i].imageAtIndex(0);
        }
    }

    private static boolean allConnected(final Subscription[] subscriptions)
    {
        for (final Subscription subscription : subscriptions)
        {
            if (!subscription.isConnected())
            {
                return false;
            }
        }
        return true;
    }

    private static boolean allConnected(final ExclusivePublication[] publications)
    {
        for (final ExclusivePublication publication : publications)
        {
            if (!publication.isConnected() || publication.availableWindow() <= 0)
            {
                return false;
            }
        }
        return true;
    }
}