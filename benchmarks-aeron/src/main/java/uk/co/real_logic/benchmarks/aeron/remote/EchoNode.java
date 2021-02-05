/*
 * Copyright 2015-2021 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.PropertyAction;
import org.agrona.SystemUtil;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.connect;
import static org.agrona.CloseHelper.closeAll;
import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.*;

/**
 * Remote node which echoes original messages back to the sender.
 */
public final class EchoNode implements AutoCloseable, Runnable
{
    private final FragmentHandler[] fragmentHandlers;
    private final ExclusivePublication[] publications;
    private final Subscription[] subscriptions;
    private final Image[] images;
    private final Subscription[] passiveSubscriptions;
    private final Image[] passiveImages;
    private final AtomicBoolean running;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final boolean ownsAeronClient;

    EchoNode(final AtomicBoolean running)
    {
        this(running, launchEmbeddedMediaDriverIfConfigured(), connect(), true);
    }

    EchoNode(
        final AtomicBoolean running, final MediaDriver mediaDriver, final Aeron aeron, final boolean ownsAeronClient)
    {
        this.running = running;
        this.mediaDriver = mediaDriver;
        this.aeron = aeron;
        this.ownsAeronClient = ownsAeronClient;

        final String[] destinationChannels = destinationChannels();
        final int[] destinationStreams = destinationStreams();
        assertChannelsAndStreamsMatch(
            destinationChannels, destinationStreams, DESTINATION_CHANNELS_PROP_NAME, DESTINATION_STREAMS_PROP_NAME);

        final String[] sourceChannels = sourceChannels();
        final int[] sourceStreams = sourceStreams();
        assertChannelsAndStreamsMatch(
            sourceChannels, sourceStreams, SOURCE_CHANNELS_PROP_NAME, SOURCE_STREAMS_PROP_NAME);

        if (destinationChannels.length != sourceChannels.length)
        {
            throw new IllegalArgumentException("Number of destinations does not match the number of sources");
        }

        final String[] passiveChannels = passiveChannels();
        final int[] passiveStreams = passiveStreams();
        assertChannelsAndStreamsMatch(
            passiveChannels, passiveStreams, PASSIVE_CHANNELS_PROP_NAME, PASSIVE_STREAMS_PROP_NAME);

        final int numActiveChannels = sourceChannels.length;
        fragmentHandlers = new FragmentHandler[numActiveChannels];
        publications = new ExclusivePublication[numActiveChannels];
        subscriptions = new Subscription[numActiveChannels];
        images = new Image[numActiveChannels];
        final BufferClaim bufferClaim = new BufferClaim();

        for (int i = 0; i < numActiveChannels; i++)
        {
            final ExclusivePublication publication = aeron.addExclusivePublication(sourceChannels[i], sourceStreams[i]);
            publications[i] = publication;
            subscriptions[i] = aeron.addSubscription(destinationChannels[i], destinationStreams[i]);
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

        if (passiveChannels.length > 0)
        {
            passiveSubscriptions = new Subscription[passiveChannels.length];
            passiveImages = new Image[passiveChannels.length];
            for (int i = 0; i < passiveChannels.length; i++)
            {
                passiveSubscriptions[i] = aeron.addSubscription(passiveChannels[i], passiveStreams[i]);
            }
        }
        else
        {
            passiveSubscriptions = EMPTY_SUBSCRIPTIONS;
            passiveImages = EMPTY_IMAGES;
        }

        while (!allConnected(subscriptions) || !allConnected(publications) || !allConnected(passiveSubscriptions))
        {
            yieldUninterruptedly();
        }

        reloadImages(passiveSubscriptions, passiveImages);
    }

    public void run()
    {
        final FragmentHandler[] fragmentHandlers = this.fragmentHandlers;
        final Subscription[] subscriptions = this.subscriptions;
        final Image[] images = this.images;
        final Image[] passiveImages = this.passiveImages;

        final IdleStrategy idleStrategy = idleStrategy();

        final int passiveChannelsPollFrequency = passiveChannelsPollFrequency();
        final AtomicBoolean running = this.running;

        reloadImages(subscriptions, images);
        final int numImages = images.length;
        int pollIndex = 0;
        int fragmentsSinceLastPassivePoll = 0;

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
                    return; // Abort execution
                }

                for (int i = 0; i < numImages; i++)
                {
                    if (images[i].isClosed())
                    {
                        return;  // Abort execution
                    }
                }
            }

            fragmentsSinceLastPassivePoll += fragments;
            if (fragmentsSinceLastPassivePoll >= passiveChannelsPollFrequency)
            {
                fragmentsSinceLastPassivePoll = 0;
                for (final Image passiveImage : passiveImages)
                {
                    passiveImage.poll(NULL_FRAGMENT_HANDLER, FRAGMENT_LIMIT);
                }
            }

            idleStrategy.idle(fragments);
        }
    }

    public void close()
    {
        closeAll(passiveSubscriptions);
        closeAll(subscriptions);
        closeAll(publications);

        if (ownsAeronClient)
        {
            closeAll(aeron, mediaDriver);
        }
    }

    private static void reloadImages(final Subscription[] subscriptions, final Image[] images)
    {
        for (int i = 0; i < subscriptions.length; i++)
        {
            images[i] = subscriptions[i].imageAtIndex(0);
        }
    }

    public static void main(final String[] args)
    {
        SystemUtil.loadPropertiesFiles(PropertyAction.PRESERVE, args);

        final AtomicBoolean running = new AtomicBoolean(true);
        installSignalHandler(running);

        try (EchoNode server = new EchoNode(running))
        {
            server.run();
        }
    }
}
