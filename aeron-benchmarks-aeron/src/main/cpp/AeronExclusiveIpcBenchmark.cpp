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

#define DISABLE_BOUNDS_CHECKS 1
#define EMBEDDED_MEDIA_DRIVER

#include <string>
#include <benchmark/benchmark.h>
#include <thread>
#include <atomic>
#include <array>
#include <inttypes.h>

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "EmbeddedAeronMediaDriver.h"

extern "C"
{
#include "concurrent/aeron_spsc_concurrent_array_queue.h"
#include "util/aeron_error.h"
}

using namespace aeron::concurrent;
using namespace aeron;

#define MAX_THREAD_COUNT (1)
#define FRAGMENT_LIMIT (128)
#define STREAM_ID (11)
#define MAX_BURST_LENGTH (100)
#define RESPONSE_QUEUE_CAPACITY (128)
#define USE_TRY_CLAIM

int SENTINEL = 0;

typedef std::array<std::uint8_t, (sizeof(std::int32_t) * MAX_BURST_LENGTH)> src_buffer_t;

template<typename P, typename IdleStrategy = BusySpinIdleStrategy>
class Burster
{
public:
    Burster(
        std::size_t burstLength,
        int id,
        std::shared_ptr<P> publication,
        aeron_spsc_concurrent_array_queue_t *responseQueue) :
        m_src(),
        m_srcBuffer(m_src, 0),
        m_savedPublication(std::move(publication)),
        m_values(),
        m_responseQueue(responseQueue),
        m_publication(m_savedPublication.get()),
        m_burstLength(burstLength)
    {
        m_src.fill(0);

        for (std::size_t i = 0; i < burstLength; i++)
        {
            m_values[i] = -(burstLength - i);
        }

        m_values[burstLength - 1] = id;
    }

    inline void sendBurst()
    {
        auto iter = m_values.begin();

        for (std::size_t i = 0; i < m_burstLength; ++iter, ++i)
        {
#ifdef USE_TRY_CLAIM
            while (m_publication->tryClaim(sizeof(std::int32_t), m_bufferClaim) < 0)
            {
                IdleStrategy::pause();
            }

            m_bufferClaim.buffer().putInt32(m_bufferClaim.offset(), *iter);
            m_bufferClaim.commit();
#else
            m_srcBuffer.putInt32(0, *iter);
            while (m_publication->offer(m_srcBuffer, 0, sizeof(std::int32_t), DEFAULT_RESERVED_VALUE_SUPPLIER) < 0)
            {
                IdleStrategy::pause();
            }
#endif
        }
    }

    inline void awaitConfirm()
    {
        while (aeron_spsc_concurrent_array_queue_poll(m_responseQueue) == nullptr)
        {
            IdleStrategy::pause();
        }
    }

private:
    AERON_DECL_ALIGNED(src_buffer_t m_src, 16);
    AtomicBuffer m_srcBuffer;
    BufferClaim m_bufferClaim;

    std::shared_ptr<P> m_savedPublication;
    std::array<std::int32_t, MAX_BURST_LENGTH> m_values;

    aeron_spsc_concurrent_array_queue_t *m_responseQueue;
    P *m_publication;
    const std::size_t m_burstLength;
};

class SharedState
{
public:
    SharedState() = default;

    ~SharedState()
    {
        running = false;
        subscriptionThread.join();

        for (std::size_t i = 0; i < MAX_THREAD_COUNT; i++)
        {
            aeron_spsc_concurrent_array_queue_close(&responseQueues[i]);
        }

#ifdef EMBEDDED_MEDIA_DRIVER
        driver.stop();
#endif
    }

    void setup()
    {
        if (!isSetup)
        {
            for (std::size_t i = 0; i < MAX_THREAD_COUNT; i++)
            {
                if (aeron_spsc_concurrent_array_queue_init(&responseQueues[i], RESPONSE_QUEUE_CAPACITY) < 0)
                {
                    std::cerr << "could not init responseQueue: " << aeron_errmsg() << std::endl;
                    std::exit(EXIT_FAILURE);
                }
            }

#ifdef EMBEDDED_MEDIA_DRIVER
            driver.start();
#endif

            Context context;
            context.preTouchMappedMemory(true);

            aeron = Aeron::connect(context);

            const std::int64_t publicationId = aeron->addExclusivePublication("aeron:ipc", STREAM_ID);
            const std::int64_t subscriptionId = aeron->addSubscription("aeron:ipc", STREAM_ID);

            publication = aeron->findExclusivePublication(publicationId);
            subscription = aeron->findSubscription(subscriptionId);

            while (!subscription)
            {
                std::this_thread::yield();
                subscription = aeron->findSubscription(subscriptionId);
            }

            while (!publication)
            {
                std::this_thread::yield();
                publication = aeron->findExclusivePublication(publicationId);
            }

            while (!publication->isConnected())
            {
                std::this_thread::yield();
            }

            subscriptionThread = std::thread(
                [&]()
                {
                    subscriberLoop();
                });

            isSetup = true;
        }
    }

    void awaitSetup()
    {
        while (!isSetup)
        {
            std::this_thread::yield();
        }
    }

    void subscriberLoop()
    {
        while (!subscription->isConnected())
        {
            std::this_thread::yield();
        }

        std::shared_ptr<Image> imageSharedPtr = subscription->imageByIndex(0);
        Image& image = *imageSharedPtr;
        aeron_spsc_concurrent_array_queue_t *q = &responseQueues[0];
        auto handler =
            [q](AtomicBuffer& buffer, util::index_t offset, util::index_t, Header&)
            {
                const std::int32_t value = buffer.getInt32(offset);
                if (value >= 0)
                {
                    while (aeron_spsc_concurrent_array_queue_offer(q, &SENTINEL) != AERON_OFFER_SUCCESS)
                    {
                        BusySpinIdleStrategy::pause();
                    }
                }
            };

        while (running)
        {
            if (image.poll(handler, FRAGMENT_LIMIT) == 0)
            {
                BusySpinIdleStrategy::pause();
            }
        }
    }

    AERON_DECL_ALIGNED(aeron_spsc_concurrent_array_queue_t responseQueues[MAX_THREAD_COUNT], 16);
    std::shared_ptr<Aeron> aeron;
    std::shared_ptr<ExclusivePublication> publication;
    std::shared_ptr<Subscription> subscription;
    std::atomic<bool> isSetup = { false };
    std::atomic<bool> running = { true };
    std::thread subscriptionThread;
#ifdef EMBEDDED_MEDIA_DRIVER
    EmbeddedMediaDriver driver;
#endif
};

SharedState sharedState;

static void BM_AeronExclusiveIpcBenchmark(benchmark::State &state)
{
    const std::size_t burstLength = state.range(0);

    sharedState.setup();

    Burster<ExclusivePublication> burster(burstLength, 0, sharedState.publication, &sharedState.responseQueues[0]);

    for (auto _ : state)
    {
        burster.sendBurst();
        burster.awaitConfirm();
    }

    char label[256];

    std::snprintf(label, sizeof(label) - 1, "Burst Length: %" PRIu64, static_cast<std::uint64_t>(burstLength));
    state.SetLabel(label);
}

BENCHMARK(BM_AeronExclusiveIpcBenchmark)
    ->Arg(1)
    ->Arg(MAX_BURST_LENGTH)
    ->UseRealTime();

BENCHMARK_MAIN();
