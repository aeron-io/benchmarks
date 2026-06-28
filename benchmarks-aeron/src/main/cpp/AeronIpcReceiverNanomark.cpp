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

#define DISABLE_BOUNDS_CHECKS 1
#define EMBEDDED_MEDIA_DRIVER

#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <cstdint>
#include <sstream>

#include "NanoMark.h"

#include "Aeron.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "EmbeddedAeronMediaDriver.h"

using namespace aeron::concurrent;
using namespace aeron;

#define STREAM_ID (12)
#define FRAGMENT_LIMIT (128)
#define NANOS_PER_SECOND (INT64_C(1000000000))

static std::int64_t benchmarkMessageRatePerSec = INT64_C(1000000);

class SharedState
{
public:
    SharedState() = default;

    ~SharedState()
    {
        stopPublisher();

#ifdef EMBEDDED_MEDIA_DRIVER
        driver.stop();
#endif
    }

    void setup()
    {
        if (isSetup)
        {
            return;
        }

#ifdef EMBEDDED_MEDIA_DRIVER
        driver.start();
#endif

        Context context;
        context.preTouchMappedMemory(true);

        aeron = Aeron::connect(context);

        const std::int64_t publicationId = aeron->addPublication("aeron:ipc", STREAM_ID);
        const std::int64_t subscriptionId = aeron->addSubscription("aeron:ipc", STREAM_ID);

        while (!(publication = aeron->findPublication(publicationId)))
        {
            std::this_thread::yield();
        }

        while (!(subscription = aeron->findSubscription(subscriptionId)))
        {
            std::this_thread::yield();
        }

        while (!publication->isConnected())
        {
            std::this_thread::yield();
        }

        isSetup = true;
    }

    void startPublisher(std::int64_t messageRatePerSec)
    {
        publisherRunning = true;
        publisherThread = std::thread(
            [this, messageRatePerSec]()
            {
                publisherLoop(messageRatePerSec);
            });
    }

    void stopPublisher()
    {
        publisherRunning = false;
        if (publisherThread.joinable())
        {
            publisherThread.join();
        }
    }

    std::shared_ptr<Aeron> aeron;
    std::shared_ptr<Publication> publication;
    std::shared_ptr<Subscription> subscription;
    std::atomic<bool> isSetup = { false };
    std::atomic<bool> publisherRunning = { false };
    std::thread publisherThread;

#ifdef EMBEDDED_MEDIA_DRIVER
    EmbeddedMediaDriver driver;
#endif

private:
    void publisherLoop(std::int64_t messageRatePerSec)
    {
        const std::int64_t sendIntervalNs = NANOS_PER_SECOND / messageRatePerSec;
        BufferClaim bufferClaim;
        Publication *pub = publication.get();

        std::int64_t intendedSendNs = static_cast<std::int64_t>(nanomark::nanoClock());

        while (publisherRunning)
        {
            while (static_cast<std::int64_t>(nanomark::nanoClock()) < intendedSendNs)
            {
                BusySpinIdleStrategy::pause();
            }

            while (pub->tryClaim(sizeof(std::int64_t), bufferClaim) < 0)
            {
                BusySpinIdleStrategy::pause();
            }

            bufferClaim.buffer().putInt64(bufferClaim.offset(), intendedSendNs);
            bufferClaim.commit();

            intendedSendNs += sendIntervalNs;
        }
    }
};

class AeronIpcReceiverNanomark : public nanomark::Nanomark
{
public:
    void setUp() override
    {
        Nanomark::setUp();

        sharedState.setup();
        sharedState.startPublisher(benchmarkMessageRatePerSec);

        while (!sharedState.subscription->isConnected())
        {
            std::this_thread::yield();
        }

        m_imageSharedPtr = sharedState.subscription->imageByIndex(0);
    }

    void tearDown() override
    {
        sharedState.stopPublisher();

        Nanomark::tearDown();

        std::cout << "Summary: " << histogramSummary(histogram()) << std::endl;

        std::cout << "Histogram:" << std::endl;
        printFullHistogram();

        m_imageSharedPtr.reset();
    }

    void perThreadSetUp(std::size_t id, std::size_t repetition) override
    {
        Nanomark::perThreadSetUp(id, repetition);

        if (repetition == 1)
        {
            hdr_reset(m_histograms[id]);
        }
    }

    void perThreadTearDown(std::size_t id, std::size_t repetition) override
    {
        Nanomark::perThreadTearDown(id, repetition);

        std::ostringstream stream;

        stream << "Thread " << std::to_string(id) << " TearDown " <<
            std::to_string(repetition + 1) << "/" << numberOfMaxRepetitions() <<
            ": " << histogramSummary(histogram(id)) << std::endl;
        std::cout << stream.str();
    }

    void recordRepetition(std::size_t id, std::size_t repetition, std::uint64_t totalNs, std::size_t numberOfRuns)
        override
    {
        Nanomark::recordRepetition(id, repetition, totalNs, numberOfRuns);

        std::ostringstream stream;

        stream << "Thread " << std::to_string(id) << " repetition " << std::to_string(repetition + 1) << ": " <<
            "nanos/op " << std::to_string((double)totalNs / (double)numberOfRuns);
        stream << std::endl;

        std::cout << stream.str();
    }

    void recordRun(std::size_t, std::uint64_t) override
    {
        // per-message latencies are recorded directly in the fragment handler
    }

    static SharedState sharedState;
    std::shared_ptr<Image> m_imageSharedPtr;
};

SharedState AeronIpcReceiverNanomark::sharedState;

NANOMARK(AeronIpcReceiverNanomark, imagePoll)(std::size_t id)
{
    hdr_histogram *const hist = m_histograms[id];
    Image &image = *m_imageSharedPtr;

    auto handler =
        [hist](AtomicBuffer &buffer, util::index_t offset, util::index_t, Header &)
        {
            const std::int64_t intendedSendNs = buffer.getInt64(offset);
            const std::int64_t latencyNs =
                static_cast<std::int64_t>(nanomark::nanoClock()) - intendedSendNs;

            // hdr_init minimum trackable value is 1; guard against clock skew across cores
            hdr_record_value(hist, latencyNs > 0 ? latencyNs : 1);
        };

    image.poll(handler, FRAGMENT_LIMIT);
}

NANOMARK(AeronIpcReceiverNanomark, subscriptionPoll)(std::size_t id)
{
    hdr_histogram *const hist = m_histograms[id];
    Subscription &subscription = *sharedState.subscription;

    auto handler =
        [hist](AtomicBuffer &buffer, util::index_t offset, util::index_t, Header &)
        {
            const std::int64_t intendedSendNs = buffer.getInt64(offset);
            const std::int64_t latencyNs =
                static_cast<std::int64_t>(nanomark::nanoClock()) - intendedSendNs;

            // hdr_init minimum trackable value is 1; guard against clock skew across cores
            hdr_record_value(hist, latencyNs > 0 ? latencyNs : 1);
        };

    subscription.poll(handler, FRAGMENT_LIMIT);
}

int main(int argc, char **argv)
{
    benchmarkMessageRatePerSec = argc > 1 ? std::stoll(argv[1]) : INT64_C(1000000);
    std::cout << "Message Rate = " << std::to_string(benchmarkMessageRatePerSec) << " msg/sec" << std::endl;
    ::nanomark::NanomarkRunner::run(1, 6);

    return 0;
}
