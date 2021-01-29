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

import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.Configuration.RECORDING_EVENTS_CHANNEL_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.RECORDING_EVENTS_ENABLED_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.connect;
import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static uk.co.real_logic.benchmarks.aeron.remote.ArchivingMediaDriver.launchArchiveWithEmbeddedDriver;

class LiveRecordingTest extends
    AbstractTest<ArchivingMediaDriver, AeronArchive, LiveRecordingMessageTransceiver, EchoNode>
{
    @BeforeEach
    void before()
    {
        setProperty(RECORDING_EVENTS_ENABLED_PROP_NAME, "true");
        setProperty(RECORDING_EVENTS_CHANNEL_PROP_NAME, IPC_CHANNEL);
    }

    @AfterEach
    void after()
    {
        clearProperty(RECORDING_EVENTS_ENABLED_PROP_NAME);
        clearProperty(RECORDING_EVENTS_CHANNEL_PROP_NAME);
    }

    protected EchoNode createNode(
        final AtomicBoolean running, final ArchivingMediaDriver archivingMediaDriver, final AeronArchive aeronArchive)
    {
        return new EchoNode(running, null, aeronArchive.context().aeron(), false);
    }

    protected ArchivingMediaDriver createDriver()
    {
        return launchArchiveWithEmbeddedDriver();
    }

    protected AeronArchive connectToDriver()
    {
        return connect();
    }

    protected Class<LiveRecordingMessageTransceiver> messageTransceiverClass()
    {
        return LiveRecordingMessageTransceiver.class;
    }

    protected LiveRecordingMessageTransceiver createMessageTransceiver(
        final ArchivingMediaDriver archivingMediaDriver,
        final AeronArchive aeronArchive)
    {
        return new LiveRecordingMessageTransceiver(archivingMediaDriver, aeronArchive, false);
    }
}
