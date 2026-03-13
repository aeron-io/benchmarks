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
package io.aeron.benchmarks;

import org.HdrHistogram.ValueRecorder;
import org.agrona.concurrent.NanoClock;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

abstract class MessageTransceiverLhsPadding
{
    boolean p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    boolean p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    boolean p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    boolean p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
    boolean p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    boolean p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    boolean p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    boolean p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

abstract class MessageTransceiverHotFields extends MessageTransceiverLhsPadding
{
    final NanoClock clock;
    final ValueRecorder valueRecorder;
    long receivedMessages;

    MessageTransceiverHotFields(final NanoClock clock, final ValueRecorder valueRecorder)
    {
        this.clock = requireNonNull(clock);
        this.valueRecorder = requireNonNull(valueRecorder);
    }

    final long receivedMessages()
    {
        return receivedMessages;
    }
}

abstract class MessageTransceiverRhsPadding extends MessageTransceiverHotFields
{
    boolean p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    boolean p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    boolean p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    boolean p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
    boolean p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    boolean p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    boolean p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    boolean p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    MessageTransceiverRhsPadding(final NanoClock clock, final ValueRecorder valueRecorder)
    {
        super(clock, valueRecorder);
    }
}


/**
 * {@code MessageTransceiver} is an SPI to be implemented by the system under test.
 */
public abstract class MessageTransceiver extends MessageTransceiverRhsPadding
{
    static final long CHECKSUM = ThreadLocalRandom.current().nextLong();

    protected MessageTransceiver(final NanoClock clock, final ValueRecorder valueRecorder)
    {
        super(clock, valueRecorder);
    }

    /**
     * Initialize state and establish all necessary connections.
     *
     * @param configuration configuration options
     * @throws Exception in case of an error
     * @implNote The method should block until it is safe to call {@link #send(int, int, long, long)}
     * and {@link #receive()} methods.
     */
    public abstract void init(Configuration configuration) throws Exception;

    /**
     * Cleanup resources and tear down the connections with remote side.
     *
     * @throws Exception in case of an error
     */
    public abstract void destroy() throws Exception;

    /**
     * Sends specified number of {@code numberOfMessages} with the given {@code messageLength} and a {@code timestamp}
     * as a payload.
     *
     * @param numberOfMessages to be sent.
     * @param messageLength    in bytes (of a single message).
     * @param timestamp        to be included at the beginning of the message payload.
     * @param checksum         to be included at the end of the message payload.
     * @return actual number of messages sent.
     * @implSpec {@code Sender} must send a message with the payload that is at least {@code messageLength} bytes long
     * and <strong>must</strong> include given {@code timestamp} value at the beginning of the message payload and
     * {@code checksum} at the end of it. Any header added by the sender <strong>may not</strong> be counted towards
     * the {@code messageLength} bytes.
     * <p>
     * If send is <em>synchronous and blocking</em>, i.e. for every message sent there will be an immediate response
     * message, then for every received message method {@link #onMessageReceived(long, long)} <strong>must</strong> be
     * called.
     * </p>
     * @implNote The implementation can re-try actual send operation multiple times if needed but it
     * <strong>should not</strong> block forever since test rig will re-try sending the batch, e.g. if a first call
     * sends only {code 3} out of {code 5} messages then there will be a second call with the batch size of {code 2}.
     */
    public abstract int send(int numberOfMessages, int messageLength, long timestamp, long checksum);

    /**
     * Receive one or more messages.
     *
     * @implSpec For every received message method {@link #onMessageReceived(long, long)} <strong>must be</strong>
     * called.
     * @implNote Can be a no op if send is <em>synchronous and blocking</em>.
     * @see #send(int, int, long, long)
     */
    public abstract void receive();

    /**
     * Callback method to be invoked for every message received.
     *
     * @param timestamp from the received message.
     * @param checksum  from the received message.
     */
    protected final void onMessageReceived(final long timestamp, final long checksum)
    {
        if (CHECKSUM != checksum)
        {
            throw new IllegalStateException("Invalid checksum: expected=" + CHECKSUM + ", actual=" + checksum);
        }

        valueRecorder.recordValue(clock.nanoTime() - timestamp);
        receivedMessages++;
    }

    /**
     * Callback method to be invoked for every message received.
     * <p>
     * Normally you want to use the {@link #onMessageReceived(long, long)} and rely on the ValueRecorder
     * inside the MessageTransceiver. But when there are multiple ValueRecorder instances, then this method
     * should be used.
     *
     * @param valueRecorder the ValueRecorder used to record the value
     * @param timestamp from the received message.
     * @param checksum  from the received message.
     */
    protected final void onMessageReceived(
        final ValueRecorder valueRecorder, final long timestamp, final long checksum)
    {
        if (CHECKSUM != checksum)
        {
            throw new IllegalStateException("Invalid checksum: expected=" + CHECKSUM + ", actual=" + checksum);
        }

        valueRecorder.recordValue(clock.nanoTime() - timestamp);
        receivedMessages++;
    }

    final void reset()
    {
        valueRecorder.reset();
        receivedMessages = 0;
    }
}
