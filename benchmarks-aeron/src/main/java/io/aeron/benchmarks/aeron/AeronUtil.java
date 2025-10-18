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
import io.aeron.CncFileDescriptor;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.benchmarks.Configuration;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static io.aeron.CncFileDescriptor.createCountersMetaDataBuffer;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.Publication.CLOSED;
import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static io.aeron.Publication.NOT_CONNECTED;
import static io.aeron.archive.status.RecordingPos.findCounterIdBySession;
import static io.aeron.archive.status.RecordingPos.getRecordingId;
import static io.aeron.benchmarks.aeron.ArchivingMediaDriver.launchArchiveWithEmbeddedDriver;
import static io.aeron.benchmarks.aeron.ArchivingMediaDriver.launchArchiveWithStandaloneDriver;
import static java.lang.Boolean.getBoolean;
import static java.lang.Integer.getInteger;
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.getProperty;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.Strings.isEmpty;
import static org.agrona.SystemUtil.parseDuration;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

public final class AeronUtil
{
    public static final int TIMESTAMP_OFFSET = 0;
    public static final int RECEIVER_INDEX_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;
    public static final int MIN_MESSAGE_LENGTH = RECEIVER_INDEX_OFFSET + SIZE_OF_LONG + SIZE_OF_LONG;

    public static final String RECEIVER_INDEX_PROP_NAME = "io.aeron.benchmarks.aeron.receiver.index";
    public static final String NUMBER_OF_RECEIVERS_PROP_NAME =
        "io.aeron.benchmarks.aeron.receiver.count";
    public static final String CLUSTER_SERVICE_PROP_NAME = "io.aeron.benchmarks.aeron.cluster.service";
    public static final String SNAPSHOT_SIZE_PROP_NAME = "io.aeron.benchmarks.aeron.cluster.snapshot.size";
    public static final long DEFAULT_SNAPSHOT_SIZE = 0;
    public static final String DESTINATION_CHANNEL_PROP_NAME =
        "io.aeron.benchmarks.aeron.destination.channel";
    public static final String DESTINATION_STREAM_PROP_NAME =
        "io.aeron.benchmarks.aeron.destination.stream";
    public static final String SOURCE_CHANNEL_PROP_NAME = "io.aeron.benchmarks.aeron.source.channel";
    public static final String SOURCE_STREAM_PROP_NAME = "io.aeron.benchmarks.aeron.source.stream";
    public static final String RECORD_CHANNEL_PROP_NAME = "io.aeron.benchmarks.aeron.record.channel";
    public static final String RECORD_STREAM_PROP_NAME = "io.aeron.benchmarks.aeron.record.stream";
    public static final String REPLAY_CHANNEL_PROP_NAME = "io.aeron.benchmarks.aeron.replay.channel";
    public static final String REPLAY_STREAM_PROP_NAME = "io.aeron.benchmarks.aeron.replay.stream";
    public static final String EMBEDDED_MEDIA_DRIVER_PROP_NAME =
        "io.aeron.benchmarks.aeron.embedded.media.driver";
    public static final String FRAGMENT_LIMIT_PROP_NAME = "io.aeron.benchmarks.aeron.fragment.limit";
    public static final String IDLE_STRATEGY_PROP_NAME = "io.aeron.benchmarks.aeron.idle.strategy";
    public static final String CONNECTION_TIMEOUT_PROP_NAME = "io.aeron.benchmarks.aeron.connection.timeout";
    public static final int FRAGMENT_LIMIT = getInteger(FRAGMENT_LIMIT_PROP_NAME, 10);
    public static final String FAILOVER_CONTROL_SERVER_HOSTNAME_PROP_NAME =
        "io.aeron.benchmarks.aeron.cluster.failover.control.server.hostname";
    public static final String FAILOVER_CONTROL_SERVER_PORT_PROP_NAME =
        "io.aeron.benchmarks.aeron.cluster.failover.control.server.port";
    public static final String FAILOVER_CONTROL_ENDPOINTS_PROP_NAME =
        "io.aeron.benchmarks.aeron.cluster.failover.control.endpoints";
    public static final String FAILOVER_DELAY_PROP_NAME =
        "io.aeron.benchmarks.aeron.cluster.failover.delay";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    private static final int SEND_ATTEMPTS = 3;

    private AeronUtil()
    {
    }

    public static int receiverCount()
    {
        return Integer.getInteger(NUMBER_OF_RECEIVERS_PROP_NAME, 1);
    }

    public static int receiverIndex()
    {
        return Integer.getInteger(RECEIVER_INDEX_PROP_NAME, 0);
    }

    public static void validateMessageLength(final int messageLength)
    {
        if (messageLength < MIN_MESSAGE_LENGTH)
        {
            throw new IllegalArgumentException("Message length must be at least " + MIN_MESSAGE_LENGTH);
        }
    }

    public static long connectionTimeoutNs()
    {
        final String value = getProperty(CONNECTION_TIMEOUT_PROP_NAME);
        if (isEmpty(value))
        {
            return TimeUnit.SECONDS.toNanos(60);
        }

        return parseDuration(CONNECTION_TIMEOUT_PROP_NAME, value);
    }

    public static String destinationChannel()
    {
        final String property = getProperty(DESTINATION_CHANNEL_PROP_NAME);
        if (isEmpty(property))
        {
            return "aeron:udp?endpoint=localhost:13333|mtu=1408";
        }

        return property;
    }

    public static int destinationStreamId()
    {
        final String property = getProperty(DESTINATION_STREAM_PROP_NAME);
        if (isEmpty(property))
        {
            return 77777;
        }

        return Integer.parseInt(property);
    }

    public static String sourceChannel()
    {
        final String property = getProperty(SOURCE_CHANNEL_PROP_NAME);
        if (isEmpty(property))
        {
            return "aeron:udp?endpoint=localhost:13334|mtu=1408";
        }

        return property;
    }

    public static int sourceStreamId()
    {
        final String property = getProperty(SOURCE_STREAM_PROP_NAME);
        if (isEmpty(property))
        {
            return 55555;
        }

        return Integer.parseInt(property);
    }

    public static String recordChannel()
    {
        final String property = getProperty(RECORD_CHANNEL_PROP_NAME);
        if (isEmpty(property))
        {
            return IPC_CHANNEL;
        }
        return property;
    }

    public static int recordStream()
    {
        final String property = getProperty(RECORD_STREAM_PROP_NAME);
        if (isEmpty(property))
        {
            return 99999;
        }
        return Integer.parseInt(property);
    }

    public static String replayChannel()
    {
        final String property = getProperty(REPLAY_CHANNEL_PROP_NAME);
        if (isEmpty(property))
        {
            return "aeron:udp?endpoint=localhost:0";
        }

        return property;
    }

    public static int replayStreamId()
    {
        final String property = getProperty(REPLAY_STREAM_PROP_NAME);
        if (isEmpty(property))
        {
            return 88888;
        }

        return Integer.parseInt(property);
    }

    public static boolean embeddedMediaDriver()
    {
        return getBoolean(EMBEDDED_MEDIA_DRIVER_PROP_NAME);
    }

    public static IdleStrategy idleStrategy()
    {
        return Configuration.newIdleStrategy(getProperty(IDLE_STRATEGY_PROP_NAME));
    }

    public static MediaDriver launchEmbeddedMediaDriverIfConfigured()
    {
        if (embeddedMediaDriver())
        {
            return MediaDriver.launch(new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .spiesSimulateConnection(true));
        }

        return null;
    }

    public static ArchivingMediaDriver launchArchivingMediaDriver()
    {
        return embeddedMediaDriver() ? launchArchiveWithEmbeddedDriver() : launchArchiveWithStandaloneDriver();
    }

    public static long awaitRecordingStart(final Aeron aeron, final int publicationSessionId, final long archiveId)
    {
        final CountersReader counters = aeron.countersReader();
        int counterId;
        do
        {
            counterId = findCounterIdBySession(counters, publicationSessionId, archiveId);
        }
        while (NULL_COUNTER_ID == counterId);

        return getRecordingId(counters, counterId);
    }

    public static long findLastRecordingId(
        final AeronArchive aeronArchive, final String recordingChannel, final int recordingStreamId)
    {
        final MutableLong lastRecordingId = new MutableLong();

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        int foundCount;
        do
        {
            foundCount = aeronArchive.listRecordingsForUri(0, 1, recordingChannel, recordingStreamId, consumer);
        }
        while (0 == foundCount);

        return lastRecordingId.get();
    }

    public static void pipeMessages(
        final Subscription subscription, final ExclusivePublication publication, final AtomicBoolean running)
    {
        final IdleStrategy idleStrategy = idleStrategy();
        final BufferClaim bufferClaim = new BufferClaim();
        final FragmentHandler dataHandler =
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

        final Image image = subscription.imageAtIndex(0);
        while (true)
        {
            final int fragmentsRead = image.poll(dataHandler, FRAGMENT_LIMIT);
            if (0 == fragmentsRead)
            {
                if (!running.get() || image.isClosed())
                {
                    break;
                }
            }

            idleStrategy.idle(fragmentsRead);
        }
    }

    public static int sendMessages(
        final ExclusivePublication publication,
        final BufferClaim bufferClaim,
        final int numberOfMessages,
        final int messageLength,
        final long timestamp,
        final long checksum,
        final MutableInteger receiverIndex,
        final int receiverCount)
    {
        int count = 0;
        for (int i = 0; i < numberOfMessages; i++)
        {
            int retryCount = SEND_ATTEMPTS;
            long result;
            while ((result = publication.tryClaim(messageLength, bufferClaim)) < 0)
            {
                checkPublicationResult(result);
                if (0 == --retryCount)
                {
                    return count;
                }
            }
            final MutableDirectBuffer buffer = bufferClaim.buffer();
            final int offset = bufferClaim.offset();
            buffer.putLong(offset + TIMESTAMP_OFFSET, timestamp, LITTLE_ENDIAN);

            // set receiverIndex to ensure only one reply will be received
            buffer.putInt(offset + RECEIVER_INDEX_OFFSET, receiverIndex.get(), LITTLE_ENDIAN);
            receiverIndex.set(BitUtil.next(receiverIndex.get(), receiverCount));

            buffer.putLong(offset + messageLength - SIZE_OF_LONG, checksum, LITTLE_ENDIAN);
            bufferClaim.commit();
            count++;
        }

        return count;
    }

    public static Object installSignalHandler(final Runnable onSignal)
    {
        try
        {
            final Class<?> signalHandlerClass =
                Class.forName("org.agrona.concurrent.ShutdownSignalBarrier.SignalHandler");
            final Constructor<ShutdownSignalBarrier> constructor =
                ShutdownSignalBarrier.class.getConstructor(signalHandlerClass);
            final Object signalHandler = Proxy.newProxyInstance(
                signalHandlerClass.getClassLoader(),
                new Class[]{ signalHandlerClass },
                (proxy, method, args) ->
                {
                    if (method.getDeclaringClass() == Object.class)
                    {
                        if ("toString".equals(method.getName()))
                        {
                            return onSignal.toString();
                        }
                    }
                    else if (method.getDeclaringClass() == signalHandlerClass)
                    {
                        onSignal.run();
                    }
                    return null;
                });
            return constructor.newInstance(signalHandler);
        }
        catch (final ReflectiveOperationException ex)
        {
            try
            {
                final Class<?> sigIntClass = Class.forName("org.agrona.concurrent.SigInt");
                final Method method = sigIntClass.getMethod("register", Runnable.class);
                method.invoke(null, onSignal);
            }
            catch (final ReflectiveOperationException ex2)
            {
                final RuntimeException exception = new RuntimeException(ex);
                exception.addSuppressed(ex2);
                throw exception;
            }
            return null;
        }
    }

    public static void close(final Object closeable)
    {
        if (closeable instanceof AutoCloseable)
        {
            CloseHelper.close((AutoCloseable)closeable);
        }
    }

    public static void yieldUninterruptedly()
    {
        Thread.yield();
        if (Thread.currentThread().isInterrupted())
        {
            throw new IllegalStateException("Interrupted while yielding...");
        }
    }

    public static long replayFullRecording(
        final AeronArchive aeronArchive, final long recordingId, final String replayChannel, final int replayStreamId)
    {
        while (true)
        {
            try
            {
                return aeronArchive.startReplay(recordingId, 0, MAX_VALUE, replayChannel, replayStreamId);
            }
            catch (final ArchiveException ex)
            {
                ex.printStackTrace();
                yieldUninterruptedly();
            }
        }
    }

    public static void awaitConnected(
        final BooleanSupplier connection,
        final long connectionTimeoutNs,
        final NanoClock clock)
    {
        final long deadlineNs = clock.nanoTime() + connectionTimeoutNs;
        while (!connection.getAsBoolean())
        {
            if (clock.nanoTime() < deadlineNs)
            {
                yieldUninterruptedly();
            }
            else
            {
                throw new IllegalStateException("Failed to connect within timeout of " + connectionTimeoutNs + "ns");
            }
        }
    }

    public static void checkPublicationResult(final long result)
    {
        if (result == CLOSED ||
            result == NOT_CONNECTED ||
            result == MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("Publication error: " + Publication.errorString(result));
        }
    }

    public static ErrorHandler printingErrorHandler(final String context)
    {
        return (Throwable throwable) ->
        {
            System.err.println(context);
            throwable.printStackTrace(System.err);
        };
    }

    public static void dumpAeronStats(final File cncFile, final Path statsFile, final Path errorFile)
    {
        Thread.interrupted(); // clear interrupt
        if (cncFile.exists() && cncFile.length() >= CncFileDescriptor.META_DATA_LENGTH)
        {
            final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(
                cncFile, FileChannel.MapMode.READ_ONLY, "CnC file");
            final UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
            try (PrintWriter statsWriter = newWriter(statsFile))
            {
                statsWriter.format("CnC version: %s%n",
                    SemanticVersion.toString(cncMetaDataBuffer.getInt(CncFileDescriptor.CNC_VERSION_FIELD_OFFSET)));
                statsWriter.format("PID: %d%n",
                    cncMetaDataBuffer.getLong(CncFileDescriptor.PID_FIELD_OFFSET));
                statsWriter.format("Start time: %s%n",
                    DATE_FORMAT.format(
                        new Date(cncMetaDataBuffer.getLong(CncFileDescriptor.START_TIMESTAMP_FIELD_OFFSET))));
                statsWriter.println("================================================================");

                final CountersReader countersReader = new CountersReader(
                    createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer),
                    createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));

                countersReader.forEach(
                    (counterId, label) ->
                    {
                        final long value = countersReader.getCounterValue(counterId);
                        statsWriter.format("%3d: %,20d - %s%n", counterId, value, label);
                    });

                saveErrors(CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), errorFile);
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
            finally
            {
                IoUtil.unmap(cncByteBuffer);
            }
        }
    }

    public static void dumpArchiveErrors(final File archiveDir, final Path destFile)
    {
        Thread.interrupted(); // clear interrupt
        final File file = resolveMarkFile(archiveDir, ArchiveMarkFile.FILENAME, ArchiveMarkFile.LINK_FILENAME);
        if (file.exists() && file.length() > 0)
        {
            try (ArchiveMarkFile markFile = new ArchiveMarkFile(
                file.getParentFile(), file.getName(), SystemEpochClock.INSTANCE, 0, (s) -> {}))
            {
                saveErrors(markFile.errorBuffer(), destFile);
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void dumpClusterErrors(
        final Path resultFile, final File clusterDir, final String markFileName, final String linkFileName)
    {
        Thread.interrupted(); // clear interrupt
        final File file = resolveMarkFile(clusterDir, markFileName, linkFileName);
        if (file.exists() && file.length() > 0)
        {
            try (ClusterMarkFile markFile = new ClusterMarkFile(
                file.getParentFile(), file.getName(), SystemEpochClock.INSTANCE, 0, (s) -> {}))
            {
                saveErrors(markFile.errorBuffer(), resultFile);
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static File resolveMarkFile(final File parentDir, final String markFileName, final String linkFileName)
    {
        final File linkFile = new File(parentDir, linkFileName);
        if (linkFile.exists() && linkFile.isFile())
        {
            try
            {
                final byte[] bytes = Files.readAllBytes(linkFile.toPath());
                final String markFileDirPath = new String(bytes, US_ASCII).trim();
                return new File(markFileDirPath, markFileName);
            }
            catch (final IOException ex)
            {
                throw new RuntimeException("failed to read link file=" + linkFile, ex);
            }
        }
        else
        {
            return new File(parentDir, markFileName);
        }
    }

    private static PrintWriter newWriter(final Path resultFile) throws IOException
    {
        return new PrintWriter(Files.newBufferedWriter(resultFile, US_ASCII, WRITE, CREATE, TRUNCATE_EXISTING));
    }

    private static void saveErrors(final AtomicBuffer errorBuffer, final Path errorFile) throws IOException
    {
        if (ErrorLogReader.hasErrors(errorBuffer))
        {
            try (PrintWriter writer = newWriter(errorFile))
            {
                final int distinctErrorCount = ErrorLogReader.read(
                    errorBuffer,
                    (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                    writer.format(
                    "%n%d observations from %s to %s for:%n %s%n",
                    observationCount,
                    DATE_FORMAT.format(new Date(firstObservationTimestamp)),
                    DATE_FORMAT.format(new Date(lastObservationTimestamp)),
                    encodedException));
                writer.format("%d distinct errors observed.%n", distinctErrorCount);
            }
        }
    }
}
