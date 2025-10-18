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

import io.aeron.archive.Archive;
import io.aeron.benchmarks.Configuration;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.CLUSTER_SERVICE_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.DEFAULT_SNAPSHOT_SIZE;
import static io.aeron.benchmarks.aeron.AeronUtil.FAILOVER_CONTROL_SERVER_HOSTNAME_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.FAILOVER_CONTROL_SERVER_PORT_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.SNAPSHOT_SIZE_PROP_NAME;
import static io.aeron.benchmarks.aeron.AeronUtil.printingErrorHandler;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;
import static org.agrona.SystemUtil.getSizeAsLong;

public final class ClusterNode
{
    @SuppressWarnings("MethodLength")
    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path logsDir = Configuration.resolveLogsDir();

        final Archive.Context archiveContext = new Archive.Context()
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false);

        final String aeronDirectoryName = archiveContext.aeronDirectoryName();

        // In local tests we could be racing with the Media Driver to start.
        // Await the driver dir to exist or creating the cluster mark file will fail.
        awaitPathExists(aeronDirectoryName);

        final EpochClock epochClock = SystemEpochClock.INSTANCE;
        final File clusterDir = new File(ClusteredServiceContainer.Configuration.clusterDirName());
        final int memberId = ConsensusModule.Configuration.clusterMemberId();
        final Supplier<IdleStrategy> idleStrategySupplier = () -> BusySpinIdleStrategy.INSTANCE;

        final ShutdownSignalBarrier signalBarrier = new ShutdownSignalBarrier();
        try
        {
            final Component<ConsensusModule> consensusModule = new Component<>(() ->
            {
                final ConsensusModule.Context ctx = new ConsensusModule.Context()
                    .errorHandler(printingErrorHandler("consensus-module"))
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(clusterDir)
                    .epochClock(epochClock)
                    .clusterMemberId(memberId)
                    .idleStrategySupplier(idleStrategySupplier)
                    .markFileDir(new File(aeronDirectoryName))
                    .terminationHook(signalBarrier::signalAll);

                return ConsensusModule.launch(ctx);
            });

            final Type type = Type.fromSystemProperty();
            final AtomicReference<Cluster.Role> roleRef = new AtomicReference<>();
            final int serviceId = ClusteredServiceContainer.Configuration.serviceId();

            final Component<ClusteredServiceContainer> clusteredServiceContainer = new Component<>(() ->
            {
                final ClusteredService clusteredService;
                if (type == Type.FAILOVER)
                {
                    clusteredService = new FailoverClusteredService(roleRef);
                }
                else
                {
                    final long snapshotSize = getSizeAsLong(SNAPSHOT_SIZE_PROP_NAME, DEFAULT_SNAPSHOT_SIZE);
                    clusteredService = new EchoClusteredService(snapshotSize);
                }

                final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
                    .clusteredService(clusteredService)
                    .errorHandler(printingErrorHandler("service-container"))
                    .aeronDirectoryName(aeronDirectoryName)
                    .clusterDir(clusterDir)
                    .epochClock(epochClock)
                    .serviceId(serviceId)
                    .idleStrategySupplier(idleStrategySupplier)
                    .markFileDir(new File(aeronDirectoryName))
                    .terminationHook(signalBarrier::signalAll);

                return ClusteredServiceContainer.launch(ctx);
            });

            IoUtil.delete(clusterDir, false);

            try (Archive archive = Archive.launch(archiveContext);
                Component<ConsensusModule> cm = consensusModule.start();
                Component<ClusteredServiceContainer> csc = clusteredServiceContainer.start();
                FailoverControlServer failoverControlServer = createFailoverControlServer(
                    type,
                    memberId,
                    consensusModule,
                    clusteredServiceContainer,
                    roleRef))
            {
                signalBarrier.await();

                final String prefix = "cluster-node-" + memberId + "-";
                AeronUtil.dumpClusterErrors(
                    logsDir.resolve(prefix + "clustered-service-errors.txt"),
                    clusterDir,
                    ClusterMarkFile.markFilenameForService(serviceId),
                    ClusterMarkFile.linkFilenameForService(serviceId));
                AeronUtil.dumpClusterErrors(
                    logsDir.resolve(prefix + "consensus-module-errors.txt"),
                    clusterDir,
                    ClusterMarkFile.FILENAME,
                    ClusterMarkFile.LINK_FILENAME);
                AeronUtil.dumpArchiveErrors(
                    archive.context().archiveDir(), logsDir.resolve(prefix + "archive-errors.txt"));
                AeronUtil.dumpAeronStats(
                    archive.context().aeron().context().cncFile(),
                    logsDir.resolve(prefix + "aeron-stat.txt"),
                    logsDir.resolve(prefix + "errors.txt"));
            }
        }
        finally
        {
            AeronUtil.close(signalBarrier);
        }
    }

    private static FailoverControlServer createFailoverControlServer(
        final Type type,
        final int memberId,
        final Component<ConsensusModule> consensusModule,
        final Component<ClusteredServiceContainer> clusteredServiceContainer,
        final AtomicReference<Cluster.Role> roleRef)
    {
        if (type == Type.FAILOVER)
        {
            final String hostname = getRequiredProperty(FAILOVER_CONTROL_SERVER_HOSTNAME_PROP_NAME);
            final int port = Integer.parseInt(getRequiredProperty(FAILOVER_CONTROL_SERVER_PORT_PROP_NAME));

            final FailoverControlServer failoverControlServer = new FailoverControlServer(
                hostname,
                port,
                memberId,
                roleRef,
                consensusModule,
                clusteredServiceContainer,
                printingErrorHandler("FailoverControlServer"));

            failoverControlServer.start();

            return failoverControlServer;
        }

        return null;
    }

    private static String getRequiredProperty(final String propertyName)
    {
        final String value = System.getProperty(propertyName);
        if (value == null)
        {
            throw new NullPointerException(propertyName + " must be set");
        }
        return value;
    }

    private static void awaitPathExists(final String path)
    {
        final File file = new File(path);

        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() - deadline < 0)
        {
            if (file.exists())
            {
                return;
            }
            Thread.yield();
        }

        throw new RuntimeException("Timed out waiting for " + path);
    }

    private enum Type
    {
        ECHO,
        FAILOVER;

        public static Type fromSystemProperty()
        {
            final String clusteredServiceName = System.getProperty(CLUSTER_SERVICE_PROP_NAME);
            return "failover".equals(clusteredServiceName) ? FAILOVER : ECHO;
        }
    }
}
