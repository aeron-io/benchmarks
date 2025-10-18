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
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

import static io.aeron.benchmarks.PropertiesUtil.loadPropertiesFiles;
import static io.aeron.benchmarks.PropertiesUtil.mergeWithSystemProperties;
import static io.aeron.benchmarks.aeron.AeronUtil.printingErrorHandler;
import static org.agrona.PropertyAction.PRESERVE;
import static org.agrona.PropertyAction.REPLACE;

public final class ClusterBackupNode
{
    public static void main(final String[] args)
    {
        mergeWithSystemProperties(PRESERVE, loadPropertiesFiles(new Properties(), REPLACE, args));
        final Path logsDir = Configuration.resolveLogsDir();

        final Archive.Context archiveContext = new Archive.Context()
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false);

        final ShutdownSignalBarrier signalBarrier = new ShutdownSignalBarrier();
        try
        {
            final ClusterBackup.Context clusterBackupContext = new ClusterBackup.Context()
                .deleteDirOnStart(true)
                .errorHandler(printingErrorHandler("cluster-backup"))
                .aeronDirectoryName(archiveContext.aeronDirectoryName())
                .markFileDir(new File(archiveContext.aeronDirectoryName()))
                .epochClock(SystemEpochClock.INSTANCE)
                .terminationHook(signalBarrier::signalAll);

            try (Archive archive = Archive.launch(archiveContext);
                ClusterBackup clusterBackup = ClusterBackup.launch(clusterBackupContext))
            {
                signalBarrier.await();

                final String prefix = "cluster-backup-node-";
                AeronUtil.dumpClusterErrors(
                    logsDir.resolve(prefix + "backup-errors.txt"),
                    clusterBackup.context().clusterDir(),
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
}
