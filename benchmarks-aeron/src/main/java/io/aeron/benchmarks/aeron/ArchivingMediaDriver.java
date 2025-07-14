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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;

public final class ArchivingMediaDriver implements AutoCloseable
{
    final MediaDriver driver;
    final Archive archive;

    private ArchivingMediaDriver(final MediaDriver driver, final Archive archive)
    {
        this.driver = driver;
        this.archive = archive;
    }

    public void close()
    {
        CloseHelper.closeAll(archive, driver);
    }

    public static ArchivingMediaDriver launchArchiveWithStandaloneDriver()
    {
        return new ArchivingMediaDriver(null, Archive.launch(new Archive.Context().deleteArchiveOnStart(true)));
    }

    public static ArchivingMediaDriver launchArchiveWithEmbeddedDriver()
    {
        MediaDriver driver = null;
        Archive archive = null;
        try
        {
            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .spiesSimulateConnection(true);

            driver = MediaDriver.launch(driverCtx);

            final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName())
                .deleteArchiveOnStart(true);

            final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
            final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
                archiveCtx.errorCounter() : new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);

            final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
                archiveCtx.errorHandler() : driverCtx.errorHandler();

            archive = Archive.launch(archiveCtx
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .aeronDirectoryName(driverCtx.aeronDirectoryName())
                .errorHandler(errorHandler)
                .errorCounter(errorCounter));

            return new ArchivingMediaDriver(driver, archive);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(archive, driver);
            throw ex;
        }
    }
}
