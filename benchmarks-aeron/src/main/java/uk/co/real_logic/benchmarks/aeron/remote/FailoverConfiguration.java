/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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

import org.agrona.SystemUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.FAILOVER_CONTROL_ENDPOINTS_PROP_NAME;
import static uk.co.real_logic.benchmarks.aeron.remote.AeronUtil.FAILOVER_DELAY_PROP_NAME;

public final class FailoverConfiguration
{
    private final List<InetSocketAddress> controlEndpoints;
    private final long failoverDelayNs;

    private FailoverConfiguration(final Builder builder)
    {
        this.controlEndpoints = new ArrayList<>(builder.controlEndpoints);
        this.failoverDelayNs = builder.failoverDelayNs;
    }

    public List<InetSocketAddress> controlEndpoints()
    {
        return Collections.unmodifiableList(controlEndpoints);
    }

    public long failoverDelayNs()
    {
        return failoverDelayNs;
    }

    public static final class Builder
    {
        private List<InetSocketAddress> controlEndpoints;
        private long failoverDelayNs = Long.MIN_VALUE;

        public Builder controlEndpoints(final List<InetSocketAddress> controlEndpoints)
        {
            this.controlEndpoints = controlEndpoints;
            return this;
        }

        public Builder failoverDelayNs(final long failoverDelayNs)
        {
            this.failoverDelayNs = failoverDelayNs;
            return this;
        }

        public FailoverConfiguration build()
        {
            if (failoverDelayNs == Long.MIN_VALUE)
            {
                throw new IllegalStateException("failoverDelayNs must be set");
            }

            return new FailoverConfiguration(this);
        }
    }

    public static FailoverConfiguration fromSystemProperties()
    {
        final Builder builder = new Builder();

        final String controlEndpoints = System.getProperty(FAILOVER_CONTROL_ENDPOINTS_PROP_NAME);
        if (controlEndpoints == null || controlEndpoints.isEmpty())
        {
            throw new IllegalStateException(FAILOVER_CONTROL_ENDPOINTS_PROP_NAME + " must be set");
        }
        builder.controlEndpoints(parseEndpoints(controlEndpoints));

        builder.failoverDelayNs(SystemUtil.getDurationInNanos(FAILOVER_DELAY_PROP_NAME, TimeUnit.SECONDS.toNanos(30)));

        return builder.build();
    }

    private static List<InetSocketAddress> parseEndpoints(final String endpoints)
    {
        final String[] endpointsArray = endpoints.split(",");

        final List<InetSocketAddress> result = new ArrayList<>(endpointsArray.length);

        for (final String endpoint : endpointsArray)
        {
            result.add(parseEndpoint(endpoint));
        }

        return result;
    }

    private static InetSocketAddress parseEndpoint(final String endpoint)
    {
        final int separator = endpoint.lastIndexOf(':');

        if (separator == -1)
        {
            throw new IllegalArgumentException("endpoint must be in <hostname>:<port> format");
        }

        final String hostname = endpoint.substring(0, separator);
        final int port = Integer.parseInt(endpoint.substring(separator + 1));

        return new InetSocketAddress(hostname, port);
    }
}
