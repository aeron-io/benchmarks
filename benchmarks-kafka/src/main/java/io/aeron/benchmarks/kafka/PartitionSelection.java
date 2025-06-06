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
package io.aeron.benchmarks.kafka;

import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.of;

public enum PartitionSelection
{
    /**
     * Indicates that a partition should be set explicitly.
     */
    EXPLICIT,

    /**
     * Indicates that a partition should be selected using a key from the record.
     */
    BY_KEY,

    /**
     * Indicates that a partition should be selected at random by the broker when message arrives.
     */
    RANDOM;

    private static final Map<String, PartitionSelection> BY_NAME = of(values())
        .collect(toMap(Enum::name, identity()));

    public static PartitionSelection byName(final String name)
    {
        return BY_NAME.getOrDefault(name, EXPLICIT);
    }
}
