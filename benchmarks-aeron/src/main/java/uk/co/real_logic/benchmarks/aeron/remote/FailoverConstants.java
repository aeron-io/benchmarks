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

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

public final class FailoverConstants
{
    public static final int MESSAGE_HEADER_LENGTH = SIZE_OF_INT;

    public static final int ECHO_MESSAGE_TYPE = 0;
    public static final int ECHO_SEQUENCE_OFFSET = MESSAGE_HEADER_LENGTH;
    public static final int ECHO_TIMESTAMP_OFFSET = ECHO_SEQUENCE_OFFSET + SIZE_OF_INT;
    public static final int ECHO_MESSAGE_LENGTH = ECHO_TIMESTAMP_OFFSET + SIZE_OF_LONG;

    public static final int SYNC_MESSAGE_TYPE = 1;
    public static final int SYNC_SEQUENCE_OFFSET = MESSAGE_HEADER_LENGTH;
    public static final int SYNC_MESSAGE_LENGTH = SYNC_SEQUENCE_OFFSET + SIZE_OF_INT;

    public static final int LEADER_STEP_DOWN_COMMAND = 1;
    public static final int RESTART_COMMAND = 2;
    public static final int CYCLE_NODE_COMMAND = 3;

    private FailoverConstants()
    {
    }
}
