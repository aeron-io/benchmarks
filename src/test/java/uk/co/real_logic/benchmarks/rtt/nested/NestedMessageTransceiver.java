/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.benchmarks.rtt.nested;

import uk.co.real_logic.benchmarks.rtt.Configuration;
import uk.co.real_logic.benchmarks.rtt.MessageRecorder;
import uk.co.real_logic.benchmarks.rtt.MessageTransceiver;

public class NestedMessageTransceiver extends MessageTransceiver
{
    public NestedMessageTransceiver(final MessageRecorder messageRecorder)
    {
        super(messageRecorder);
    }

    public void init(final Configuration configuration) throws Exception
    {
    }

    public void destroy() throws Exception
    {
    }

    public int send(final int numberOfMessages, final int messageLength, final long timestamp, final long checksum)
    {
        return 0;
    }

    public void receive()
    {
    }
}
