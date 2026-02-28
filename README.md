# Benchmarks

This project is a collection of the various benchmarks primarily targeting the [Aeron](https://github.com/aeron-io/aeron) project.
The benchmarks can be divided into two major categories:
- [Messaging (remote) benchmarks](#remote-benchmarks-multiple-machines).

    The core of the remote benchmarks is implemented by the [`LoadTestRig`](https://github.com/aeron-io/benchmarks/blob/master/benchmarks-api/src/main/java/uk/co/real_logic/benchmarks/remote/LoadTestRig.java)
    class which is a benchmarking harness that is sending messages to the remote node(s) and timing the responses as
    they are received. During a test run the `LoadTestRig` sends messages at the specified fixed rate with the specified
    payload size and the burst size. In the end it produces a latency histogram for an entire test run.

    The `LoadTestRig` relies on the implementation of the [`MessageTransceiver`](https://github.com/aeron-io/benchmarks/blob/master/benchmarks-api/src/main/java/uk/co/real_logic/benchmarks/remote/MessageTransceiver.java)
    abstract class which is responsible for sending and receiving messages to/from the remote node.

    *NB: These benchmarks are written in Java, but they can target systems in other languages provided there is a
    Java client for it.*


- [Other benchmarks](#other-benchmarks-single-machine).

   A collection of the benchmarks that run on a single machine (e.g. Agrona ring buffer, Aeron IPC, Aeorn C++
   benchmarks, JDK queues etc.).

## Systems under test

This section lists the systems under test which implement the remote benchmarks and the corresponding test scenarios.

### Aeron

For [Aeron](https://aeron.io/) the following test scenarios were implemented:

1. Echo benchmark.

   An Aeron Transport benchmark which consist of a client process that sends messages over UDP via an exclusive 
   publication using either zero-copy `tryClaim` or `offer` API. Configuration option
   `io.aeron.benchmarks.aeron.use.try.claim` controls which API is used. If no value is specified then `tryClaim` will
   be used.
   And the server process which echoes the complete (re-assembled) received messages back using `offer` API.


2. Live replay from a remote Archive.

    The client publishes messages to the server using publication over UDP. The server pipes those messages into a local 
    IPC publication which records them into an Archive. Finally, the client subscribes to the replay from that Archive
    over UDP and receives persisted messages.


3. Live recording to a local Archive.

    The client publishes messages over UDP to the server. It also has a recording running on that publication using
    local Archive. The server simply pipes message back. Finally, the client performs a controlled poll on the 
    subscription from the server limited by the "recording progress" which it gets via the recording events.

    The biggest difference between scenario 2 and this scenario is that there is no replay of recorded messages and
    hence no reading from disc while still allowing consumption of only those messages that were successfully persisted.


4. Cluster benchmark.

   The client sends messages to the Aeron Cluster over UDP. The Cluster sequences the messages into a log, reaches the
   consensus on the received messages, processes them and then replies to the client over UDP.


5. Aeron Echo MDC benchmark. 

   An extension to Aeron Echo benchmark which uses an MDC (or a multicast) channel to send the same data to multiple
   receivers. Only one receiver at a time will respond to a given incoming message ensuring that the number of replies
   matches the number of messages sent.


6. Aeron Archive Replay MDC benchmark.

   Aeron Archive benchmark that multiple replays. The benchmark consists of at least three nodes: 
   - the client node sending the data
   - the Archive node recording the data stream to disc
   - the replay nodes requesting replay of the recording from the Archive
   
   Similar to the Aeron Echo MDC benchmark only one replay node at a time will send a response message back to the
   client node thus ensuring that the number of messages sent and the number of replays match.

Please the documentation in the ``scripts/aeron`` directory for more information.

### gRPC

For [gRPC](https://grpc.io/) there is only echo benchmark with a single implementation:
- Streaming client - client uses streaming API to send and receive messages.

Please read the documentation in the ``scripts/grpc`` directory for more information.

### Kafka

Unlike the gRPC that simply echoes messages the [Kafka](https://kafka.apache.org/) will persist them so the benchmark is
similar to the Aeron's replay from a remote Archive.

Please read the documentation in the `scripts/kafka` directory for more information.

## Remote benchmarks (multiple machines)

The `scripts` directory contains scripts to run the _remote benchmarks_, i.e. the benchmarks that involve multiple
machines where one is the _client_ (the benchmarking harness) and the rest are the _server nodes_.

The `remote.io.aeron.benchmarks.LoadTestRig` class implements the benchmarking harness. Whereas the
`remote.io.aeron.benchmarks.Configuration` class provides the configuration for the benchmarking harness.

Before the benchmarks can be executed they have to be built. This can be done by running the following command in the
root directory of this project:
```bash
./gradlew clean deployTar
```
Once complete it will create a `build/distributions/benchmarks.tar` file that should be deployed to the remote machines.

### Running benchmarks via SSH (i.e. automated way)

The easiest way to run the benchmarks is by using the `remote_*` wrapper scripts which invoke scripts remotely using
the SSH protocol. When the script finishes its execution it will download an archive with the results (histograms).

The following steps are required to run the benchmarks:
1. Build the tar file (see above).
2. Copy tar file to the destination machines and unpack it, i.e. `tar xf benchmarks.tar -C <destination_dir>`.
3. On the local machine create a wrapper script that sets all the necessary configuration parameters for the target
benchmark. See example below.
4. Run the wrapper script from step 3.
5. Once the execution is finished an archive file with the results will be downloaded to the local machine. By default,
it will be placed under the `scripts` directory in the project folder.

Here is an example of a wrapper script for the Aeron echo benchmarks.
_NB: All the values in angle brackets (`<...>`) will have to be replaced with the actual values._
```bash
# SSH connection properties
export SSH_CLIENT_USER=<SSH client machine user>
export SSH_CLIENT_KEY_FILE=<private SSH key to connect to the client machine>
export SSH_CLIENT_NODE=<IP of the client machine>
export SSH_SERVER_USER=<SSH server machine user>
export SSH_SERVER_KEY_FILE=<private SSH key to connect to the server machine>
export SSH_SERVER_NODE=<IP of the server machine>

# Set of required configuration options
export CLIENT_BENCHMARKS_PATH=<directory containing the unpacked benchmarks.tar>
export CLIENT_JAVA_HOME=<path to JAVA_HOME (JDK 17+)>
export CLIENT_DRIVER_CONDUCTOR_CPU_CORE=<CPU core to pin the 'conductor' thread>
export CLIENT_DRIVER_SENDER_CPU_CORE=<CPU core to pin the 'sender' thread>
export CLIENT_DRIVER_RECEIVER_CPU_CORE=<CPU core to pin the 'receiver' thread>
export CLIENT_LOAD_TEST_RIG_MAIN_CPU_CORE=<CPU core to pin 'load-test-rig' thread>
export CLIENT_NON_ISOLATED_CPU_CORES=<a set of non-isolated CPU cores to run the auxilary/JVM client threads on>
export CLIENT_CPU_NODE=<CPU node (socket) to run the client processes on (both MD and the test rig)>
export CLIENT_AERON_DPDK_GATEWAY_IPV4_ADDRESS=
export CLIENT_AERON_DPDK_LOCAL_IPV4_ADDRESS=
export CLIENT_SOURCE_CHANNEL="aeron:udp?endpoint=<SOURCE_IP>:13100|interface=<SOURCE_IP>/24"
export CLIENT_DESTINATION_CHANNEL="aeron:udp?endpoint=<DESTINATION_IP>:13000|interface=<DESTINATION_IP>/24"
export SERVER_BENCHMARKS_PATH=<directory containing the unpacked benchmarks.tar>
export SERVER_JAVA_HOME=<path to JAVA_HOME (JDK 17+)>
export SERVER_DRIVER_CONDUCTOR_CPU_CORE=<CPU core to pin the 'conductor' thread>
export SERVER_DRIVER_SENDER_CPU_CORE=<CPU core to pin the 'sender' thread>
export SERVER_DRIVER_RECEIVER_CPU_CORE=<CPU core to pin the 'receiver' thread>
export SERVER_ECHO_CPU_CORE=<CPU core to pin 'echo' thread>
export SERVER_NON_ISOLATED_CPU_CORES=<a set of non-isolated CPU cores to run the auxilary/JVM server threads on>
export SERVER_CPU_NODE=<CPU node (socket) to run the server processes on (both MD and the echo node)>
export SERVER_AERON_DPDK_GATEWAY_IPV4_ADDRESS=
export SERVER_AERON_DPDK_LOCAL_IPV4_ADDRESS=
export SERVER_SOURCE_CHANNEL="${CLIENT_SOURCE_CHANNEL}"
export SERVER_DESTINATION_CHANNEL="${CLIENT_DESTINATION_CHANNEL}"

# (Optional) Overrides for the runner configuration options 
#export MESSAGE_LENGTH="288" # defaults to "32,288,1344"
#export MESSAGE_RATE="100K"  # defaults to "1M,500K,100K"

# Invoke the actual script and optionally configure specific parameters
"aeron/remote-echo-benchmarks" --client-drivers "java" --server-drivers "java" --mtu 8K --context "my-test"

## DPDK-specific configuration
#export CLIENT_AERON_DPDK_GATEWAY_IPV4_ADDRESS=<SOURCE_DPDK_GATEWAY_ADDRESS>
#export SERVER_AERON_DPDK_GATEWAY_IPV4_ADDRESS=<DESTINATION_DPDK_GATEWAY_ADDRESS>
#export CLIENT_AERON_DPDK_LOCAL_IPV4_ADDRESS=<SOURCE_DPDK_ADDRESS>
#export SERVER_AERON_DPDK_LOCAL_IPV4_ADDRESS=<DESTINATION_DPDK_ADDRESS>
#export CLIENT_SOURCE_CHANNEL="aeron:udp?endpoint=${CLIENT_AERON_DPDK_LOCAL_IPV4_ADDRESS}:13100"
#export CLIENT_DESTINATION_CHANNEL="aeron:udp?endpoint=${SERVER_AERON_DPDK_LOCAL_IPV4_ADDRESS}:13000"
#export SERVER_SOURCE_CHANNEL="${CLIENT_SOURCE_CHANNEL}"
#export SERVER_DESTINATION_CHANNEL="${CLIENT_DESTINATION_CHANNEL}"
#"aeron/remote-echo-benchmarks" --client-drivers "c-dpdk" --server-drivers "c-dpdk" --mtu 8K --context "my-test"
```

### Running benchmarks manually (single shot execution)

The following steps are required to run the benchmarks:
1. Build the tar file (see above).
2. Copy tar file to the destination machines and unpack it, i.e. `tar xf benchmarks.tar -C <destination_dir>`.
3. Follow the documentation for a particular benchmark to know which scripts to run and in which order.
4. Run the `benchmark-runner` script specifying the _benchmark client script_ to execute.

Here is an example of running the Aeron echo benchmark using the embedded Java MediaDriver on two nodes:
server (`192.168.0.20`) and client (`192.168.0.10`).
```bash
server:~/benchmarks/scripts$ JVM_OPTS="\
-Dio.aeron.benchmarks.aeron.embedded.media.driver=true \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=192.168.0.10:13000 \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=192.168.0.20:13001" aeron/echo-server

client:~/benchmarks/scripts$ JVM_OPTS="\
-Dio.aeron.benchmarks.aeron.embedded.media.driver=true \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=192.168.0.10:13000 \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=192.168.0.20:13001" \
./benchmark-runner --output-file "aeron-echo-test" --messages "100K" --message-length "288" --iterations 60 "aeron/echo-client"
```
_**Note**: At the end of a single run the server-side process (e.g. `aeron/echo-server`) will exit, i.e. in order to do
another manual run (with different parameters etc.) one has to start the server process again. Alternative is to run the
benchmarks [via the SSH](#running-benchmarks-via-ssh-ie-automated-way)._

### Aggregating the results

To aggregate the results of the multiple runs into a single file use the `aggregate-results` script.

For example if the ``results`` directory contains the following files:
```bash
results
├── echo-test_rate=1000_batch=1_length=32-0.hdr
├── echo-test_rate=1000_batch=1_length=32-1.hdr
├── echo-test_rate=1000_batch=1_length=32-2.hdr
├── echo-test_rate=1000_batch=1_length=32-3.hdr
└── echo-test_rate=1000_batch=1_length=32-4.hdr
```   

Running:
```bash
./aggregate-results results
```

Will produce the following result:
```bash
results
├── echo-test_rate=1000_batch=1_length=32-0.hdr
├── echo-test_rate=1000_batch=1_length=32-1.hdr
├── echo-test_rate=1000_batch=1_length=32-2.hdr
├── echo-test_rate=1000_batch=1_length=32-3.hdr
├── echo-test_rate=1000_batch=1_length=32-4.hdr
├── echo-test_rate=1000_batch=1_length=32-combined.hdr
└── echo-test_rate=1000_batch=1_length=32-report.hgrm
```
where `echo-test_rate=1000_batch=1_length=32-combined.hdr` is the
aggregated histogram of five runs and the `echo-test_rate=1000_batch=1_length=32-report.hgrm` is an export of the
aggregated histogram that can be plotted using http://hdrhistogram.github.io/HdrHistogram/plotFiles.html.

### Plotting the results

Aggregated results can be plotted using the `results-plotter.py` script which uses [hdr-plot](https://github.com/BrunoBonacci/hdr-plot) in order to produce latency plots of the histograms (the library needs to be installed in order to use the script).

Running

```bash
./results-plotter.py results
```

will produce plots in which the histograms are grouped by test scenario by default. It is possible to produce graphs with a different kind of aggregation and to apply filters on the histograms to plot within a directory. Run `./results-plotter.py` (without arguments) in order to get an overview of the capabilities of the plotting script.

## Running on Kubernetes

You will need the following Docker containers built & injected into a repository that you can use.

The tests currently support Aeron Echo testing with either Java or C-DPDK media drivers.

### Components & Containers

**Benchmarks:**

This is the code in *this* repository. It must be built as a Docker container.


**Optional: Aeron DPDK Media driver**

Premium feature.

If required/activated in your test configuration - see https://github.com/aeron-io/premium-extensions/ or ask your support contact at [Adaptive](https://weareadaptive.com/)

This is expected to reside in a container called in an accessible repository.

**Optional: Aeron C Media driver:**

Support coming soon

### Running the tests


1. Build the benchmarks container and push it to a repo that your K8s nodes can pull from:
    ```
    docker build -t <your_repo>:aeron-benchmarks .
    docker push <your_repo>:aeron-benchmarks
    ```
2. Update the following files with configuration from your test environment - you can skip scenario config you don't plan to test
   * `scripts/k8s/base/settings.yml`
   * `scripts/k8s/base/aeron-echo-dpdk/settings.yml`
   * `scripts/k8s/base/aeron-echo-java/settings.yml`

3. Make sure your test environment is the active `kubecontext`

4. If you are attempting to run DPDK tests, make sure you have a DPDK enabled Pod/Host. Setting this up is outside the scope of this documentation, please see https://github.com/AdaptiveConsulting/k8s-dpdk-mgr for an example of how to do this.

5. Ensure you are permissioned to write to a K8s namespace, by default the tooling will use the `default` namespace.

6. Run:
   ```
   ./scripts/k8s-remote-testing.sh (-t aeron-echo-java | aeron-echo-dpdk ) ( -n my_namespace )
   ```

## Other benchmarks (single machine)
Set of latency benchmarks testing round trip time (RTT) between threads or processes (IPC) via FIFO data structures and messaging systems.

### Java Benchmarks

To run the Java benchmarks execute the Gradle script in the base directory.

    $ ./gradlew runJavaIpcBenchmarks

or just the Aeron benchmarks

    $ ./gradlew runAeronJavaIpcBenchmarks

### C++ Benchmarks

To generate the benchmarks, execute the `cppbuild` script from the base directory.

    $ cppbuild/cppbuild

To run the benchmarks, execute the individual benchmarks.

    $ cppbuild/Release/binaries/baseline
    $ cppbuild/Release/binaries/aeronExclusiveIpcBenchmark
    $ cppbuild/Release/binaries/aeronIpcBenchmark
    $ cppbuild/Release/binaries/aeronExclusiveIpcNanomark
    $ cppbuild/Release/binaries/aeronIpcNanomark

**Note**: On MacOS, it will be necessary to set `DYLD_LIBRARY_PATH` for the Aeron
driver shared library. For example:

    $ env DYLD_LIBRARY_PATH=cppbuild/Release/aeron-prefix/src/aeron-build/lib cppbuild/Release/binaries/aeronIpcBenchmark

The binaries with __Benchmark__ in the name use Google Benchmark and only displays average times.

While the binaries with __Nanomark__ in the name use Nanomark (included in the source) and displays full histograms.

To pick a specific tag for Aeron, specify `--aeron-git-tag` parameter when invoking `cppbuild` script.
For example:
```bash
cppbuild/cppbuild --aeron-git-tag="1.42.0"
```
will use Aeron `1.42.0` release.

License (See LICENSE file for full license)
-------------------------------------------
Copyright 2015-2025 Real Logic Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
