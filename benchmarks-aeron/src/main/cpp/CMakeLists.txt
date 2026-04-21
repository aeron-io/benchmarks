#
# Copyright 2015-2025 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set(
    HEADERS
    EmbeddedAeronMediaDriver.h
    NanoMark.h
)

check_include_file("bsd/stdlib.h" BSDSTDLIB_H_EXISTS)
find_library(LIBBSD_EXISTS NAMES bsd libbsd)

if (LIBBSD_EXISTS)
    set(AERON_LIB_BSD_LIBS bsd)
endif ()

function(benchmark name file)
    add_executable(${name} ${HEADERS} ${file})
    target_link_libraries(${name} ${AERON_LIBS} ${CMAKE_DL_LIBS} ${GOOGLE_BENCHMARK_LIBS} ${HDRHISTOGRAM_LIBS} ${CMAKE_THREAD_LIBS_INIT} ${AERON_LIB_BSD_LIBS})
    add_dependencies(${name} aeron google_benchmark hdr_histogram)
endfunction()

benchmark(baseline Baseline.cpp)
benchmark(timeBenchmark TimeBenchmark.cpp)
benchmark(aeronIpcBenchmark AeronIpcBenchmark.cpp)
benchmark(aeronExclusiveIpcBenchmark AeronExclusiveIpcBenchmark.cpp)
benchmark(nanoMarkTest NanoMarkTest.cpp)
benchmark(aeronExclusiveIpcNanomark AeronExclusiveIpcNanomark.cpp)
benchmark(aeronIpcNanomark AeronIpcNanomark.cpp)
