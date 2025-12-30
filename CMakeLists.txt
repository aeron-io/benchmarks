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
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
cmake_policy(VERSION 3.16)

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed. Please make a new directory (called a build directory)"
        "and run CMake from there. You may need to remove CMakeCache.txt.")
endif ()

include(ExternalProject)

project("benchmarks")

# default build type is Release
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build" FORCE)
endif (NOT CMAKE_BUILD_TYPE)

set(AERON_THIRDPARTY_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/thirdparty")

##########################################################
# Google benchmark

ExternalProject_Add(
    google_benchmark
    URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/benchmark-1.8.2.zip
    URL_MD5 dbdc6b760ea4aad9c1ace0de813701e2
    CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER};-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER};
    -DBENCHMARK_ENABLE_TESTING=OFF;-DBENCHMARK_ENABLE_GTEST_TESTS=OFF;-DBENCHMARK_ENABLE_ASSEMBLY_TESTS=OFF;
    -DCMAKE_BUILD_TYPE=Release
    PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/google_benchmark"
    BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/google_benchmark/src/google_benchmark-build/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(google_benchmark source_dir)
set(GOOGLE_BENCHMARK_SOURCE_DIR ${source_dir})
ExternalProject_Get_Property(google_benchmark binary_dir)
set(GOOGLE_BENCHMARK_BINARY_DIR ${binary_dir})

set(
    GOOGLE_BENCHMARK_LIBS
    ${GOOGLE_BENCHMARK_BINARY_DIR}/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}
)

##########################################################
# HDR Histogram

ExternalProject_Add(
    hdr_histogram
    URL ${CMAKE_CURRENT_SOURCE_DIR}/cppbuild/HdrHistogram_c-0.11.8.zip
    URL_MD5 e993a4a08ad084ea0a895e7b2502de99
    CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER};-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER};-DCMAKE_C_STANDARD=99;-DHDR_LOG_REQUIRED=OFF
    PREFIX "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram"
    BUILD_BYPRODUCTS "${AERON_THIRDPARTY_BINARY_DIR}/hdr_histogram/src/hdr_histogram-build/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}"
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(hdr_histogram source_dir)
set(HDRHISTOGRAM_SOURCE_DIR ${source_dir})
ExternalProject_Get_Property(hdr_histogram binary_dir)
set(HDRHISTOGRAM_BINARY_DIR ${binary_dir})

set(
    HDRHISTOGRAM_LIBS
    ${HDRHISTOGRAM_BINARY_DIR}/src/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}hdr_histogram_static${CMAKE_STATIC_LIBRARY_SUFFIX}
)

##########################################################
# Aeron from github or local git

if (NOT AERON_GIT_URL)
    set(AERON_GIT_URL "https://github.com/aeron-io/aeron.git" CACHE STRING "Location of the Aeron git repo" FORCE)
endif (NOT AERON_GIT_URL)

if (NOT AERON_GIT_SHA)
    if (NOT AERON_GIT_TAG)
        set(AERON_GIT_TAG "origin/HEAD" CACHE STRING "Tag in the Aeron git repo" FORCE)
    endif (NOT AERON_GIT_TAG)

    ExternalProject_Add(
        aeron
        GIT_REPOSITORY ${AERON_GIT_URL}
        GIT_TAG ${AERON_GIT_TAG}
        GIT_SHALLOW TRUE
        CMAKE_ARGS "-DAERON_TESTS=OFF" "-DAERON_SYSTEM_TESTS=OFF" "-DAERON_SLOW_SYSTEM_TESTS=OFF" "-DCMAKE_BUILD_TYPE=Release"
        INSTALL_COMMAND ""
    )
else ()
    ExternalProject_Add(
        aeron
        GIT_REPOSITORY ${AERON_GIT_URL}
        GIT_TAG ${AERON_GIT_SHA}
        CMAKE_ARGS "-DAERON_TESTS=OFF" "-DAERON_SYSTEM_TESTS=OFF" "-DAERON_SLOW_SYSTEM_TESTS=OFF" "-DCMAKE_BUILD_TYPE=Release"
        INSTALL_COMMAND ""
    )
endif ()

ExternalProject_Get_Property(aeron source_dir)
set(AERON_SOURCE_DIR ${source_dir})
ExternalProject_Get_Property(aeron binary_dir)
set(AERON_BINARY_DIR ${binary_dir})

set(
    AERON_LIBS
    ${AERON_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}aeron_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    ${AERON_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}aeron_driver${CMAKE_SHARED_LIBRARY_SUFFIX}
)

##########################################################
# Platform flags, etc.

find_package(Threads)

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_CXX_EXTENSIONS)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

if (NOT DEFINED CMAKE_C_STANDARD)
    set(CMAKE_C_STANDARD 11)
endif ()

if (NOT DEFINED CMAKE_C_EXTENSIONS)
    set(CMAKE_C_EXTENSIONS OFF)
endif ()

# all UNIX-based platform compiler flags
if (UNIX)
    add_compile_options(-fstrict-aliasing -Wall -Wpedantic -Wextra -Wno-unused-parameter -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)

    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0")
        add_compile_options(-Wno-error=maybe-uninitialized)
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "21.0")
        # See gtest: https://github.com/google/googletest/issues/4762
        add_compile_options(-Wno-error=character-conversion)
    endif ()

    if (C_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:C>:-Werror>)
    endif (C_WARNINGS_AS_ERRORS)

    if (CXX_WARNINGS_AS_ERRORS)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Werror>)
    endif (CXX_WARNINGS_AS_ERRORS)
endif ()

# platform specific flags
if (APPLE)
    add_compile_options(-Wsign-compare)
    add_definitions(-DDarwin)
    add_compile_options(-Wno-deprecated-register)
endif ()

##########################################################
# Project variables, etc.

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/binaries")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")

# google benchmark
include_directories(${GOOGLE_BENCHMARK_SOURCE_DIR}/include)
# Hdr Histogram
include_directories(${HDRHISTOGRAM_SOURCE_DIR}/include/hdr)
include_directories(${AERON_SOURCE_DIR}/aeron-client/src/main/cpp_wrapper)
include_directories(${AERON_SOURCE_DIR}/aeron-client/src/main/c)
include_directories(${AERON_SOURCE_DIR}/aeron-driver/src/main/c)

include_directories(${CMAKE_SOURCE_DIR}/benchmarks-aeron/src/main/cpp)

##########################################################

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/benchmarks-aeron/src/main/cpp")
