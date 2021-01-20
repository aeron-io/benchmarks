::
:: Copyright 2015-2021 Real Logic Limited.
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
:: https://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "DIR=%~dp0"

"%JAVA_HOME%\bin\java" ^
  -cp "%DIR%\..\benchmarks-all\build\libs\benchmarks.jar" ^
  -XX:+UnlockExperimentalVMOptions ^
  -XX:+TrustFinalNonStaticFields ^
  -XX:+UnlockDiagnosticVMOptions ^
  -XX:GuaranteedSafepointInterval=300000 ^
  -XX:+UseBiasedLocking ^
  -XX:BiasedLockingStartupDelay=0 ^
  -XX:+UseParallelOldGC ^
  -Xms4G ^
  -Xmx4G ^
  -XX:+AlwaysPreTouch ^
  -XX:MaxMetaspaceSize=1G ^
  -XX:ReservedCodeCacheSize=1G ^
  -XX:+PerfDisableSharedMem ^
  %JVM_OPTS% ^
  %*
