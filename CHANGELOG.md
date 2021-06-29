# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

##0.6.8
### Changed
- Fixed [166](https://github.com/faust-streaming/faust/issues/166)

##0.6.7
### Changed
- aiokafka dependency is set to >=0.7.1,<0.8.0

##0.6.6
### Bug Fixes
- Error when logging slow process in aiokafka with python 3.9 [153](https://github.com/faust-streaming/faust/issues/153)
- FIX Record instances deserialize properly when returned by agent.ask[151](https://github.com/faust-streaming/faust/issues/151)

##0.6.5
### Bug Fixes
- Fix leader topic replication [149](https://github.com/faust-streaming/faust/issues/149)

##0.6.4
### Bug Fixes
- Fix partition from message for globaltables or tables that use partitioner [129](https://github.com/faust-streaming/faust/issues/129)
- Calling window close callback after popping the value from store [137](https://github.com/faust-streaming/faust/pull/137)
##0.6.3
### Bug Fixes
- Fix for [126](https://github.com/faust-streaming/faust/issues/126)
##0.6.2
### Bug Fixes
- Add app_name to prometheus sensors[120](https://github.com/faust-streaming/faust/pull/120)
-DatadogMonitor - IndexError: deque index out of range[113](https://github.com/faust-streaming/faust/issues/113)

##0.6.1
### Bug Fixes
- fix agents with multiple topics[116](https://github.com/faust-streaming/faust/pull/116)
-Simplify the code by getting rid of deque_pushpopmax in favour of using maxlen= parameter of deque[115](https://github.com/faust-streaming/faust/pull/115)
-fixes a race condition in writing messages to topics that resulted in a violation of the ordering guarantee (especially changelog topics)[112](https://github.com/faust-streaming/faust/pull/112)
##0.6.0
### Bug Fixes
-Adding support for aerospike [114](https://github.com/faust-streaming/faust/issues/114)

##0.4.7
### Bug Fixes
- Allow multiple workers to share rocksdb data dir[98](https://github.com/faust-streaming/faust/issues/98)
- Fix rebalance and recovery issues [83](https://github.com/faust-streaming/faust/issues/83)
-[92](https://github.com/faust-streaming/faust/issues/92)

##0.4.6
### Bug Fixes
- Fix for [85](https://github.com/faust-streaming/faust/issues/85)
- Fix for [88](https://github.com/faust-streaming/faust/issues/88)
- Fix for [91](https://github.com/faust-streaming/faust/issues/91)
- Enabled Tests [79](https://github.com/faust-streaming/faust/issues/79)
- Fix for [84](https://github.com/faust-streaming/faust/issues/84)

##0.4.5
### Features
- Peformance improvements in rocksdb by ignore bloom filters within an event context

##0.4.3
### Features
- New Sensor Support to monitor ThreadedProducer
[84](https://github.com/faust-streaming/faust/issues/84)
- Support for new rocksdb library faust-streaming-rocksdb
[85](https://github.com/faust-streaming/faust/issues/85)

##0.4.2
### Features
- New threaded producer for applications using send_soon for sending messages

##0.4.1
### Fixed
- Adding Prometheus Sensor
- Stability fixes for table recovery when the stream buffer is full
[75](https://github.com/faust-streaming/faust/pull/75)

##0.4.0
### Fixed
- Prevent stream buffer overflow by lowering the rate of incoming partitions
[53](https://github.com/faust-streaming/faust/issues/53)
-Recovery thread updating standby partition writes in single writes instead of using writeBatch
[51](https://github.com/faust-streaming/faust/issues/51)
- IllegalStateException on seek to offset of a partition that was removed by a rebalance
[54](https://github.com/faust-streaming/faust/issues/54)


## 0.3.1
### Fixed
-Updating opentracing dependency[50](https://github.com/faust-streaming/faust/issues/50)
-New-offset is off between 0.2.0 and 0.3.0, resulting in reprocessing last record (or many records) on worker restart
 [48](https://github.com/faust-streaming/faust/issues/48)
-Worker fails to recover table with exactly_once guarantee [47](https://github.com/faust-streaming/faust/issues/47)

## 0.3.0
### Fixed
Recovery Crash [44](https://github.com/faust-streaming/faust/issues/44)
[37](https://github.com/faust-streaming/faust/issues/37)
## 0.2.2
### Fixed
- Consumer offsets not progressing for certain partitions
- Agent dies silenty when mode cancels pending tasks [678](https://github.com/robinhood/faust/issues/678)

## 0.2.1

### Fixed

- Prometheus rebalance typo [#26](https://github.com/faust-streaming/faust/pull/26)
- Make SCRAM-SHA-256/512 SASL Mechanisms available [#29](https://github.com/faust-streaming/faust/pull/29)

## 0.2.0

### Added

- Hanging of workers on kafka rebalance [#21](https://github.com/faust-streaming/faust/pull/21)

## 0.1.1

### Fixed

- web bind to should be 0.0.0.0 instead of localhost [#24](https://github.com/faust-streaming/faust/pull/24)

## 0.1.0

### Added

- First release of faust fork
- Replaced robinhood-aiokafka with aiokafka
- Implemented transaction support with aiokafka instead of using the
  the MultiTXNProducer in the robinhood-aiokafka version. Note that this new transaction
  implementation will create a producer for each kafka group/partition pair
