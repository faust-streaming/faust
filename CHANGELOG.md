# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
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
