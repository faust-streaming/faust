# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v0.8.10](https://github.com/faust-streaming/faust/releases/tag/v0.8.10) - 2022-09-14

[Compare with v0.8.9](https://github.com/faust-streaming/faust/compare/v0.8.9...v0.8.10)

### Changed
- Change `aioeventlet` extension to `faust-aioeventlet`
- Update versioning of `click>=6.7,<8.2`

## [v0.8.9](https://github.com/faust-streaming/faust/releases/tag/v0.8.9) - 2022-09-04

[Compare with v0.8.8](https://github.com/faust-streaming/faust/compare/v0.8.8...v0.8.9)

### Added
- Add improvements to documentation (#351) ([2091326](https://github.com/faust-streaming/faust/commit/20913264c916d26f0ce50ba28f66e7008e38787e) by William Barnhart).
- Added aerospike to stores (#350) ([ba000ee](https://github.com/faust-streaming/faust/commit/ba000ee319d969070d396ad99f24f4f3265abbb9) by William Barnhart).
- Add aerospike docs (#348) ([3680bbd](https://github.com/faust-streaming/faust/commit/3680bbdb6f3c251a270365d023b1229dd0432d7e) by William Barnhart).
- Add noqa: b024 to base classes to pass lint check (#344) ([7648c6e](https://github.com/faust-streaming/faust/commit/7648c6e053dc7e6daff9e7cb86a4baa5cd411965) by William Barnhart).
- Address topics.py error in #175 by callling message.message (#342) ([76f720a](https://github.com/faust-streaming/faust/commit/76f720ad1dfc9366c7a1cd308cf29171888d65ae) by William Barnhart).

### Changed
- Change version to 0.8.9 in preparaion of next release ([6475f8c](https://github.com/faust-streaming/faust/commit/6475f8ca9ee8b713996661f6a0fd88cda05493f3) by William Barnhart).

### Fixed
- Fix running `make develop` and restore `pre-commit` tool (#145) ([85534ec](https://github.com/faust-streaming/faust/commit/85534ec2031f06ba0bdebabb77688476ffe9f806) by Taybin Rutkin).


## [v0.8.8](https://github.com/faust-streaming/faust/releases/tag/v0.8.8) - 2022-08-15

[Compare with v0.8.7](https://github.com/faust-streaming/faust/compare/v0.8.7...v0.8.8)

### Added
- Adding intervaltree to manage gaps in topics to prevent oom (#282) ([1e7be3a](https://github.com/faust-streaming/faust/commit/1e7be3a1dba51e13e23dd804bfda9d630cc729bd) by Vikram Patki).


## [v0.8.7](https://github.com/faust-streaming/faust/releases/tag/v0.8.7) - 2022-08-11

[Compare with v0.8.6](https://github.com/faust-streaming/faust/compare/v0.8.6...v0.8.7)

### Added
- Add dist.yml for uploading builds to pypi (#338) ([79d672c](https://github.com/faust-streaming/faust/commit/79d672cda7785803af43c9f2c0c28483788b7dd5) by William Barnhart).

### Fixed
- Fix recovery of partitions for large volume writes (#335) ([5a2ba13](https://github.com/faust-streaming/faust/commit/5a2ba13ddc45bcfdf7948d6f77daaba34dcaef97) by William Barnhart). Related issues/PRs: [#333](https://github.com/faust-streaming/faust/issues/333)

### Removed
- Remove a broken link in readme.md (#332) ([9eb770a](https://github.com/faust-streaming/faust/commit/9eb770a028bc63396eb33b97e3ee3c692723701b) by Sefa Degirmenci).


## [v0.8.6](https://github.com/faust-streaming/faust/releases/tag/v0.8.6) - 2022-07-19

[Compare with v0.8.5](https://github.com/faust-streaming/faust/compare/v0.8.5...v0.8.6)

### Added
- Add method to rocksdb for backing up partitions (#304) ([0bb2685](https://github.com/faust-streaming/faust/commit/0bb2685e545d28c4e8a604cc748c12e0911c1260) by William Barnhart).

### Fixed
- Fixed filter not acking filtered out messages. (#208) ([a887571](https://github.com/faust-streaming/faust/commit/a887571e143f875c3ab4df488964e2ebde6dc5d2) by Matthew Drago).
- Fix twisted link in readme.md (#309) ([49574b2](https://github.com/faust-streaming/faust/commit/49574b264a7c6dfdff03e6fb5f58a5033bc0c5b4) by William Barnhart).
- Fix flake warning with bound loop var (#326) ([1e9d4a5](https://github.com/faust-streaming/faust/commit/1e9d4a5bfc768cce0ed0766fe709c47de019288b) by William Barnhart).


## [v0.8.5](https://github.com/faust-streaming/faust/releases/tag/v0.8.5) - 2022-06-02

[Compare with v0.8.4](https://github.com/faust-streaming/faust/compare/v0.8.4...v0.8.5)

### Added
- Add support for additional sentry tags and arguments (#285) ([c230076](https://github.com/faust-streaming/faust/commit/c230076202e48cab273b656f01f938d3e12db54f) by Ran K).
- Added connections_max_idle_ms for consumer and producer (#281) ([84302f5](https://github.com/faust-streaming/faust/commit/84302f57e8a232ca81c2689805b9ee8b63bcc202) by Magnus Zotterman).
- Added metadata_max_age_ms option for both consumer and producer (#279) ([3b7f079](https://github.com/faust-streaming/faust/commit/3b7f079996977f852eaef96daaf94f9cb7d7e366) by Roman).

### Fixed
- Fix readme typos (#308) ([26ff8fc](https://github.com/faust-streaming/faust/commit/26ff8fc6d98a9154b5fe9b4daad5610314b0144b) by William Barnhart).
- Fix user guide create channel example to run (#299) ([f52b783](https://github.com/faust-streaming/faust/commit/f52b783b4f6308b947666bcb1bd9f79aa19e9c15) by Robbie Palmer).
- Fix (#287) ([ed026a0](https://github.com/faust-streaming/faust/commit/ed026a0e1865a7f927fdd16cdf30cd38241f3b1b) by ameen-orca).
- Fix: rewrite non enabled unit tests (#272) ([d15d349](https://github.com/faust-streaming/faust/commit/d15d349c2f53c48e7f5dfa6ae24d43309005c21f) by Christoph Brand).
- Fix aiohttp threadedproducer driver python36, add unit tests (#277) ([784bee7](https://github.com/faust-streaming/faust/commit/784bee7f843fd830c3724c2ed55a6eda26fef785) by Christoph Brand).


## [v0.8.4](https://github.com/faust-streaming/faust/releases/tag/v0.8.4) - 2022-02-25

[Compare with v0.8.2](https://github.com/faust-streaming/faust/compare/v0.8.2...v0.8.4)

### Fixed
- Fix: producer send pending check with threads (#270) ([4e32327](https://github.com/faust-streaming/faust/commit/4e32327ed784ab0d242a095eb2f45f760ecee3a1) by Christoph Brand).
- Fix name_prefix usage (#266) ([ea8be8c](https://github.com/faust-streaming/faust/commit/ea8be8c8ffeaa1f9f69a700e4f4fb67db5a5317a) by Julien Surloppe).


## [v0.8.2](https://github.com/faust-streaming/faust/releases/tag/v0.8.2) - 2022-02-04

[Compare with v0.8.1](https://github.com/faust-streaming/faust/compare/v0.8.1...v0.8.2)

### Fixed
- Fix errors in seek when flow is not active (#264) ([a313821](https://github.com/faust-streaming/faust/commit/a31382119c57da006b2ff073094358119f0d7f3d) by Vikram Patki).


## [v0.8.1](https://github.com/faust-streaming/faust/releases/tag/v0.8.1) - 2022-01-19

[Compare with v0.8.0](https://github.com/faust-streaming/faust/compare/v0.8.0...v0.8.1)


## [v0.8.0](https://github.com/faust-streaming/faust/releases/tag/v0.8.0) - 2022-01-11

[Compare with v0.7.9](https://github.com/faust-streaming/faust/compare/v0.7.9...v0.8.0)


## [v0.7.9](https://github.com/faust-streaming/faust/releases/tag/v0.7.9) - 2022-01-07

[Compare with v0.7.8](https://github.com/faust-streaming/faust/compare/v0.7.8...v0.7.9)

### Fixed
- Fix condition to delete old table keys (#251) ([a0e9a31](https://github.com/faust-streaming/faust/commit/a0e9a31c8f831b99a40ed5245219d053831571e3) by Dima Kovalchuk).


## [v0.7.8](https://github.com/faust-streaming/faust/releases/tag/v0.7.8) - 2021-12-18

[Compare with v0.7.7](https://github.com/faust-streaming/faust/compare/v0.7.7...v0.7.8)


## [v0.7.7](https://github.com/faust-streaming/faust/releases/tag/v0.7.7) - 2021-12-17

[Compare with v0.7.6](https://github.com/faust-streaming/faust/compare/v0.7.6...v0.7.7)

### Fixed
- Fix race conditions on rebalance (#241) ([6f3c783](https://github.com/faust-streaming/faust/commit/6f3c783022b612d171de33003968fa056f48b325) by Vikram Patki).


## [v0.7.6](https://github.com/faust-streaming/faust/releases/tag/v0.7.6) - 2021-12-15

[Compare with v0.7.5](https://github.com/faust-streaming/faust/compare/v0.7.5...v0.7.6)

### Removed
- Remove wait_first and extra log (#240) ([6e5c301](https://github.com/faust-streaming/faust/commit/6e5c301e08ff0a8b9fc693ba9cc88d11e5951cd2) by ekerstens).


## [v0.7.5](https://github.com/faust-streaming/faust/releases/tag/v0.7.5) - 2021-12-14

[Compare with v0.7.4](https://github.com/faust-streaming/faust/compare/v0.7.4...v0.7.5)


## [v0.7.4](https://github.com/faust-streaming/faust/releases/tag/v0.7.4) - 2021-12-13

[Compare with v0.7.3](https://github.com/faust-streaming/faust/compare/v0.7.3...v0.7.4)

### Fixed
- Fix race condition when buffers are full (#237) ([7d861dc](https://github.com/faust-streaming/faust/commit/7d861dc9616a26d0105b102eb70c793c69af2759) by Vikram Patki). Related issues/PRs: [#166](https://github.com/faust-streaming/faust/issues/166)


## [v0.7.3](https://github.com/faust-streaming/faust/releases/tag/v0.7.3) - 2021-12-10

[Compare with v0.7.2](https://github.com/faust-streaming/faust/compare/v0.7.2...v0.7.3)


## [v0.7.2](https://github.com/faust-streaming/faust/releases/tag/v0.7.2) - 2021-12-10

[Compare with v0.7.1](https://github.com/faust-streaming/faust/compare/v0.7.1...v0.7.2)


## [v0.7.1](https://github.com/faust-streaming/faust/releases/tag/v0.7.1) - 2021-12-10

[Compare with v0.7.0](https://github.com/faust-streaming/faust/compare/v0.7.0...v0.7.1)


## [v0.7.0](https://github.com/faust-streaming/faust/releases/tag/v0.7.0) - 2021-12-03

[Compare with v0.6.14](https://github.com/faust-streaming/faust/compare/v0.6.14...v0.7.0)


## [v0.6.14](https://github.com/faust-streaming/faust/releases/tag/v0.6.14) - 2021-11-30

[Compare with v0.6.13](https://github.com/faust-streaming/faust/compare/v0.6.13...v0.6.14)

### Fixed
- Fix for out of order events (#228) ([7e3c60c](https://github.com/faust-streaming/faust/commit/7e3c60cad80b79fd177ca1e1448d1b33385a1f10) by Vikram Patki).
- Fix flake8 (#227) ([3caf810](https://github.com/faust-streaming/faust/commit/3caf8104948827b110a94e50f9535df7b32737a2) by ekerstens).
- Fixed the hello world example ([02afc3c](https://github.com/faust-streaming/faust/commit/02afc3c0ac529da87a14ffe70033d53708afbad6) by Luís Braga).


## [v0.6.13](https://github.com/faust-streaming/faust/releases/tag/v0.6.13) - 2021-11-29

[Compare with v0.6.12](https://github.com/faust-streaming/faust/compare/v0.6.12...v0.6.13)


## [v0.6.12](https://github.com/faust-streaming/faust/releases/tag/v0.6.12) - 2021-11-18

[Compare with v0.6.11](https://github.com/faust-streaming/faust/compare/v0.6.11...v0.6.12)

### Added
- Add default to external_topic_distribution (#222) ([23c2d90](https://github.com/faust-streaming/faust/commit/23c2d902380419ec8804cd05b3f9e8c47279e5d0) by Stegallo).


## [v0.6.11](https://github.com/faust-streaming/faust/releases/tag/v0.6.11) - 2021-11-17

[Compare with v0.6.10](https://github.com/faust-streaming/faust/compare/v0.6.10...v0.6.11)

### Added
- Add all partitions of global tables as active in order to let them recover in the beginning (#213) ([60a8696](https://github.com/faust-streaming/faust/commit/60a869654d1e13914d36f3db0c66fef28911c7ce) by aoberegg).

### Fixed
- Fix recovery._resume_streams (#217) ([e3bd128](https://github.com/faust-streaming/faust/commit/e3bd128e5631657c58d301943754b9921d3cbe95) by ekerstens).
- Fix worker default web_host to be none (#210) ([41200e4](https://github.com/faust-streaming/faust/commit/41200e44cb80000e5a2f09f2a94de18168239dde) by Ran K).

### Removed
- Remove outdated colorclass library (#204) ([9385c81](https://github.com/faust-streaming/faust/commit/9385c8166256bc85c5c4b985fab6b749d070ba68) by Taybin Rutkin).


## [v0.6.10](https://github.com/faust-streaming/faust/releases/tag/v0.6.10) - 2021-10-21

[Compare with v0.6.9](https://github.com/faust-streaming/faust/compare/v0.6.9...v0.6.10)

### Added
- Adding new feature to retry on aerospike runtime exceptions issue#202 (#203) ([868d7a4](https://github.com/faust-streaming/faust/commit/868d7a40c04450924237914591ea26dc88eb00ba) by Vikram Patki).

### Fixed
- Fix canonical url when setting either port of host from the cli (#196) ([808312b](https://github.com/faust-streaming/faust/commit/808312b4093c9e99a4b28e848574240290de5d12) by Ran K).
- Fixes #197 (#198) ([3959268](https://github.com/faust-streaming/faust/commit/395926869a372d81ec6564fc4d117e57a159e0d6) by David Parker).
- Fix routing method #188 (#191) ([835f37e](https://github.com/faust-streaming/faust/commit/835f37e7d451d08ee8d3bfad86d8361b785d2333) by Ondřej Chmelař).
- Fix for reassign table keys faust-streaming#171 (#174) ([c1a6b0e](https://github.com/faust-streaming/faust/commit/c1a6b0e216f62fe01f898f7dca7054bb7622161f) by Philipp Jaschke).
- Fix for #121 (#168) ([fe343c0](https://github.com/faust-streaming/faust/commit/fe343c01a7640d2d33ae7514de0c172bba759a4d) by Taybin Rutkin).
- Fix markdown formatting in changelog ([727987b](https://github.com/faust-streaming/faust/commit/727987b76ccfbaa61809af971e0fcb66e16e76cf) by Taybin Rutkin).
- Fix lint warning ([e80829c](https://github.com/faust-streaming/faust/commit/e80829c645bbd543c3c4769a272a852148386544) by Taybin Rutkin).


## [v0.6.9](https://github.com/faust-streaming/faust/releases/tag/v0.6.9) - 2021-07-06

[Compare with v0.6.8](https://github.com/faust-streaming/faust/compare/v0.6.8...v0.6.9)

### Fixed
- Fix error messages in faust app #166 ([b06e579](https://github.com/faust-streaming/faust/commit/b06e5799653d75021aa8a409c3d3e6f83a266f1f) by Vikram Patki 24489).


## [v0.6.8](https://github.com/faust-streaming/faust/releases/tag/v0.6.8) - 2021-06-29

[Compare with v0.6.7](https://github.com/faust-streaming/faust/compare/v0.6.7...v0.6.8)

### Fixed
- Fix for consumer errors in app #166 (#167) ([761713b](https://github.com/faust-streaming/faust/commit/761713b475f1ca4df45b1a0c3d833e4d18735184) by Vikram Patki).
- Fixed a few small mistakes in streams userguide (#161) ([c45a464](https://github.com/faust-streaming/faust/commit/c45a46429340a4edb199af01637ce19fdf16a873) by Dongkuo Ma).


## [v0.6.7](https://github.com/faust-streaming/faust/releases/tag/v0.6.7) - 2021-06-07

[Compare with v0.6.6](https://github.com/faust-streaming/faust/compare/v0.6.6...v0.6.7)


## [v0.6.6](https://github.com/faust-streaming/faust/releases/tag/v0.6.6) - 2021-06-03

[Compare with v0.6.5](https://github.com/faust-streaming/faust/compare/v0.6.5...v0.6.6)

### Fixed
- Fix string formatting error when logging slow processing (#156) ([e71145c](https://github.com/faust-streaming/faust/commit/e71145c2053c4a9bf0b9f74eec0f5180f8f6b877) by Erik Forsberg). Related issues/PRs: [#153](https://github.com/faust-streaming/faust/issues/153)
- Fix record instances deserialize properly when returned by agent.ask (#152) ([df0856f](https://github.com/faust-streaming/faust/commit/df0856fbc9e3d4c667c05cc49ae4883fc4076db6) by tarbaig).


## [v0.6.5](https://github.com/faust-streaming/faust/releases/tag/v0.6.5) - 2021-05-14

[Compare with v0.6.4](https://github.com/faust-streaming/faust/compare/v0.6.4...v0.6.5)

### Added
- Adding replication factor to the leader topic (#150) ([7ab0647](https://github.com/faust-streaming/faust/commit/7ab0647b66e7c45ef255810e2cd5648cd631759e) by Vikram Patki).

### Fixed
- Fix tests directory name in makefile (#134) ([1a5c431](https://github.com/faust-streaming/faust/commit/1a5c4314ab5394f3a5777c874c85c73c82f06854) by Taybin Rutkin).


## [v0.6.4](https://github.com/faust-streaming/faust/releases/tag/v0.6.4) - 2021-04-26

[Compare with v0.6.3](https://github.com/faust-streaming/faust/compare/v0.6.3...v0.6.4)

### Fixed
- Fix rocksdb for use with global tables or tables that use_partitioner (#130) ([ec3ac3e](https://github.com/faust-streaming/faust/commit/ec3ac3ec5946be54134703bab53fe87a16abaa53) by aoberegg).


## [v0.6.3](https://github.com/faust-streaming/faust/releases/tag/v0.6.3) - 2021-04-06

[Compare with v0.6.2](https://github.com/faust-streaming/faust/compare/v0.6.2...v0.6.3)

### Added
- Adding changelog ([72c59b7](https://github.com/faust-streaming/faust/commit/72c59b78553f5ee85e84f07c6962a7deaac1a22f) by Vikram Patki 24489).

### Fixed
- Fix for https://github.com/faust-streaming/faust/issues/126 (#127) ([159ad62](https://github.com/faust-streaming/faust/commit/159ad62978e84fb5f7d5a47afcb45446123a0255) by Vikram Patki).


## [v0.6.2](https://github.com/faust-streaming/faust/releases/tag/v0.6.2) - 2021-03-12

[Compare with v0.6.1](https://github.com/faust-streaming/faust/compare/v0.6.1...v0.6.2)

### Added
- Add app_name to prometheus metrics (#120) ([e0010f7](https://github.com/faust-streaming/faust/commit/e0010f72b0b0084c24c328962184d455bf2a02c8) by Alexey Kuzyashin).


## [v0.6.1](https://github.com/faust-streaming/faust/releases/tag/v0.6.1) - 2021-02-28

[Compare with v0.6.0](https://github.com/faust-streaming/faust/compare/v0.6.0...v0.6.1)

### Fixed
- Fix scan options (#117) ([750e3ad](https://github.com/faust-streaming/faust/commit/750e3ad2303bf6fa21fb4e1020b8f15dec3101a2) by Vikram Patki).
- Fix iterating over keys when changelog topic is set. (#106) ([7d29cad](https://github.com/faust-streaming/faust/commit/7d29cada3471fb665a1ae0093d4a0fa4f71c9e5d) by aoberegg).
- Fix race (out of order) in flushing topics (wrong state is stored in changelog) (#112) ([a841a0e](https://github.com/faust-streaming/faust/commit/a841a0e0ca8e8ab0c4884ffe9423ac26df8e1df0) by trauter).
- Fix agents with multiple topics (#116) ([70e5516](https://github.com/faust-streaming/faust/commit/70e5516d48b7d44c0cb389c14e5639781069d613) by Stevan Milic).


## [v0.6.0](https://github.com/faust-streaming/faust/releases/tag/v0.6.0) - 2021-02-25

[Compare with v0.5.2](https://github.com/faust-streaming/faust/compare/v0.5.2...v0.6.0)

### Fixed
- Fix table rebalance issue (#110) ([cd136ad](https://github.com/faust-streaming/faust/commit/cd136ad791546c3b0e1fdac44a27cb6cd2caea21) by Bob Haddleton).


## [v0.5.2](https://github.com/faust-streaming/faust/releases/tag/v0.5.2) - 2021-02-19

[Compare with v0.5.1](https://github.com/faust-streaming/faust/compare/v0.5.1...v0.5.2)

### Fixed
- Fix extra parameter (#101) (#109) ([d3f28bd](https://github.com/faust-streaming/faust/commit/d3f28bdfa657131877e8c8a7ad59c73397ac7797) by Bob Haddleton).


## [v0.5.1](https://github.com/faust-streaming/faust/releases/tag/v0.5.1) - 2021-02-18

[Compare with v0.5.0](https://github.com/faust-streaming/faust/compare/v0.5.0...v0.5.1)


## [v0.5.0](https://github.com/faust-streaming/faust/releases/tag/v0.5.0) - 2021-02-18

[Compare with v0.4.7](https://github.com/faust-streaming/faust/compare/v0.4.7...v0.5.0)


## [v0.4.7](https://github.com/faust-streaming/faust/releases/tag/v0.4.7) - 2021-02-10

[Compare with v0.4.6](https://github.com/faust-streaming/faust/compare/v0.4.6...v0.4.7)

### Added
- Adding changelog for release ([4f7735d](https://github.com/faust-streaming/faust/commit/4f7735ddf8d591eeab5d54a0bbe3d38cd43644c4) by Vikram Patki 24489).

### Fixed
- Fix ipv6 address case in server_list function (#66) ([05c0ff9](https://github.com/faust-streaming/faust/commit/05c0ff987df1ea3e68c84f42a38f6fc5fb55b2f4) by Damien Nadé).
- Fix commit exceptions (#94) (#95) ([5e799c2](https://github.com/faust-streaming/faust/commit/5e799c2e6746745da07e67ae35fa6abf3279bb0c) by Bob Haddleton).


## [v0.4.6](https://github.com/faust-streaming/faust/releases/tag/v0.4.6) - 2021-01-29

[Compare with v0.4.5](https://github.com/faust-streaming/faust/compare/v0.4.5...v0.4.6)

### Fixed
- Fixing race conditions in opening rocksdb (#90) ([f51a850](https://github.com/faust-streaming/faust/commit/f51a8508836ed0aa46e13fe9bb3d64a35d261b42) by Vikram Patki).
- Fix dropped messages when topic backpressure is enabled (#88) (#89) ([5263720](https://github.com/faust-streaming/faust/commit/5263720825a26c2871513b3524226f7eb2cc51be) by Bob Haddleton).


## [v0.4.5](https://github.com/faust-streaming/faust/releases/tag/v0.4.5) - 2021-01-28

[Compare with v0.4.3](https://github.com/faust-streaming/faust/compare/v0.4.3...v0.4.5)

### Added
- Adding performance improvement for getters (#87) ([d4392d5](https://github.com/faust-streaming/faust/commit/d4392d5b84c99d654caf8b2848029948c1709a7d) by Vikram Patki).


## [v0.4.3](https://github.com/faust-streaming/faust/releases/tag/v0.4.3) - 2021-01-26

[Compare with v0.4.2](https://github.com/faust-streaming/faust/compare/v0.4.2...v0.4.3)


## [v0.4.2](https://github.com/faust-streaming/faust/releases/tag/v0.4.2) - 2021-01-21

[Compare with v0.4.1](https://github.com/faust-streaming/faust/compare/v0.4.1...v0.4.2)

### Added
- Adding threaded producer (#68) ([fda7e52](https://github.com/faust-streaming/faust/commit/fda7e5264a4e19321e286d3151a0e0bb56ee5545) by Vikram Patki).


## [v0.4.1](https://github.com/faust-streaming/faust/releases/tag/v0.4.1) - 2021-01-15

[Compare with 0.4.0](https://github.com/faust-streaming/faust/compare/0.4.0...v0.4.1)

### Added
- Add web_ssl_context to allow internal web server to support https (#69) ([fcb6b18](https://github.com/faust-streaming/faust/commit/fcb6b185105239329fd21e4af0f23e5244a2c8eb) by Bob Haddleton).

### Fixed
- Fix for outdated sensors after rebalancing (#72) ([0a4d059](https://github.com/faust-streaming/faust/commit/0a4d059d3234bd2ceccf3845f206d35a706bbefc) by krzysieksulejczak).


## [0.4.0](https://github.com/faust-streaming/faust/releases/tag/0.4.0) - 2020-12-22

[Compare with v0.4.0](https://github.com/faust-streaming/faust/compare/v0.4.0...0.4.0)

### Changed
- Change requirements from mode to mode-streaming (#65) ([e239534](https://github.com/faust-streaming/faust/commit/e23953473e3aca2c56f77963926ceec49aba1e57) by Thomas Sarboni).


## [v0.4.0](https://github.com/faust-streaming/faust/releases/tag/v0.4.0) - 2020-12-18

[Compare with v0.3.1](https://github.com/faust-streaming/faust/compare/v0.3.1...v0.4.0)

### Added
- Add backpressure to slow stream processing to avoid filing up the buffer (#55) ([ccaf0a7](https://github.com/faust-streaming/faust/commit/ccaf0a77d37738873e39235d738172c6f6386035) by Vikram Patki).

### Fixed
- Fix for writebatch when not in recovery for standby writes (#56) ([979ca5a](https://github.com/faust-streaming/faust/commit/979ca5a360d082ff767032daa76b41fcf0c9b092) by Vikram Patki).
- Fix for rebalance exception causing recovery to crash (#57) ([8d6758f](https://github.com/faust-streaming/faust/commit/8d6758f13944bd50ba65cbe65259f47e7816edc1) by Vikram Patki).


## [v0.3.1](https://github.com/faust-streaming/faust/releases/tag/v0.3.1) - 2020-12-07

[Compare with v0.3.0](https://github.com/faust-streaming/faust/compare/v0.3.0...v0.3.1)

### Fixed
- Fix recovery issue in transaction and reprocessing message in consumer (#49) ([be1e6db](https://github.com/faust-streaming/faust/commit/be1e6dbe2507fd068a40391dcd2ddd436372661e) by Vikram Patki).


## [v0.3.0](https://github.com/faust-streaming/faust/releases/tag/v0.3.0) - 2020-11-24

[Compare with v0.2.2](https://github.com/faust-streaming/faust/compare/v0.2.2...v0.3.0)


## [v0.2.2](https://github.com/faust-streaming/faust/releases/tag/v0.2.2) - 2020-11-20

[Compare with v0.2.1](https://github.com/faust-streaming/faust/compare/v0.2.1...v0.2.2)

### Fixed
- Fix for canceled from mode (#42) ([1131106](https://github.com/faust-streaming/faust/commit/1131106b8c954c6df9f5a9d4f7321f3211f65327) by Vikram Patki).
- Fixed ack for tombstones in cython-stream (#39) ([41d3e6b](https://github.com/faust-streaming/faust/commit/41d3e6b97baa5287c336d0997701e396400ab508) by trauter).
- Fix commited offset is always behind the real offset by 1 (#33) ([18230a7](https://github.com/faust-streaming/faust/commit/18230a729a717a05aad2a2df58dfe494917de2ab) by Vikram Patki).
- Fix(aiokafka): release all fetch waiters (#32) ([9fe472f](https://github.com/faust-streaming/faust/commit/9fe472f4d7c4b870cdc76ae5400a4017b7985b34) by Vikram Patki).
- Fix commited offset is always behind the real offset by 1 ([361b09d](https://github.com/faust-streaming/faust/commit/361b09dde7de3c7314d7cac7447e029a8e54c97b) by Maxim Musayev).
- Fix(aiokafka): release all fetch waiters ([8cd7ad4](https://github.com/faust-streaming/faust/commit/8cd7ad4b8ec398dfc3916961cc37064e3f8c3223) by Bob Haddleton).
- Fix: readme updated ([7493b6e](https://github.com/faust-streaming/faust/commit/7493b6e033822b64b80d2c890df5f0a8468a625c) by marcosschroh).


## [v0.2.1](https://github.com/faust-streaming/faust/releases/tag/v0.2.1) - 2020-11-13

[Compare with v0.2.0](https://github.com/faust-streaming/faust/compare/v0.2.0...v0.2.1)

### Fixed
- Fix typo (#26) ([0db67af](https://github.com/faust-streaming/faust/commit/0db67af566335f257c5bf1dbef7f3d352ad8218c) by M).


## [v0.2.0](https://github.com/faust-streaming/faust/releases/tag/v0.2.0) - 2020-11-11

[Compare with v0.1.1](https://github.com/faust-streaming/faust/compare/v0.1.1...v0.2.0)


## [v0.1.1](https://github.com/faust-streaming/faust/releases/tag/v0.1.1) - 2020-11-11

[Compare with v0.1.0rc6](https://github.com/faust-streaming/faust/compare/v0.1.0rc6...v0.1.1)

### Fixed
- Fixed recovery hang ([107142c](https://github.com/faust-streaming/faust/commit/107142cedf2b193590847b52d3326d952e60f34b) by Vikram Patki 24489).
- Fix: web bind to should be 0.0.0.0 instead of localhost ([ea9a322](https://github.com/faust-streaming/faust/commit/ea9a3225e2857b5a76cff75f5e9bb185696fa580) by marcosschroh).


## [v0.1.0rc6](https://github.com/faust-streaming/faust/releases/tag/v0.1.0rc6) - 2020-11-10

[Compare with v0.1.0rc5](https://github.com/faust-streaming/faust/compare/v0.1.0rc5...v0.1.0rc6)

### Fixed
- Fixed recovery hang ([1f3111a](https://github.com/faust-streaming/faust/commit/1f3111a9cc692e4a25ea29d316b55acd95a889b0) by Vikram Patki 24489).


## [v0.1.0rc5](https://github.com/faust-streaming/faust/releases/tag/v0.1.0rc5) - 2020-11-09

[Compare with v0.1.0rc3-2](https://github.com/faust-streaming/faust/compare/v0.1.0rc3-2...v0.1.0rc5)

### Fixed
- Fixed recovery hang ([40ea0b0](https://github.com/faust-streaming/faust/commit/40ea0b0967ad5a4fee2754bacefa8ee8f9d45ee5) by Vikram Patki 24489).


## [v0.1.0rc3-2](https://github.com/faust-streaming/faust/releases/tag/v0.1.0rc3-2) - 2020-11-08

[Compare with v0.1.0](https://github.com/faust-streaming/faust/compare/v0.1.0...v0.1.0rc3-2)

### Fixed
- Fixed recovery hang ([7902e02](https://github.com/faust-streaming/faust/commit/7902e0280971410ff50e5ebc4a4b911872e4bccf) by Vikram Patki 24489).
- Fixed recovery hang and undefined set_close method in aiokafka ([06755bc](https://github.com/faust-streaming/faust/commit/06755bc76c901e67b6f5c02e6a3e90373e445b64) by Vikram Patki 24489).
- Fix: manifest and setup.py fixed. close #20 ([d34b084](https://github.com/faust-streaming/faust/commit/d34b084f8e3e88afe5e15ff84a5182ac40dda3aa) by marcosschroh).
- Fix: codecov added ([2caf669](https://github.com/faust-streaming/faust/commit/2caf669067fe10c05e5a92375f6a501e423a35be) by marcosschroh).
- Fixed typo ([774e36d](https://github.com/faust-streaming/faust/commit/774e36d2a78e7d4e67af054b4c9e70fea81ae7b4) by Vikram Patki 24489).


## [v0.1.0](https://github.com/faust-streaming/faust/releases/tag/v0.1.0) - 2020-11-04

[Compare with first commit](https://github.com/faust-streaming/faust/compare/b4d2fde2f5170aecf56c46c502a95266486bff04...v0.1.0)

### Added

- First release of faust fork
- Replaced robinhood-aiokafka with aiokafka
- Implemented transaction support with aiokafka instead of using the
  the MultiTXNProducer in the robinhood-aiokafka version. Note that this new transaction
  implementation will create a producer for each kafka group/partition pair
