# faust-streaming 0.12.0

> Set the release date at tag time. This is the first `CHANGELOG.md`-tracked
> release since v0.8.10; v0.9.0–v0.11.3 are documented only in their GitHub
> release notes.
>
> Scope: these notes list only work **already merged to `master`**. Fixes and
> features still in open PRs (the offset-commit data-loss fixes, the optional
> OpenTracing/OpenTelemetry extras, and the reported-issue fix stack) will be
> folded in as they merge.

This release drops end-of-life Python versions, re-introduces the
confluent-kafka driver, refreshes the client/runtime dependencies, and stands up
a live-broker CI harness. Please read the **Breaking changes / upgrade notes**
before upgrading.

## Highlights

- **confluent-kafka driver is back.** Use it with `confluent://` broker URLs
  after installing the `faust[ckafka]` extra. It has been rewritten and tested
  against the released `confluent-kafka` API.
- **Python 3.13 and 3.14 supported; 3.8 and 3.9 dropped.** The supported range
  is now Python 3.10–3.14.
- **Redis backend moved to `redis-py`**, replacing the unmaintained `aredis`.
- **A real Kafka broker now runs in CI**, round-tripping messages through both
  the aiokafka and confluent-kafka client libraries (#706), giving us a
  foundation for broker-dependent regression tests.

## Breaking changes / upgrade notes

- **Python 3.8 and 3.9 are no longer supported** (#650). The minimum is now
  Python 3.10. Pin `faust-streaming<0.12` if you are still on an older
  interpreter.
- **`aredis` has been replaced by `redis-py`** for the Redis store and leader
  (#635). No code change is required for typical `redis://` usage, but custom
  integrations that imported `aredis` internals will need updating.
- **confluent-kafka is an optional extra.** `confluent://` broker URLs require
  `faust-streaming[ckafka]` (`confluent-kafka >= 2.0.0`).

## Added

- Re-add the confluent-kafka transport driver (`confluent://`,
  `faust[ckafka]`), rewritten against the released `confluent-kafka` API.
- Support Python 3.13 and 3.14 (with and without Cython).
- `on_clear` handlers on `(Global)Table` and `ChangeloggedSet` (#645).
- Live-broker integration test harness in CI (#706).

## Changed

- Dropped support for Python 3.8 and 3.9 (#650).
- Replaced `aredis` with `redis-py` (#635).
- Support recent `aiokafka` releases: handle 0.13.0 removing `api_version`
  (#674), and guard aiokafka metadata/admin calls by the negotiated protocol-API
  version while keeping the Faust assignor for changelog tables (#682).
- Modernize CI and wheel building — wheel runners, Windows wheels, cibuildwheel,
  and the test matrix (#690).
- Pin `black`/`isort` and update the `click` constraint for reproducible CI
  (#677, #680).

## Fixed

- Fix release-artifact upload/download for `upload-artifact@v4` (#684).

## Dependencies

- Runs on the maintained mode fork **`mode-streaming >= 0.4.0`**.
- **`aiokafka >= 0.10.0`**, now compatible with recent aiokafka releases
  (0.13.x / 0.14.x) — see #674 and #682.
- Adds **`confluent-kafka >= 2.0.0`**, required by the optional `faust[ckafka]`
  transport driver.
- The `faust[cchardet]` extra now uses the maintained **`faust-cchardet`** fork
  in place of the unmaintained `cchardet`.

**Full changelog:** https://github.com/faust-streaming/faust/compare/v0.11.3...v0.12.0
