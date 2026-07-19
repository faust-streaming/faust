# faust-streaming 0.12.0

> Set the release date at tag time. This is the first `CHANGELOG.md`-tracked
> release since v0.8.10; v0.9.0–v0.11.3 are documented only in their GitHub
> release notes.

This release drops end-of-life Python versions, makes tracing dependencies
opt-in, re-introduces the confluent-kafka driver, and fixes two data-loss bugs
in offset handling. Please read the **Breaking changes / upgrade notes** before
upgrading.

## Highlights

- **Two data-loss fixes in offset handling.** Faust no longer commits past an
  in-flight (unacked) message (#606, #707), and no longer advances committed
  offsets when the commit itself fails (#316, #692). Both could silently skip
  unprocessed messages after a restart or rebalance.
- **confluent-kafka driver is back.** Use it with `confluent://` broker URLs
  after installing the `faust[ckafka]` extra. It has been rewritten and tested
  against the released `confluent-kafka` API.
- **Python 3.14 supported; 3.8 and 3.9 dropped.** The supported range is now
  Python 3.10–3.14.
- **Tracing is now opt-in.** OpenTracing moved behind `faust[opentracing]`, and
  new OpenTelemetry instrumentation is available via `faust[opentelemetry]`.
- **A real Kafka broker now runs in CI**, round-tripping messages through both
  the aiokafka and confluent-kafka client libraries (#706), giving us a
  foundation for broker-dependent regression tests.

## Breaking changes / upgrade notes

- **Python 3.8 and 3.9 are no longer supported** (#650). The minimum is now
  Python 3.10. Pin `faust-streaming<0.12` if you are still on an older
  interpreter.
- **OpenTracing is no longer installed by default** (#685, #686). If you rely on
  OpenTracing tracing, change your dependency to `faust-streaming[opentracing]`.
- **`aredis` has been replaced by `redis-py`** for the Redis store and leader
  (#635). No code change is required for typical `redis://` usage, but custom
  integrations that imported `aredis` internals will need updating.
- **confluent-kafka is an optional extra.** `confluent://` broker URLs require
  `faust-streaming[ckafka]`.

## Added

- Re-add the confluent-kafka transport driver (`confluent://`,
  `faust[ckafka]`), rewritten against the released `confluent-kafka` API.
- Support Python 3.14 (with and without Cython).
- Optional OpenTelemetry instrumentation via `faust[opentelemetry]`, documented
  in the sensors user guide (#688, #681).
- `on_clear` handlers on `(Global)Table` and `ChangeloggedSet` (#645).
- `web_application_options` setting to configure the web application
  (#551, #704).
- Live-broker integration test harness in CI (#706).

## Changed

- Dropped support for Python 3.8 and 3.9 (#650).
- OpenTracing is now an optional dependency (#685, #686).
- Replaced `aredis` with `redis-py` (#635).
- Support `aiokafka` 0.13.0 (removed `api_version`); guard aiokafka
  metadata/admin calls by protocol-API version and keep the Faust assignor for
  changelog tables (#674, #682).
- Modernize CI and wheel building — wheel runners, Windows wheels, cibuildwheel,
  and the test matrix (#690).
- Pin `black`/`isort` and update the `click` constraint for reproducible CI
  (#677, #680).

## Fixed

- Never commit past an in-flight (unacked) message (#606, #707).
- Don't advance committed-offset bookkeeping when the commit fails (#316, #692).
- Parse ISO-8601 string fields in `relative_to_field()` (#389, #700).
- Record event runtime for events acked via `stream.take()` (#319, #701).
- Don't crash collecting actor tracebacks for finished coroutines (#105, #698).
- Allow `test_context()` for sink-less (non-yielding) agents (#433, #699).
- Don't crash the livelock detector when the consumer isn't started (#446, #702).
- Skip the end-offset metric when the highwater is `None` (#214, #703).
- Preserve the original event timestamp in `Event.forward` (#427, #705).
- Don't crash publishing a message with a non-bytes key (#513, #695).
- Harden reply-topic creation against a hardcoded `replicas=0` (#76, #694).
- Respect an explicit autodiscover module list under Django (#500, #697).
- Fix Prometheus latency histograms mis-scaled by 1000x (#260, #693).
- Add `charset=utf-8` to JSON responses on the orjson path (#557, #696).
- Fix release-artifact upload/download for `upload-artifact@v4` (#684).

**Full changelog:** https://github.com/faust-streaming/faust/compare/v0.11.3...v0.12.0
