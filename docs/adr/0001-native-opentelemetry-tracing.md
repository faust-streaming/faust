# ADR 0001: Migrate built-in distributed tracing to native OpenTelemetry

- Status: **Proposed**
- Date: 2026-07-18
- Supersedes: the OpenTracing shim bridge (`faust[opentelemetry]` extra, PR #688) — that
  remains a valid *interim* for existing deployments, not the destination.

## Context

Faust ships a built-in distributed-tracing layer that emits spans for the
processing pipeline — agents, streams, table recovery, the assignor, timers,
Crontabs, and the Kafka rebalance sequence. It is wired through the public
`app.tracer` extension point (`faust.types.app.TracerT`) and is currently
coupled to the **OpenTracing** API:

- `opentracing>=1.3.0,<=2.4.0` is a **hard core dependency**
  (`requirements/requirements.txt`).
- Spans are OpenTracing `Span` objects threaded through ~134 call sites across
  12 modules.
- Current-span propagation uses Python `contextvars`
  (`faust/utils/tracing.py`).
- Cross-service context crosses the Kafka boundary via OpenTracing
  `inject`/`extract` (`Format.TEXT_MAP`) into message headers
  (`faust/sensors/distributed_tracing.py`).

OpenTracing is **archived / EOL**, and the OpenTelemetry project formally
**deprecated OpenTracing compatibility on 2026-03-19** (spec PR #4938). The
`opentelemetry-opentracing-shim` package still ships (0.65b0, 2026-07-16) but
its earliest removal is no sooner than **March 2027**. Building the long-term
story on the shim is building on a sunset path.

### Why not delegate to an instrumentor

There is **no `opentelemetry-instrumentation-faust`** package — not on PyPI,
not in `opentelemetry-python-contrib`, and nothing community-maintained.
(`opentelemetry-instrumentation-faststream` targets the unrelated *FastStream*
library.) Transport-level instrumentors do exist —
`opentelemetry-instrumentation-aiokafka` and
`opentelemetry-instrumentation-confluent-kafka` (both 0.65b0, beta) — but they
patch the *raw* Kafka clients and produce only produce/consume spans. They know
nothing about Faust's agent / stream / recovery / rebalance boundaries, which is
precisely the value Faust's built-in tracing adds. They **complement** Faust's
spans; they cannot replace them.

## Decision

Rewrite the built-in tracing layer to emit **native OpenTelemetry spans**,
depending only on `opentelemetry-api`, while **keeping the full span tree**.

Key design points:

1. **API-only dependency.** As a library, Faust depends on `opentelemetry-api`
   (an optional extra), never the SDK. Calls are cheap no-ops until the *app*
   registers a `TracerProvider`. This replaces the hard `opentracing` core
   dependency.

2. **`app.tracer` becomes optional.** Native OTel apps configure a global
   `TracerProvider`; Faust resolves its tracer via
   `opentelemetry.trace.get_tracer("faust", __version__)`. Users no longer need
   to implement `TracerT` at all. `app.tracer` is retained as an explicit
   override/legacy seam.

3. **Contextvars map 1:1.** `opentelemetry.context` is contextvars-based by
   default, so Faust's existing current-span propagation — including across
   `await` boundaries in agents/streams — carries over without new plumbing.
   Extracted remote context is made current with `context.attach`/`detach`.

4. **W3C propagation across Kafka headers.** Header inject/extract moves from
   OpenTracing to `opentelemetry.propagate` (W3C `traceparent`). This is a
   wire-format change, and an improvement: it is the standard format and lets
   Faust spans nest correctly under the transport instrumentors when a user
   enables them.

5. **Semantic-convention coexistence** (see below).

6. **Breaking change, phased with a compatibility adapter** (see below).

### Semantic-convention coexistence

Faust adopts the OpenTelemetry `messaging.*` semantic conventions **additively**,
without breaking existing dashboards:

- **Span names stay legacy by default** (`consume-from-{topic}`, `job-{topic}`,
  `produce-to-{topic}`, …). A one-time deprecation notice is logged, stating the
  legacy names will become opt-in in a future major and semconv names
  (`{topic} process`, `{topic} publish`, …) will become the default.
- **Attributes carry both.** Every span emits its legacy tags
  (`kafka-topic`, `kafka-partition`, …) *and* the semconv attributes
  (`messaging.system=kafka`, `messaging.destination.name`,
  `messaging.operation.type`, `messaging.destination.partition.id`, …), plus the
  appropriate `SpanKind` (`CONSUMER`/`PRODUCER`).
- **Opt-in name flip.** A setting (`app.conf`, e.g. `tracing_span_names`,
  default `"legacy"`) lets early adopters switch span *names* to semconv now.
  Attributes coexist in both modes.

This satisfies "preserve current names but indicate they're deprecated, and
adopt semconv in a coexisting way."

### Re-expressing the two non-portable hacks

Both were prototyped against the real SDK before this ADR.

1. **Deterministic rebalance trace_id.** Today
   `span.context.trace_id = murmur2(f"reb-{app_id}-{generation}")` mutates the
   span context — impossible in OTel (contexts are immutable). Native
   re-expression: mint a `NonRecordingSpan` parent whose `SpanContext` carries
   the murmur2-derived trace_id, put it in context via `set_span_in_context`,
   and start the real rebalance span as its child; the child inherits the
   deterministic trace_id. Cross-generation relationships that previously relied
   on trace_id rewriting use **span links**.

2. **Lazy/monkeypatched `span.finish`.** Today `_transform_span_lazy` builds a
   dynamic `LazySpan` subclass overriding `.finish`. OTel spans are not
   subclassable that way. Native re-expression: **defer span creation** — buffer
   a lightweight pending descriptor and start+end the real span when
   `on_generation_id_known` fires (or cancel it in `flush_spans`).

## Migration phases

- **Phase 0 — interim (done):** `opentracing` optional (#686) + shim bridge
  (#688). Non-breaking.
- **Phase 1a — foundation (this change):** native OTel tracing core
  (`faust/utils/otel_tracing.py`) with tracer resolution, deterministic
  rebalance context, semconv coexistence helpers, W3C header propagation, and
  the deferred-finish helper — fully unit-tested, **no call sites rewired yet**.
  Adds the `faust[opentelemetry]` extra (`opentelemetry-api`).
- **Phase 1b — rewire:** convert the 12 modules to the native core; redefine
  `TracerT` in OTel terms; ship a legacy adapter that wraps a user's existing
  OpenTracing `app.tracer` through the shim, kept behind `faust[opentracing]`
  for one major cycle. Target a **major version bump**.
- **Phase 2 — remove:** drop `opentracing` from core requirements and delete the
  legacy adapter in the following major.

## Consequences

- **Positive:** no archived/EOL dependency in core; standard W3C wire format;
  interoperates with the transport instrumentors; cleaner code (no context
  mutation, no monkeypatch); `app.tracer` becomes optional; SDK choice deferred
  to the app as OTel intends.
- **Negative / risk:** `app.tracer`/`TracerT` signature change is breaking —
  mitigated by the phased adapter and a major-version bump. Span *names* remain
  legacy by default to protect existing dashboards; the eventual default flip to
  semconv is a separately announced deprecation. The rebalance link/`NonRecordingSpan`
  approach must be validated against real backends (Jaeger/Tempo) during 1b.
