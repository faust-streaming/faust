---
name: verify
description: Verify faust changes by driving the public API (faust.App, topic.send, app.consumer.start) without a Kafka broker.
---

# Verifying faust changes

faust is a library; the surface is `import faust` in a script, not `faust/...` internals.

## Setup (cloud container, Python 3.11)

```bash
pip install -e .
# tests/conftest.py needs pytest<8; opentracing needs stdlib distutils on Debian:
SETUPTOOLS_USE_DISTUTILS=stdlib pip install opentracing
pip install "click>=6.7,<8.4" prometheus_client
```

## Driving without a broker

aiokafka validates config in its constructors, so producer/consumer setup errors surface
before any network I/O. A config that passes validation fails later with
`KafkaConnectionError: Unable to bootstrap from [...]`, which is the "good" outcome
when no broker is running.

```python
import asyncio, faust
app = faust.App("x", broker="kafka://localhost:9098",
                broker_credentials=faust.SASLCredentials(username="u", password="p", mechanism="PLAIN"))
async def main():
    await asyncio.wait_for(app.topic("t").send(value=b"v"), timeout=10)   # producer path
    await asyncio.wait_for(app.consumer.start(), timeout=10)              # consumer path
asyncio.run(main())
```

Compare against master with `git checkout -q origin/master` and the same script.

## Gotchas

- Lowering `broker_request_timeout` below `broker_session_timeout` (60s) makes consumer
  start fail with an unrelated `ImproperlyConfigured` before auth is reached. Keep defaults
  when probing the consumer path.
