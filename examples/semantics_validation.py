"""Crash-window validation for exactly-once and manual at-most-once behavior.

Install Faust and start the broker from the repository root:

    .venv-1/Scripts/python.exe -m pip install -e .
    docker compose -f docker-compose.semantics.yml up -d

The commands below pre-create the application topics because auto-declare is
disabled for compatibility with recent aiokafka clients. Reset the broker
between mode trials so each consumer group starts at input offset zero.

For exactly-once, create the input, output, and leader topics, then start the
worker with a marker path shared across restarts:

    docker compose -f docker-compose.semantics.yml exec redpanda rpk topic create semantics-validation-input --partitions 1
    docker compose -f docker-compose.semantics.yml exec redpanda rpk topic create semantics-validation-output --partitions 1
    docker compose -f docker-compose.semantics.yml exec redpanda rpk topic create faust-semantics-exactly-once-__assignor-__leader --partitions 1
    $env:TOPIC_ALLOW_DECLARE = "false"
    $env:SEMANTICS_PAUSE_MARKER = "$PWD/.semantics-pause"
    Remove-Item .semantics-pause -ErrorAction SilentlyContinue
    .venv-1/Scripts/python.exe -m examples.exactly_once worker -l info

In another window, produce a batch:

    .venv-1/Scripts/python.exe -m examples.exactly_once produce --count 100 --run-id exactly-once-trial

When the marker appears, force-stop the worker process. Restart it with the
same marker path; it replays the open transaction and commits the batch. Verify:

    .venv-1/Scripts/python.exe -m examples.exactly_once verify --count 100 --run-id exactly-once-trial

Exactly-once passes with 100 unique outputs and no duplicates.

Repeat from a clean broker for at-most-once, using the
`faust-semantics-at-most-once-__assignor-__leader` topic and
`examples.at_most_once` commands. This mode acknowledges before the paused
side effect; it passes with at least one missing output and no duplicates.
At-most-once is a manual pattern here, not a Faust `processing_guarantee`.
"""

import argparse
import asyncio
from contextlib import suppress
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set

import faust
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition


BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "semantics-validation-input"
OUTPUT_TOPIC = "semantics-validation-output"
MODE = os.environ.get("SEMANTICS_MODE", "exactly-once")
GUARANTEE = "exactly_once" if MODE == "exactly-once" else "at_least_once"


class ValidationEvent(faust.Record, serializer="json"):
    id: str


app = faust.App(
    f"faust-semantics-{MODE}",
    broker=f"kafka://{BROKER}",
    processing_guarantee=GUARANTEE,
    broker_commit_interval=0.5,
    topic_partitions=1,
)
input_topic = app.topic(INPUT_TOPIC, key_type=str, value_type=ValidationEvent)
output_topic = app.topic(OUTPUT_TOPIC, key_type=str, value_type=ValidationEvent)


async def pause_once() -> None:
    marker_value = os.environ.get("SEMANTICS_PAUSE_MARKER")
    if not marker_value:
        return
    marker = Path(marker_value)
    if marker.exists():
        return
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    release = marker.with_name(marker.name + ".continue")
    while not release.exists():
        await asyncio.sleep(0.1)


@app.agent(input_topic)
async def process_events(events: Any) -> None:
    if MODE == "at-most-once":
        manual_ack_stream = events.noack()
        async for payload in manual_ack_stream:
            event = manual_ack_stream.current_event
            if event is not None:
                await manual_ack_stream.ack(event)
            await pause_once()
            await output_topic.send(key=payload.id, value=payload)
    else:
        async for payload in events:
            await output_topic.send(key=payload.id, value=payload)
            await pause_once()


@app.agent(output_topic)
async def drain_output(records: Any) -> None:
    async for output_record in records:
        pass


async def produce(count: int, run_id: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    try:
        for index in range(count):
            event_id = f"{run_id}-{index:06d}"
            await producer.send_and_wait(
                INPUT_TOPIC,
                key=event_id.encode("utf-8"),
                value=json.dumps({"id": event_id}).encode("utf-8"),
            )
    finally:
        await producer.stop()
    print(f"Produced {count} events for run {run_id!r}.")


async def collect_output(timeout: float) -> List[Dict[str, Any]]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BROKER,
        group_id=None,
        enable_auto_commit=False,
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        topic_partitions = [TopicPartition(OUTPUT_TOPIC, 0)]
        consumer.assign(topic_partitions)
        await consumer.seek_to_beginning(*topic_partitions)
        messages: List[Dict[str, Any]] = []
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            records = await consumer.getmany(timeout_ms=500)
            for batch in records.values():
                messages.extend(json.loads(record.value) for record in batch)
        return messages
    finally:
        with suppress(asyncio.CancelledError):
            await consumer.stop()


def verify(messages: List[Dict[str, Any]], count: int, run_id: str) -> bool:
    expected: Set[str] = {f"{run_id}-{index:06d}" for index in range(count)}
    actual = [message.get("id") for message in messages]
    actual_for_run = [event_id for event_id in actual if event_id in expected]
    unique = set(actual_for_run)
    duplicates = len(actual_for_run) - len(unique)
    missing = len(expected - unique)

    print(
        f"mode={MODE} expected={count} observed={len(actual_for_run)} "
        f"unique={len(unique)} duplicates={duplicates} missing={missing}"
    )
    if MODE == "exactly-once":
        passed = unique == expected and duplicates == 0
    elif MODE == "at-most-once":
        passed = duplicates == 0 and missing > 0
    elif MODE == "at-least-once":
        passed = unique == expected
    else:
        raise ValueError(f"Unsupported SEMANTICS_MODE: {MODE}")
    print("PASS" if passed else "FAIL")
    return passed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka-backed crash-window checks for Faust processing semantics."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument("--count", type=int, default=100)
    produce_parser.add_argument("--run-id", default="trial")
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--count", type=int, default=100)
    verify_parser.add_argument("--run-id", default="trial")
    verify_parser.add_argument("--timeout", type=float, default=5.0)
    subparsers.add_parser("worker")
    args = parser.parse_known_args()[0]

    if args.command == "produce":
        asyncio.run(produce(args.count, args.run_id))
    elif args.command == "verify":
        passed = verify(
            asyncio.run(collect_output(args.timeout)), args.count, args.run_id
        )
        raise SystemExit(0 if passed else 1)
    else:
        app.main()


if __name__ == "__main__":
    main()
