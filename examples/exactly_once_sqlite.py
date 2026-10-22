"""Exactly-once SQLite effects using idempotent event IDs.

Kafka offsets are committed after the handler. SQLite is not part of Kafka's
distributed transaction, so this example makes the database effect idempotent:
the event ID is a primary key, and recording it plus incrementing the run count
happen in one SQLite transaction. A crash after that transaction but before the
Kafka offset commit replays the event without incrementing twice.

Run from the repository root with a local Kafka broker. Create the input and
Faust leader topics first when TOPIC_ALLOW_DECLARE=false:

    .venv-1/Scripts/python.exe -m pip install -e .
    docker compose -f docker-compose.semantics.yml up -d
    docker compose -f docker-compose.semantics.yml exec redpanda rpk topic create sqlite-semantics-input --partitions 1
    docker compose -f docker-compose.semantics.yml exec redpanda rpk topic create faust-sqlite-exactly-once-__assignor-__leader --partitions 1
    $env:TOPIC_ALLOW_DECLARE = "false"
    $env:SEMANTICS_SQLITE_DB = "$env:TEMP/faust-semantics.sqlite3"
    $env:SEMANTICS_SQLITE_PAUSE_MARKER = "$PWD/.sqlite-pause"
    Remove-Item .sqlite-pause -ErrorAction SilentlyContinue
    .venv-1/Scripts/python.exe -m examples.exactly_once_sqlite worker -l info

In another window, produce a fresh run, force-stop the worker when the pause
marker appears, then restart it with the same environment:

    .venv-1/Scripts/python.exe -m examples.exactly_once_sqlite produce --count 100 --run-id sqlite-trial
    .venv-1/Scripts/python.exe -m examples.exactly_once_sqlite verify --count 100 --run-id sqlite-trial

The SQLite verifier must report 100 stored IDs. This verifies exactly-once
database effects through deduplication, not an atomic distributed SQLite/Kafka
transaction. Use unique, stable event IDs and a durable SQLite file.
"""

import argparse
import asyncio
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import faust
from aiokafka import AIOKafkaProducer


BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "sqlite-semantics-input"
DEFAULT_DB = Path.home() / ".faust" / "semantics.sqlite3"
DB_PATH = Path(os.environ.get("SEMANTICS_SQLITE_DB", str(DEFAULT_DB)))


class ValidationEvent(faust.Record, serializer="json"):
    id: str
    run_id: str


app = faust.App(
    "faust-sqlite-exactly-once",
    broker=f"kafka://{BROKER}",
    processing_guarantee="exactly_once",
    broker_commit_interval=0.5,
    topic_partitions=1,
)
input_topic = app.topic(INPUT_TOPIC, key_type=str, value_type=ValidationEvent)


class SqliteLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=30) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS processed_events ("
                "event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS run_counts ("
                "run_id TEXT PRIMARY KEY, event_count INTEGER NOT NULL)"
            )

    def record_once(self, event: ValidationEvent) -> None:
        with sqlite3.connect(self.path, timeout=30) as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO processed_events (event_id, run_id) "
                "VALUES (?, ?)",
                (event.id, event.run_id),
            )
            if cursor.rowcount:
                connection.execute(
                    "INSERT INTO run_counts (run_id, event_count) VALUES (?, 1) "
                    "ON CONFLICT(run_id) DO UPDATE SET "
                    "event_count = event_count + 1",
                    (event.run_id,),
                )

    def count(self, run_id: str) -> int:
        with sqlite3.connect(self.path, timeout=30) as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM processed_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return int(row[0])


ledger = SqliteLedger(DB_PATH)


async def pause_after_database_commit() -> None:
    marker_value = os.environ.get("SEMANTICS_SQLITE_PAUSE_MARKER")
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
    async for event in events:
        ledger.record_once(event)
        await pause_after_database_commit()


async def produce(count: int, run_id: str) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    try:
        for index in range(count):
            event_id = f"{run_id}-{index:06d}"
            payload = ValidationEvent(id=event_id, run_id=run_id)
            await producer.send_and_wait(
                INPUT_TOPIC,
                key=event_id.encode("utf-8"),
                value=json.dumps(payload.to_representation()).encode("utf-8"),
            )
    finally:
        await producer.stop()
    print(f"Produced {count} SQLite trial events for run {run_id!r}.")


def verify(count: int, run_id: str) -> bool:
    actual = ledger.count(run_id)
    passed = actual == count
    print(f"run={run_id} expected={count} sqlite_records={actual}")
    print("PASS" if passed else "FAIL")
    return passed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exactly-once SQLite effects with a Kafka-backed Faust worker."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument("--count", type=int, default=100)
    produce_parser.add_argument("--run-id", default="sqlite-trial")
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--count", type=int, default=100)
    verify_parser.add_argument("--run-id", default="sqlite-trial")
    subparsers.add_parser("worker")
    args = parser.parse_known_args()[0]

    if args.command == "produce":
        asyncio.run(produce(args.count, args.run_id))
    elif args.command == "verify":
        raise SystemExit(0 if verify(args.count, args.run_id) else 1)
    else:
        app.main()


if __name__ == "__main__":
    main()