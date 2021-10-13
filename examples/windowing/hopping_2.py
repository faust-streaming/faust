#!/usr/bin/env python

# In this exapmple we have a function `publish_every_2secs` publishing a
# message every 2 seconds to topic `hopping_topic`.
# We have created an agent `print_windowed_events` consuming events from
# `hopping_topic` that mutates the windowed table `hopping_table`.

# `hopping_table` is a table with hopping (overlaping) windows. Each of
# its windows is 10 seconds of duration, and we create a new window every 5
# seconds.
# |----------|
#       |-----------|
#             |-----------|
#                   |-----------|
# Since we produce an event every 2 seconds and our windows are 10
# seconds of duration we expect different the following results per method
# called in `WindowWrapper`:
# - now(): Gets the closest value to current local time. It will always be
# between 1 and 3.
# - current(): Gets the value relative to the event's timestamp. It will
# always be between 1 and 3.
# - value(): Gets the value relative to default relative option. It will
# always be between 1 and 3.
# - delta(30): Gets the value of window 30 secs before the current event. For
# the first 20 seconds it will be 0 and after second 30 it will always be 5.

import sys
from datetime import datetime, timedelta
from time import time
import faust


class RawModel(faust.Record):
    date: datetime
    value: float


class WinModel(faust.Record):
    win: list


TOPIC = 'raw-event'
TABLE = 'hopping_table'
KAFKA = 'kafka://localhost:9092'
CLEANUP_INTERVAL = 10
WINDOW = 6
STEP = 3
WINDOW_EXPIRES = 60
PARTITIONS = 1

app = faust.App('windowed-hopping', broker=KAFKA, topic_partitions=PARTITIONS)

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, value_type=RawModel)


def window_processor(key, events):
    timestamp = key[1][0]
    count = len(events)

    print(
        f'processing window:'
        f'{count} events,'
        f'timestamp {timestamp}',
    )


hopping_table = (
    app.Table(
        TABLE,
        default=list,
        partitions=PARTITIONS,
        on_window_close=window_processor,
    )
    .hopping(WINDOW, STEP, expires=timedelta(seconds=WINDOW_EXPIRES))
    .relative_to_field(RawModel.date)
)


@app.agent(source)
async def print_windowed_events(stream):
    async for event in stream:
        value_list = hopping_table['events'].value()

        if len(value_list) > 0:
            event.value = value_list[-1].value + 1
        print("Receive message : " + str(event))

        value_list.append(event)
        hopping_table['events'] = value_list


@app.timer(0.1)
async def produce():
    value = 1
    await source.send(value=RawModel(value=value, date=int(time())))
    # print(f'Produce Message :: send messge {value}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
