import logging
from random import randint
import time
from functools import partial
from typing import AsyncIterator

import faust
from faust import ChannelT, StreamT

app = faust.App("bound_agent")
logger = logging.getLogger(__name__)

class DeviceAction(faust.Record):
    device_id: str

class DeadLetter(faust.Record):
    stage: str
    record: DeviceAction

async def stage1_agent(
    dead_letters: ChannelT,
    stream: StreamT[DeviceAction]
) -> AsyncIterator[DeviceAction]:
    async for action in stream:
        now_ts = int(time.time())
        try:
            if now_ts % 3 == 0:
                raise Exception("!!!")
            
            logger.info(f"[stage1] action arrived: {action}")
            yield action
        except:
            await dead_letters.send(value=DeadLetter(stage="stage1", record=action))

async def stage2_agent(
    dead_letters: ChannelT,
    stream: StreamT[DeviceAction]
) -> AsyncIterator[DeviceAction]:
    async for action in stream:
        now_ts = int(time.time())
        try:
            if now_ts % 3 == 1:
                raise Exception("!!!")

            logger.info(f"[stage2] action arrived: {action}")
            yield action
        except:
            await dead_letters.send(value=DeadLetter(stage="stage2", record=action))

async def deadletter_agent(stream: StreamT[DeviceAction]) -> AsyncIterator[DeviceAction]:
    async for dl in stream:
        logger.error(f"[dead letter] arrived: {dl}")
        yield dl

async def action_generator(device_actions: ChannelT):
    for i in range(0, randint(3, 101)):
        await device_actions.send(value=DeviceAction(device_id=i))
        

def main():
    channel_device_action = app.channel(value_type=DeviceAction)
    channel_stage1_stage2 = app.channel(value_type=DeviceAction)
    channel_deadletter = app.channel(value_type=DeadLetter)

    app.timer(interval=3, on_leader=True)(partial(action_generator, channel_device_action))
    app.agent(channel_deadletter, name="dead-letter-agent")(deadletter_agent)

    app.agent(channel_device_action, name="stage1-agent", sink=[channel_stage1_stage2])(
        partial(stage1_agent, channel_deadletter)
    )
    app.agent(channel_stage1_stage2, name="stage2-agent")(
        partial(stage2_agent, channel_deadletter)
    )

    app.main()


if __name__ == "__main__":
    main()
