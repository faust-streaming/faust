from uuid import uuid4

from fastapi import APIRouter
from my_faust.app import faust_app
from my_faust.topic.my_topic import greetings_topic

router = APIRouter()


async def produce_greeting() -> None:
    await greetings_topic.send(value=uuid4().hex)


@router.get("/produce")
async def produce():
    await produce_greeting()
    return {"success": True}


# Keep the timer separate from the route.  Stacking ``@faust_app.timer`` on top
# of ``@router.get`` registers the undecorated function as the HTTP route and
# the timer-wrapped one as the timer -- two different callables, which is
# rarely what people mean.
@faust_app.timer(5)
async def produce_periodically() -> None:
    await produce_greeting()
