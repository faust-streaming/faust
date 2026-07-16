from uuid import uuid4
from fastapi import APIRouter

from my_faust.app import faust_app
from my_faust.topic.my_topic import greetings_topic

router = APIRouter()


@faust_app.timer(5)  # make sure you *always* add the timer above if you're using one
@router.get("/produce")
async def produce():
    await greetings_topic.send(value=uuid4().hex)
    return {"success": True}
