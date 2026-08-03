#!/usr/bin/env python
"""hello_world.py, co-hosted with a FastAPI application.

Faust and the web server share one process and one event loop, so an endpoint
can produce to a topic directly::

    $ uvicorn fastapi_example:api --reload

    # ...then visit http://127.0.0.1:8000/docs

The same file also works as a plain worker, because ``app`` is the Faust app::

    $ faust -A fastapi_example worker -l info

Requires ``pip install "faust-streaming[fastapi]"``.
"""

from contextlib import asynccontextmanager
from typing import Union

from fastapi import FastAPI

import faust
from faust.contrib.fastapi import faust_app_running


def fake_answer_to_everything_ml_model(x: float):
    return x * 42


ml_models = {}

# ``web_enabled=False`` turns off Faust's own aiohttp server, since uvicorn is
# already serving.  Leave it on if you also want ``@app.page`` views -- they
# are served separately, on ``web_port`` (6066 by default).
app = faust_app = faust.App(
    "hello-world-fastapi",
    broker="kafka://localhost:9092",
    web_enabled=False,
)

greetings_topic = faust_app.topic("greetings", value_type=str)


@asynccontextmanager
async def lifespan(api: FastAPI):
    # ``faust_app_running`` binds the app to uvicorn's event loop, starts it,
    # and stops it again on shutdown.  With no setup of your own to do, use
    # ``FastAPI(lifespan=faust_lifespan(faust_app))`` instead.
    async with faust_app_running(faust_app):
        ml_models["answer_to_everything"] = fake_answer_to_everything_ml_model
        yield
        ml_models.clear()


api = FastAPI(lifespan=lifespan)


@api.get("/")
def read_root():
    return {"Hello": "World"}


@api.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@faust_app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)


async def produce_greetings(count: int = 100) -> None:
    for i in range(count):
        await greetings_topic.send(value=f"hello {i}")


@api.get("/produce")
async def produce():
    await produce_greetings()
    return {"success": True}


# Register the timer separately rather than stacking it on the route.  Stacking
# registers the undecorated function as the HTTP route and the timer-wrapped
# one as the timer, which is rarely what people mean.
@faust_app.timer(5)
async def produce_periodically() -> None:
    await produce_greetings()
