#!/usr/bin/env python
import asyncio
from contextlib import asynccontextmanager
from typing import Union

from fastapi import FastAPI

import faust


# This is just hello_world.py integrated with a FastAPI application


def fake_answer_to_everything_ml_model(x: float):
    return x * 42


ml_models = {}


# You MUST have "app" defined in order for Faust to discover the app
# if you're using "faust" on CLI, but this doesn't work yet
faust_app = faust.App(
    'hello-world-fastapi',
    broker='kafka://localhost:9092',
    web_enabled=False,
)
# app = faust_app

greetings_topic = faust_app.topic('greetings', value_type=str)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    ml_models["answer_to_everything"] = fake_answer_to_everything_ml_model
    await faust_app.start()
    yield
    # Clean up the ML models and release the resources
    ml_models.clear()
    await faust_app.stop()


app = fastapi_app = FastAPI(
    lifespan=lifespan,
)
# For now, run via "uvicorn fastapi_example:app"
# then visit http://127.0.0.1:8000/docs


@fastapi_app.get("/")
def read_root():
    return {"Hello": "World"}


@fastapi_app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@faust_app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)


@faust_app.timer(5)  # make sure you *always* add the timer above if you're using one
@fastapi_app.get("/produce")
async def produce():
    for i in range(100):
        await greetings_topic.send(value=f'hello {i}')
    return {"success": True}
