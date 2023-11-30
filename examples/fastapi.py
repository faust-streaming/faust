#!/usr/bin/env python

# This is just hello_world.py integrated with a FastAPI application

import mode.loop.uvloop  # noqa

import faust

from contextlib import asynccontextmanager

from fastapi import FastAPI


def fake_answer_to_everything_ml_model(x: float):
    return x * 42


ml_models = {}


# You MUST have "app" defined
app = faust_app = faust.App(
    'hello-world-fastapi',
    broker='kafka://localhost:9092',
)

greetings_topic = faust_app.topic('greetings', value_type=str)


@faust_app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(greeting)


@faust_app.timer(5)
async def produce():
    for i in range(100):
        await greetings_topic.send(value=f'hello {i}')


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    ml_models["answer_to_everything"] = fake_answer_to_everything_ml_model
    await faust_app.start()
    yield
    # Clean up the ML models and release the resources
    ml_models.clear()
    await faust_app.stop()


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.get("/predict")
async def predict(x: float):
    result = ml_models["answer_to_everything"](x)
    return {"result": result}

if __name__ == '__main__':
    faust_app.main()
