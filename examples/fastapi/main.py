from contextlib import asynccontextmanager
from fastapi import FastAPI
from api import router as api_router

from my_faust.timer import router as timer_router
from my_faust.app import faust_app


# This is just hello_world.py integrated with a FastAPI application


def fake_answer_to_everything_ml_model(x: float):
    return x * 42


ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    faust_app.discover()
    await faust_app.start()
    yield
    await faust_app.stop()


# You MUST have "app" defined in order for Faust to discover the app
# if you're using "faust" on CLI, but this doesn't work yet
app = fastapi_app = FastAPI(
    lifespan=lifespan,
)

# For now, run via "uvicorn fastapi_example:app"
# then visit http://127.0.0.1:8000/docs

app.include_router(router=api_router)
app.include_router(router=timer_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
