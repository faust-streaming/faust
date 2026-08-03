"""hello_world.py as a package-structured FastAPI + Faust application.

Serve it with uvicorn -- Faust starts and stops from the ASGI lifespan::

    $ uvicorn main:api --reload

    # ...then visit http://127.0.0.1:8000/docs

The Faust worker is a separate entry point, since the app lives in its own
module and is found by autodiscovery::

    $ faust -A my_faust.app worker -l info

See ``worker_main.py`` for the other direction: one ``faust worker`` process
serving this same API on its own event loop.

Requires ``pip install "faust-streaming[fastapi]"``.
"""

from api import router as api_router
from fastapi import FastAPI
from my_faust.app import faust_app
from my_faust.timer import router as timer_router

from faust.contrib.fastapi import faust_lifespan

# ``faust_lifespan`` binds the app to uvicorn's event loop, runs autodiscovery
# (the app sets ``autodiscover``), starts it, and stops it on shutdown.
api = FastAPI(lifespan=faust_lifespan(faust_app))

api.include_router(router=api_router)
api.include_router(router=timer_router)


@api.get("/")
def read_root():
    return {"Hello": "World"}
