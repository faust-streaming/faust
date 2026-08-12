"""Serve the API from inside the Faust worker, on the worker's event loop.

This is the mirror image of ``main.py``: instead of uvicorn starting Faust from
an ASGI lifespan, ``faust worker`` starts uvicorn as one of its services::

    $ faust -A worker_main worker -l info

The API is then on http://127.0.0.1:8000 and the worker is a single process.
``serve_asgi`` registers the server as an app service, so it starts only after
table recovery finishes -- which is when it is actually safe to serve traffic.

Requires ``pip install "faust-streaming[fastapi]"``.
"""

from api import router as api_router
from fastapi import FastAPI
from my_faust.app import faust_app
from my_faust.timer import router as timer_router

from faust.contrib.asgi import serve_asgi

# ``faust -A worker_main`` looks for an attribute named ``app``.
app = faust_app

api = FastAPI()
api.include_router(router=api_router)
api.include_router(router=timer_router)


@api.get("/")
def read_root():
    return {"Hello": "World"}


serve_asgi(faust_app, api, port=8000)
