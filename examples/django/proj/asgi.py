"""ASGI entry point that runs Django and Faust on the same event loop."""

import os

from django.core.asgi import get_asgi_application

from faust.contrib.asgi import FaustLifespanMiddleware

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "proj.settings")

django_application = get_asgi_application()

# Django exposes an ASGI callable but does not provide a constructor-level
# lifespan hook.  This middleware owns only the lifespan scope; HTTP and
# WebSocket traffic passes through to Django unchanged.
from faustapp.app import app as faust_app  # noqa: E402

application = FaustLifespanMiddleware(django_application, faust_app)
