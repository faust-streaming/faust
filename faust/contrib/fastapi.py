"""Compatibility imports for the renamed ASGI integration.

The integration does not depend on FastAPI specifically.  New code should
import from :mod:`faust.contrib.asgi`; this module remains as an alias for the
older, framework-specific import path.
"""

from .asgi import *  # noqa: F401,F403
from .asgi import __all__  # noqa: F401
