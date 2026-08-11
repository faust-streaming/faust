"""agent_stopper for Faust when app fails"""

import logging
import traceback

from faust.types import AppT

log = logging.getLogger(__name__)


async def agent_stopper(app: AppT) -> None:
    """
    Raise exception and crash app
    """
    log.error("%s", traceback.format_exc())
    log.warning("Closing application")

    # force the exit code of the application not to be 0
    # and prevent offsets from progressing
    # ``Service._crash`` is annotated as taking an exception instance, but the
    # reason is only stored and later re-raised, and ``raise`` accepts an
    # exception class just as well.  Passing the class (not an instance) is
    # deliberate here and is pinned by tests/unit/utils/test_agent_stopper.py.
    app._crash(RuntimeError)  # type: ignore[arg-type]
