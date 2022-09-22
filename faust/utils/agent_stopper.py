"""agent_stopper for Faust when app fails"""
import logging
import traceback

log = logging.getLogger(__name__)


async def agent_stopper(app) -> None:
    """
    Raise exception and crash app
    """
    log.error("%s", traceback.format_exc())
    log.warning("Closing application")

    # force the exit code of the application not to be 0
    # and prevent offsets from progressing
    app._crash(RuntimeError)
