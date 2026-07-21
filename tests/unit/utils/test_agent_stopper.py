from unittest.mock import Mock

import pytest

from faust.utils.agent_stopper import agent_stopper


@pytest.mark.asyncio
async def test_agent_stopper_crashes_app():
    app = Mock(name="app")
    await agent_stopper(app)
    # It must force a non-zero exit by crashing the app with a RuntimeError.
    app._crash.assert_called_once_with(RuntimeError)
