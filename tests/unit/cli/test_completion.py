from unittest.mock import patch

import pytest

from faust.cli.completion import completion


class Test_completion:
    @pytest.fixture()
    def command(self, *, context):
        return completion(context)

    @pytest.mark.asyncio
    async def test_run(self, *, command):
        with patch("faust.cli.completion.click_completion") as cc:
            await command.run()
            cc.get_code.assert_called_once_with(shell=command.shell())

    @pytest.mark.asyncio
    async def test_run__no_completion(self, *, command):
        with patch("faust.cli.completion.click_completion", None):
            with pytest.raises(command.UsageError):
                await command.run()
