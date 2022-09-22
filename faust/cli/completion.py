"""completion - Command line utility for completion.

Supports ``bash``, ``ksh``, ``zsh``, etc.
"""
import os
from pathlib import Path

from .base import AppCommand

try:
    import click_completion
except ImportError:  # pragma: no cover
    click_completion = None  # noqa
else:  # pragma: no cover
    click_completion.init()


class completion(AppCommand):
    """Output shell completion to be evaluated by the shell."""

    require_app = False

    async def run(self) -> None:
        """Dump click completion script for Faust CLI."""
        if click_completion is None:
            raise self.UsageError(
                "Missing required dependency, but this is easy to fix.\n"
                "Run `pip install click_completion` from your virtualenv\n"
                "and try again!"
            )
        self.say(click_completion.get_code(shell=self.shell()))  # nosec: B604

    def shell(self) -> str:
        """Return the current shell used in this environment."""
        shell_path = Path(os.environ.get("SHELL", "auto"))
        return shell_path.stem
