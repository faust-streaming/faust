"""Django Fixups - Integration with Django."""
import os
import typing
import warnings
from typing import Iterable

from mode.utils.imports import symbol_by_name
from mode.utils.objects import cached_property

from . import base

if typing.TYPE_CHECKING:  # pragma: no cover
    from django.apps.registry import Apps as _Apps
    from django.settings import Settings as _Settings
else:

    class _Apps:
        ...  # noqa

    class _Settings:
        ...  # noqa


__all__ = ["Fixup"]

WARN_NOT_INSTALLED = """\
Environment variable DJANGO_SETTINGS_MODULE is defined
but Django isn't installed.  Won't apply Django fix-ups!
"""

WARN_DEBUG_ENABLED = """\
Using settings.DEBUG leads to a memory leak, never
use this setting in production environments!
"""


class Fixup(base.Fixup):
    """Django fixup.

    This fixup is enabled if

    1) the :envvar:`DJANGO_SETTINGS_MODULE` environment variable is set,
    2) the :pypi:`django` package is installed.

    Once enabled it will modify the following features:

    - Autodiscovery

        If ``faust.App(autodiscovery=True)``, the Django fixup
        will automatically autodiscover agents/tasks/web views,
        and so on found in installed Django apps.

    - Setup

        The Django machinery will be set up when Faust commands
        are executed.

    """

    def enabled(self) -> bool:
        """Return :const:`True` if Django is used in this environment."""
        if os.environ.get("DJANGO_SETTINGS_MODULE"):
            try:
                import django  # noqa
            except ImportError:
                warnings.warn(WARN_NOT_INSTALLED)
            else:
                return True
        return False

    def wait_for_django(self) -> None:
        import django

        django.setup()
        if self.settings.DEBUG:
            warnings.warn(WARN_DEBUG_ENABLED)

    def autodiscover_modules(self) -> Iterable[str]:
        """Return list of additional autodiscover modules.

        For Django we run autodiscovery in all packages
        listed in the ``INSTALLED_APPS`` setting (with support for
        custom app configurations).
        """
        self.wait_for_django()
        return [config.name for config in self.apps.get_app_configs()]

    def on_worker_init(self) -> None:
        """Initialize Django before worker/CLI command starts."""
        from django.core.checks import run_checks

        self.wait_for_django()
        run_checks()

    @cached_property
    def apps(self) -> _Apps:
        """Return the Django app registry."""
        return symbol_by_name("django.apps:apps")

    @cached_property
    def settings(self) -> _Settings:
        """Return the Django settings object."""
        return symbol_by_name("django.conf:settings")
