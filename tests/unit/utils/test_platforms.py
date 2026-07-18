import builtins
import resource
from unittest.mock import patch

from faust.utils.platforms import max_open_files


@patch("resource.getrlimit", return_value=(1024, 4096))
def test_max_open_files__returns_hard_limit(getrlimit):
    assert max_open_files() == 4096


@patch("platform.system", return_value="Linux")
@patch("resource.getrlimit", return_value=(1024, resource.RLIM_INFINITY))
def test_max_open_files__infinity_non_darwin(getrlimit, system):
    assert max_open_files() is None


@patch("subprocess.check_output", return_value=b"kern.maxfilesperproc: 24576")
@patch("platform.system", return_value="Darwin")
@patch("resource.getrlimit", return_value=(1024, resource.RLIM_INFINITY))
def test_max_open_files__infinity_darwin_uses_sysctl(getrlimit, system, check_output):
    assert max_open_files() == 24576


def test_max_open_files__no_resource_module():
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "resource":
            raise ImportError("no resource module")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        assert max_open_files() is None
