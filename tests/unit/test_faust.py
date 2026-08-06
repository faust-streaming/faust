from itertools import takewhile

import pytest

import faust
import faust.exceptions  # noqa: F401
import faust.transport.base  # noqa: F401
import faust.transport.drivers.aiokafka  # noqa: F401
from faust import VersionInfo


@pytest.mark.parametrize(
    "version_string,expected",
    [
        ("0.11.5", VersionInfo(0, 11, 5, None, None)),
        ("v0.11.5", VersionInfo(0, 11, 5, None, None)),
        ("0.11.5.dev1+g1234", VersionInfo(0, 11, 5, "dev1+g1234", None)),
        ("0.11.5rc1", VersionInfo(0, 11, 5, "rc1", None)),
        ("0.11.5+local.1", VersionInfo(0, 11, 5, "+local.1", None)),
        ("1.2.3.4", VersionInfo(1, 2, 3, "4", None)),
        # fewer than three components
        ("1.2", VersionInfo(1, 2, 0, None, None)),
        ("2", VersionInfo(2, 0, 0, None, None)),
        # non-numeric components must not raise
        ("", VersionInfo(0, 0, 0, None, None)),
        ("nonsense", VersionInfo(0, 0, 0, "nonsense", None)),
        ("1.x.3", VersionInfo(1, 0, 0, "x.3", None)),
    ],
)
def test_parse_version(version_string, expected):
    assert faust._parse_version(version_string) == expected


def test_version_info_is_numeric():
    assert faust.VERSION is faust.version_info
    assert isinstance(faust.version_info, VersionInfo)
    assert isinstance(faust.version_info.major, int)
    assert isinstance(faust.version_info.minor, int)
    assert isinstance(faust.version_info.micro, int)
    assert faust.version_info.releaselevel is None or isinstance(
        faust.version_info.releaselevel, str
    )


def test_version_info_matches_version_string():
    leading = faust.__version__.lstrip("v").split("+")[0].split(".")[:3]
    expected = [int(part) for part in takewhile(str.isdigit, leading)]
    actual = [
        faust.version_info.major,
        faust.version_info.minor,
        faust.version_info.micro,
    ]
    assert actual[: len(expected)] == expected
