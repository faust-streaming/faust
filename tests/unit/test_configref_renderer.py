"""Tests for ``extra/tools/render_configuration_reference.py``.

The script generates ``docs/includes/settingref.txt``, which
``docs/userguide/settings.rst`` includes -- so a bug in it silently corrupts
the published configuration reference rather than failing anything.  Three did:

* it dedented docstrings by scanning for the first *indented* line, which broke
  once Python 3.13 started stripping common leading whitespace from docstrings
  at compile time -- from then on the first indented line it found was the body
  of a ``.. warning::``, whose indent it removed, so the content escaped the
  admonition;
* it stripped one character too many while doing it; and
* it rendered types through ``__module__``, which for :class:`pathlib.Path`
  became the private ``pathlib._local`` on 3.13.

All three made the output depend on which interpreter ran the script, which is
what these tests exist to prevent.
"""

import importlib.util
import pathlib

import pytest

RENDERER = (
    pathlib.Path(__file__).parents[2]
    / "extra"
    / "tools"
    / "render_configuration_reference.py"
)


def _load_renderer():
    spec = importlib.util.spec_from_file_location("_configref", RENDERER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rst():
    if not RENDERER.exists():  # pragma: no cover
        pytest.skip("renderer not present")
    return _load_renderer().Rst()


#: The same docstring as CPython <=3.12 and >=3.13 present it.  3.13 strips the
#: common leading whitespace at compile time, so the body arrives flush left.
DOCSTRING_INDENTED = (
    "Summary line.\n"
    "\n"
    "        Body paragraph.\n"
    "\n"
    "        .. warning::\n"
    "\n"
    "            Nested directive body.\n"
    "        "
)
DOCSTRING_DEDENTED = (
    "Summary line.\n"
    "\n"
    "Body paragraph.\n"
    "\n"
    ".. warning::\n"
    "\n"
    "    Nested directive body.\n"
)


def test_normalize_is_interpreter_independent(rst) -> None:
    """The two docstring forms must render identically.

    Otherwise the generated reference depends on which Python ran `make
    configref`, and every regeneration produces a spurious diff.
    """
    assert rst.normalize_docstring_indent(
        DOCSTRING_INDENTED
    ) == rst.normalize_docstring_indent(DOCSTRING_DEDENTED)


@pytest.mark.parametrize(
    "docstring", [DOCSTRING_INDENTED, DOCSTRING_DEDENTED], ids=["indented", "dedented"]
)
def test_directive_body_keeps_its_indent(rst, docstring) -> None:
    """A ``.. warning::`` body must stay indented under the directive.

    This is the failure that mattered: flush-left content after a directive is
    not part of it, so the admonition renders empty and its text becomes loose
    paragraphs.
    """
    out = rst.normalize_docstring_indent(docstring).splitlines()

    body = out.index(".. warning::")
    nested = next(line for line in out[body + 1 :] if line.strip())
    indent = len(nested) - len(nested.lstrip())
    rendered = "\n".join(out)
    assert (
        indent > 0
    ), f"directive body is flush left, so it escaped the warning:\n{rendered}"


@pytest.mark.parametrize(
    "docstring", [DOCSTRING_INDENTED, DOCSTRING_DEDENTED], ids=["indented", "dedented"]
)
def test_body_is_dedented_to_column_zero(rst, docstring) -> None:
    """Ordinary paragraphs must end up flush left.

    They are spliced into the page at top level; leaving them indented would
    make RST read them as a block quote.
    """
    out = rst.normalize_docstring_indent(docstring).splitlines()
    assert out[0] == "Summary line."
    assert out[2] == "Body paragraph."


def test_public_module_prefers_the_documented_name(rst) -> None:
    """`pathlib.Path` must render as `pathlib.Path` on every version.

    3.13 moved it to ``pathlib._local``; a reference to that resolves nowhere.
    """
    assert rst.public_module(pathlib.Path) == "pathlib"
    assert "._local" not in rst.to_ref(pathlib.Path)


def test_public_module_leaves_ordinary_types_alone(rst) -> None:
    assert rst.public_module(int) == "builtins"
    assert rst.public_module(pytest.ExceptionInfo).startswith("_pytest")


def test_related_cli_options_are_lists(rst) -> None:
    """Every setting must declare its CLI options as a list, not a string.

    The renderer iterates the value, so a bare string is rendered one character
    at a time -- ``:option:`faust -`, :option:`faust -`, :option:`faust d```
    and so on.  The declared type is ``Mapping[str, List[str]]``, but the
    decorator takes ``**kwargs: Any``, so nothing else catches this.
    """
    from faust.types.settings import Settings

    bad = {
        name: param.related_cli_options
        for name, param in Settings.SETTINGS.items()
        if param.related_cli_options
        and any(isinstance(opts, str) for opts in param.related_cli_options.values())
    }
    assert not bad, f"settings declaring CLI options as a bare string: {bad}"


def test_renders_without_error() -> None:
    """The whole reference renders -- a smoke test over every real setting."""
    import io

    module = _load_renderer()
    out = io.StringIO()
    module.render(fh=out)
    text = out.getvalue()

    assert ".. setting:: broker" in text
    # The per-character regression, spelled out: `faust -` would appear if any
    # setting's options were iterated as a string.
    assert ":option:`faust -`" not in text
