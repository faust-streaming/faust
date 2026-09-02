"""Guard the hand-maintained parts of ``setup.py``.

``extras_require()`` is driven entirely by the ``BUNDLES`` set, so a
``requirements/extras/<name>.txt`` file whose name is missing from ``BUNDLES``
is a dead extra: ``pip install faust-streaming[<name>]`` silently installs
nothing.  That is exactly what happened to ``aerospike``, which was advertised
in the README while resolving to an empty list.
"""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SETUP_PY = ROOT / "setup.py"
EXTRAS_DIR = ROOT / "requirements" / "extras"


def _bundles_from_setup_py():
    """Read ``BUNDLES`` without importing (and thus running) ``setup.py``."""
    tree = ast.parse(SETUP_PY.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "BUNDLES" for t in node.targets
        ):
            return set(ast.literal_eval(node.value))
    raise AssertionError("BUNDLES not found in setup.py")


@pytest.mark.skipif(
    not SETUP_PY.exists(), reason="running against an installed package, not a checkout"
)
def test_every_extras_file_is_declared_in_bundles():
    bundles = _bundles_from_setup_py()
    on_disk = {path.stem for path in EXTRAS_DIR.glob("*.txt")}

    undeclared = on_disk - bundles
    assert not undeclared, (
        f"requirements/extras/{{{','.join(sorted(undeclared))}}}.txt exist but are "
        f"missing from BUNDLES in setup.py, so those extras install nothing"
    )


@pytest.mark.skipif(
    not SETUP_PY.exists(), reason="running against an installed package, not a checkout"
)
def test_every_bundle_has_a_requirements_file():
    bundles = _bundles_from_setup_py()
    on_disk = {path.stem for path in EXTRAS_DIR.glob("*.txt")}

    missing = bundles - on_disk
    assert not missing, (
        f"BUNDLES declares {sorted(missing)} but "
        f"requirements/extras/<name>.txt is missing for them"
    )
