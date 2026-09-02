# faust-streaming 0.12.1

Packaging-only patch release, dated 2026-07-19.

## Why this release

v0.12.0's **wheels** published to PyPI successfully, but its **source
distribution** upload was rejected:

```
400 Filename 'faust-streaming-0.12.0.tar.gz' is invalid,
    should be 'faust_streaming-0.12.0.tar.gz'.
```

PyPI now enforces [PEP 625](https://peps.python.org/pep-0625/), which requires
the sdist filename to use the *normalized* project name (underscores). The
release job built the sdist with the deprecated `setup.py sdist` under an old
setuptools, producing the legacy hyphenated name.

## Fixed

- Build the source distribution with `python -m build` and raise the
  `[build-system]` setuptools floor to `>=69`, so the sdist is named
  `faust_streaming-<version>.tar.gz` and PyPI accepts it (#712).

## Upgrade notes

There are **no code changes** in this release — it is functionally identical to
v0.12.0. It exists only so the source distribution is available on PyPI (e.g.
for consumers that build from sdist). If you install from wheels, v0.12.0 and
v0.12.1 are equivalent.

**Full changelog:** https://github.com/faust-streaming/faust/compare/v0.12.0...v0.12.1
