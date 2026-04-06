# Portable Paths Hardening

## What was broken

The project had a small number of path assumptions that were safe only on one machine or only when commands were started from the repo root.

Examples:

- one helper script wrote results to a fixed `/Users/...` path
- some code located fixtures, knowledge files, or card templates by repeating local parent-directory math in each module
- some helper scripts wrote output using the current working directory instead of the project directory

That makes deployment and automation more brittle when the repo is moved, renamed, mounted in a container, or started from another folder.

## What changed

A shared project path helper was added in `app/project_paths.py`.

It now gives the app one consistent way to resolve:

- the repo root
- repo-relative paths like `tests/fixtures/...`
- paths passed in as relative strings

The following runtime paths now use that shared resolver instead of machine-specific or ad hoc path logic:

- bank-foundry fixture lookup
- support history fixture lookup
- insight card template directory lookup
- knowledge-base and KB index root lookup
- helper script artifact output paths

## How to verify

Run:

```bash
pytest -q tests/test_project_paths.py
```

Recommended broader check:

```bash
pytest -q \
  tests/test_project_paths.py \
  tests/test_adaptive_provider_layer.py \
  tests/test_adaptive_intelligence_layer.py
```

Manual spot check:

1. Move or rename the repo folder.
2. Run `python run_custom_tests.py`.
3. Confirm output is written under `artifacts/sprint1_manual_validation/` inside the moved repo.
4. Start the app from a different working directory and confirm fixture-backed and knowledge-backed features still load.

## Real issues vs noise

Real product issues:

- the app cannot find the repo root because required files like `config.py` or `app/` are missing
- a deployment mounts only part of the repo, so runtime fixture or prompt files are genuinely absent
- a configurable path is passed in incorrectly and points outside the deployed project

Usually not a product issue:

- docs still contain absolute clickable workspace links for local review notes
- archived reports still mention old local paths
- tests use in-memory sqlite URLs like `sqlite+pysqlite:///:memory:`; those are database URLs, not filesystem path problems
