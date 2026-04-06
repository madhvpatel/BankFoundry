from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


def _looks_like_repo_root(path: Path) -> bool:
    return path.is_dir() and (path / "app").is_dir() and (path / "config.py").is_file()


@lru_cache(maxsize=1)
def repo_root() -> Path:
    env_root = str(os.getenv("ACQUIGURU_REPO_ROOT") or os.getenv("APP_REPO_ROOT") or "").strip()
    if env_root:
        candidate = Path(env_root).expanduser().resolve()
        if _looks_like_repo_root(candidate):
            return candidate

    here = Path(__file__).resolve()
    for candidate in (here.parent, *here.parents):
        if _looks_like_repo_root(candidate):
            return candidate

    return here.parents[1]


def repo_path(*parts: str) -> Path:
    return repo_root().joinpath(*parts)


def resolve_repo_relative(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    return path if path.is_absolute() else repo_root() / path
