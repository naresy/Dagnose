"""Recent git commits that touched a DAG file (read-only `git log` / `git show`)."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from dagnose.models import Commit

_FIELD = "\x1f"
_RECORD = "\x1e"


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr.strip()[:300]}")
    return result.stdout


def recent_commits(repo: Path, relpath: str, limit: int, diff_max_chars: int) -> list[Commit]:
    """Newest first. `relpath` is the DAG file path relative to the repo root."""
    if not (repo / relpath).exists():
        raise FileNotFoundError(f"{relpath} not found in git repo {repo}")
    log = _git(
        repo,
        "log",
        f"-n{limit}",
        f"--format=%H{_FIELD}%an{_FIELD}%aI{_FIELD}%s{_RECORD}",
        "--",
        relpath,
    )
    commits: list[Commit] = []
    for record in filter(None, (r.strip() for r in log.split(_RECORD))):
        sha, author, date, subject = record.split(_FIELD, 3)
        diff = _git(repo, "show", "--format=", "--patch", sha, "--", relpath)
        commits.append(
            Commit(
                sha=sha,
                author=author,
                # git prints "Z" for UTC; fromisoformat accepts it only from Python 3.11.
                date=datetime.fromisoformat(date.replace("Z", "+00:00")),
                subject=subject,
                diff=diff[:diff_max_chars],
                diff_truncated=len(diff) > diff_max_chars,
            )
        )
    return commits
