"""Fail if a real-looking credential is in the files git would commit.

Runs in CI on every PR and as an optional pre-commit hook (dev/install_hooks.sh).
Prints file:line and the kind of secret, never the secret itself.

Test fixtures use fake tokens assembled at runtime ("ghp_" + "..."), so no
token-shaped literal is in the repo; anything this finds is worth a look.

    uv run python dev/scan_secrets.py            # tracked + staged files
    uv run python dev/scan_secrets.py --staged   # only what is about to be committed
"""

from __future__ import annotations

import re
import subprocess
import sys

PATTERNS = {
    "Anthropic API key": r"sk-ant-[A-Za-z0-9_-]{20,}",
    "OpenAI API key": r"sk-(?:proj-)?[A-Za-z0-9]{32,}",
    "AWS access key": r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
    "GitHub token": r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})",
    "Slack token": r"\bxox[abposr]-[0-9]{6,}-[A-Za-z0-9-]{10,}",
    "Slack webhook": r"hooks\.slack\.com/services/T[A-Z0-9]{6,}/B[A-Z0-9]{6,}/[A-Za-z0-9]{16,}",
    "Stripe live key": r"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}",
    "Private key": r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----",
}
COMPILED = {kind: re.compile(p) for kind, p in PATTERNS.items()}
SKIP_FILES = {"uv.lock"}  # hashes, not secrets


def files(staged: bool) -> list[str]:
    args = ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"] if staged else [
        "git", "ls-files", "--cached", "--others", "--exclude-standard"
    ]  # fmt: skip
    out = subprocess.run(args, capture_output=True, text=True, check=True).stdout
    return [f for f in out.splitlines() if f and f not in SKIP_FILES]


def main() -> int:
    staged = "--staged" in sys.argv
    hits = 0
    for name in files(staged):
        try:
            with open(name, encoding="utf-8", errors="ignore") as fh:
                for n, line in enumerate(fh, start=1):
                    for kind, regex in COMPILED.items():
                        if regex.search(line):
                            article = "an" if kind[0] in "AEIOU" else "a"
                            print(f"{name}:{n}: looks like {article} {kind}")
                            hits += 1
        except (IsADirectoryError, FileNotFoundError):
            continue
    if hits:
        print(f"\n{hits} possible secret(s).")
        print("Remove them (put keys in .env, which git ignores) and rotate any real key.")
        return 1
    print("no secrets found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
