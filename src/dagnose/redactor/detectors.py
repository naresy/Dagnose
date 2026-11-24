"""What counts as a secret. Each detector finds a span of text to replace.

Order matters: specific formats first (a JWT is also a high-entropy string), generic
ones last. Detectors never see text that was already replaced by a placeholder.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class Pattern:
    kind: str
    regex: re.Pattern[str]
    group: int | str = 0  # the group holding the secret; the rest of the match is kept
    in_code: bool = True  # also apply to source code (DAG source, git diffs)


# Known token formats and credentials embedded in URLs.
PATTERNS: tuple[Pattern, ...] = (
    Pattern(
        "private_key",
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)"
        ),
    ),
    Pattern("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")),
    Pattern("aws_access_key", re.compile(r"\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b")),
    Pattern(
        "github_token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})\b"),
    ),
    Pattern("slack_token", re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{10,}")),
    Pattern("slack_webhook", re.compile(r"https://hooks\.slack\.com/services/[A-Za-z0-9/_-]+")),
    Pattern("stripe_key", re.compile(r"\b(?:sk|rk|pk)_(?:live|test)_[A-Za-z0-9]{16,}")),
    Pattern("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}")),
    Pattern("llm_api_key", re.compile(r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{20,}")),
    # scheme://user:PASSWORD@host -- keep scheme, user and host (useful for diagnosis).
    Pattern(
        "password",
        re.compile(r"\b[a-z][a-z0-9+.\-]*://[^\s:/@'\"]*:(?P<secret>[^\s@/'\"]+)@", re.I),
        group="secret",
    ),
    # Authorization: Bearer XXX / Basic XXX / token XXX
    Pattern(
        "token",
        re.compile(r"\b(?:bearer|basic|token)\s+(?P<secret>[A-Za-z0-9\-._~+/]{12,}=*)", re.I),
        group="secret",
        in_code=False,
    ),
)

# Names whose value is a secret, e.g. password=..., "api_key": "...", db_password: ...
SENSITIVE_KEY = (
    r"(?:[a-z0-9]+[_-])*"
    r"(?:password|passwd|secret|token|api[_-]?key|apikey|access[_-]?key(?:[_-]?id)?"
    r"|secret[_-]?key|private[_-]?key|client[_-]?secret|credentials?|signature|sig"
    r"|session[_-]?id|cookie)"
)
SENSITIVE_KEY_RE = re.compile(rf"(?i){SENSITIVE_KEY}")
KEY_VALUE_RE = re.compile(
    rf"(?i)(?<![A-Za-z0-9])(?P<key>{SENSITIVE_KEY})['\"]?\s*[:=]\s*"
    r"(?:\"(?P<dq>[^\"\n]*)\"|'(?P<sq>[^'\n]*)'|(?P<bare>[^\s,;&'\"}\])]+))"
)

EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+\-])[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}\b"
)

# Candidate tokens for the entropy check.
TOKEN_RE = re.compile(r"(?<![A-Za-z0-9+/_\-])[A-Za-z0-9+/_\-]{24,}={0,2}(?![A-Za-z0-9+/_\-])")
_HEX_RE = re.compile(r"[0-9a-fA-F\-]+")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)

# Identifiers built from words and dates (run ids, file names such as
# "demo_orders_tax__compute_tax__manual__2026-11-24T14_13_18"): a date, or a word of
# four or more lowercase letters between separators. Random keys have neither.
_IDENTIFIER_RE = re.compile(r"\d{4}-\d{2}-\d{2}|(?:^|[_\-/])[a-z]{4,}(?=[_\-/]|$)")

PLACEHOLDER_RE = re.compile(r"\[REDACTED:[a-z_]+#\d+\]")

# Values that are not secrets even under a sensitive key.
_NOT_SECRET = {"", "none", "null", "true", "false", "***", "*****", "redacted", "masked"}


def is_placeholder_or_empty(value: str) -> bool:
    v = value.strip()
    return (
        v.lower() in _NOT_SECRET or v.startswith(("[REDACTED", "{{", "${", "%(")) or set(v) <= {"*"}
    )


def shannon_entropy(text: str) -> float:
    counts = Counter(text)
    n = len(text)
    return -sum(c / n * math.log2(c / n) for c in counts.values())


def looks_random(token: str) -> bool:
    """High-entropy, mixed-case alphanumeric token, e.g. an unlabelled API key.

    Deliberately conservative: hex strings (git SHAs, UUIDs, task instance ids),
    lowercase-only words, CamelCase class names and file paths are kept, because
    they are evidence. Labelled secrets (api_key=...) are caught by KEY_VALUE_RE."""
    core = token.rstrip("=")
    if len(core) < 24 or _UUID_RE.fullmatch(core) or _HEX_RE.fullmatch(core):
        return False
    if _IDENTIFIER_RE.search(core):
        return False
    has_upper = any(c.isupper() for c in core)
    has_lower = any(c.islower() for c in core)
    has_digit = any(c.isdigit() for c in core)
    if not (has_upper and has_lower and has_digit):
        return False
    return shannon_entropy(core) >= 4.0
