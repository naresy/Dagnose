"""Replace secrets in a FailureContext with placeholders before it leaves the process.

Every string in the context is scanned, so fields added later are covered too. A few
fields that are identifiers, not free text (ids, timestamps, line numbers), are left
alone. Redaction is one-way: the original values are never stored, so nothing
downstream (LLM prompts, fix PRs, saved files) can put a secret back.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from airflow_doctor.models import FailureContext, RedactedContext, Redaction
from airflow_doctor.redactor.detectors import (
    EMAIL_RE,
    KEY_VALUE_RE,
    PATTERNS,
    PLACEHOLDER_RE,
    SENSITIVE_KEY_RE,
    TOKEN_RE,
    is_placeholder_or_empty,
    looks_random,
)

# Keys whose values are identifiers or metadata, not free text. Scanning them only
# risks false positives (a run id or git SHA looks random) and they hold no secrets.
_SKIP_KEYS = frozenset(
    {
        "job", "dag_id", "run_id", "task_id", "try_number", "map_index", "sha",
        "timestamp", "start_date", "end_date", "date", "collected_at", "failed_at",
        "api_version", "airflow_version", "level", "logger", "task_state", "state",
        "operator", "dag_file", "filename", "lineno", "name", "type", "qualified_type",
        "n", "is_current", "duration_s", "log_total_lines", "redactions",
    }
)  # fmt: skip
# Fields holding source code: only quoted values count as secrets there, because an
# unquoted `password = Variable.get(...)` is an expression, not a secret.
_CODE_KEYS = frozenset({"dag_source", "diff"})
# Fields holding names of people.
_PERSON_KEYS = frozenset({"author"})
# Known secret values shorter than this are not replaced by plain string match, so a
# short value (a 3-letter pin) cannot wipe out unrelated words.
MIN_KNOWN_VALUE = 6
# Free-form user data: a sensitive-looking key redacts its whole value.
_PARAM_ROOTS = frozenset({"task_params", "run_conf"})


@dataclass
class _State:
    placeholders: dict[str, str] = field(default_factory=dict)  # value -> placeholder
    counters: dict[str, int] = field(default_factory=dict)
    redactions: list[Redaction] = field(default_factory=list)
    _seen: set[tuple[str, str]] = field(default_factory=set)

    def placeholder(self, kind: str, value: str, path: str) -> str:
        ph = self.placeholders.get(value)
        if ph is None:
            self.counters[kind] = self.counters.get(kind, 0) + 1
            ph = f"[REDACTED:{kind}#{self.counters[kind]}]"
            self.placeholders[value] = ph
        if (path, ph) not in self._seen:
            self._seen.add((path, ph))
            kind_of_ph = ph[len("[REDACTED:") : ph.index("#")]
            self.redactions.append(Redaction(kind=kind_of_ph, path=path, placeholder=ph))
        return ph


class Redactor:
    def __init__(
        self,
        extra_patterns: Iterable[str] = (),
        allow_patterns: Iterable[str] = (),
        redact_people: bool = True,
    ) -> None:
        self.extra = [re.compile(p) for p in extra_patterns]
        self.allow = [re.compile(p) for p in allow_patterns]
        self.redact_people = redact_people

    @classmethod
    def from_settings(cls, settings: Any) -> Redactor:
        return cls(
            extra_patterns=settings.redact_extra_patterns,
            allow_patterns=settings.redact_allow_patterns,
            redact_people=settings.redact_people,
        )

    def redact(self, ctx: FailureContext) -> RedactedContext:
        return self.session().redact(ctx)

    def session(self) -> RedactionSession:
        """A per-job session: later text (e.g. logs fetched by the agent's tools) gets
        the same placeholders as the context. The value -> placeholder map lives in
        memory only and is never serialized."""
        return RedactionSession(self)

    def _redact_with(self, ctx: FailureContext, state: _State) -> tuple[RedactedContext, Any]:
        data = ctx.model_dump(mode="json")
        data.pop("redactions", None)
        people = (
            sorted({c["author"] for c in data.get("recent_commits", []) if c.get("author")})
            if self.redact_people
            else []
        )
        people_re = (
            re.compile(r"\b(?:" + "|".join(re.escape(p) for p in people if len(p) >= 3) + r")\b")
            if any(len(p) >= 3 for p in people)
            else None
        )
        walked = self._walk(data, "", state, people_re, in_params=False)
        # Second pass: a secret found in a later field may also appear, unlabelled, in
        # an earlier one. Placeholders are left alone, so this is cheap and idempotent.
        walked = self._walk(walked, "", state, people_re, in_params=False)
        redacted = RedactedContext.model_validate({**walked, "redactions": state.redactions})
        return redacted, people_re

    # ---- tree walk ---------------------------------------------------------------

    def _walk(
        self,
        node: Any,
        path: str,
        state: _State,
        people_re: re.Pattern[str] | None,
        *,
        in_params: bool,
        key: str = "",
    ) -> Any:
        if isinstance(node, dict):
            out: dict[str, Any] = {}
            for k, v in node.items():
                child = f"{path}.{k}" if path else str(k)
                if k in _SKIP_KEYS and not in_params:
                    out[k] = v
                elif in_params and SENSITIVE_KEY_RE.fullmatch(str(k)) and _is_secret_value(v):
                    out[k] = state.placeholder("secret_param", str(v), child)
                else:
                    out[k] = self._walk(
                        v,
                        child,
                        state,
                        people_re,
                        in_params=in_params or (not path and k in _PARAM_ROOTS),
                        key=str(k),
                    )
            return out
        if isinstance(node, list):
            return [
                self._walk(v, f"{path}[{i}]", state, people_re, in_params=in_params, key=key)
                for i, v in enumerate(node)
            ]
        if isinstance(node, str):
            if key in _PERSON_KEYS and self.redact_people and not is_placeholder_or_empty(node):
                return state.placeholder("person", node, path)
            return self.redact_text(node, path, state, people_re, code=key in _CODE_KEYS)
        return node

    # ---- text ---------------------------------------------------------------------

    def redact_text(
        self,
        text: str,
        path: str,
        state: _State,
        people_re: re.Pattern[str] | None = None,
        *,
        code: bool = False,
    ) -> str:
        for pattern in PATTERNS:
            if code and not pattern.in_code:
                continue
            text = self._sub(pattern.regex, pattern.group, pattern.kind, text, path, state)
        text = self._key_values(text, path, state, code=code)
        text = self._sub(EMAIL_RE, 0, "email", text, path, state)
        text = self._entropy(text, path, state)
        for extra in self.extra:
            text = self._sub(extra, 0, "custom", text, path, state)
        if people_re is not None:
            text = self._sub(people_re, 0, "person", text, path, state)
        return self._known_values(text, path, state)

    def _known_values(self, text: str, path: str, state: _State) -> str:
        """Replace any value already identified as a secret in this job, wherever and
        however it appears again (e.g. a password found in a URL, later printed bare)."""
        for value, placeholder in sorted(
            state.placeholders.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if _replaceable(value) and value in text:
                text = text.replace(value, placeholder)
                state.placeholder("", value, path)  # record the extra location
        return text

    def _allowed(self, value: str) -> bool:
        return is_placeholder_or_empty(value) or any(a.search(value) for a in self.allow)

    def _sub(
        self,
        regex: re.Pattern[str],
        group: int | str,
        kind: str,
        text: str,
        path: str,
        state: _State,
    ) -> str:
        def replace(m: re.Match[str]) -> str:
            value = m.group(group)
            if value is None or self._allowed(value) or _inside_placeholder(m):
                return m.group(0)
            start, end = m.start(group) - m.start(0), m.end(group) - m.start(0)
            whole = m.group(0)
            return whole[:start] + state.placeholder(kind, value, path) + whole[end:]

        return regex.sub(replace, text)

    def _key_values(self, text: str, path: str, state: _State, *, code: bool) -> str:
        def replace(m: re.Match[str]) -> str:
            for group in ("dq", "sq") if code else ("dq", "sq", "bare"):
                value = m.group(group)
                if value is None:
                    continue
                if self._allowed(value):
                    return m.group(0)
                start, end = m.start(group) - m.start(0), m.end(group) - m.start(0)
                whole = m.group(0)
                return whole[:start] + state.placeholder("secret", value, path) + whole[end:]
            return m.group(0)

        return KEY_VALUE_RE.sub(replace, text)

    def _entropy(self, text: str, path: str, state: _State) -> str:
        def replace(m: re.Match[str]) -> str:
            token = m.group(0)
            if not looks_random(token) or self._allowed(token):
                return token
            return state.placeholder("high_entropy", token, path)

        return TOKEN_RE.sub(replace, text)


def _replaceable(value: str) -> bool:
    """Safe to replace by plain string match: long enough, and not an ordinary word
    (a param like api_token="default" must not wipe out every "default" in the logs)."""
    if len(value) < MIN_KNOWN_VALUE:
        return False
    return len(value) >= 12 or not value.replace(" ", "").isalpha()


def _inside_placeholder(m: re.Match[str]) -> bool:
    """True when the match overlaps a placeholder already in the text."""
    for ph in PLACEHOLDER_RE.finditer(m.string):
        if ph.start() < m.end() and m.start() < ph.end():
            return True
    return False


def _is_secret_value(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    return isinstance(value, str) and not is_placeholder_or_empty(value)


class RedactionSession:
    """Redacts one job's context and any follow-up text with one placeholder map."""

    def __init__(self, redactor: Redactor) -> None:
        self._redactor = redactor
        self._state = _State()
        self._people_re: re.Pattern[str] | None = None

    def redact(self, ctx: FailureContext) -> RedactedContext:
        redacted, self._people_re = self._redactor._redact_with(ctx, self._state)
        return redacted

    def redact_text(self, text: str, path: str, *, code: bool = False) -> str:
        """Redact extra text (e.g. a tool result) with this job's placeholders."""
        return self._redactor.redact_text(text, path, self._state, self._people_re, code=code)


def redact(ctx: FailureContext, **kwargs: Any) -> RedactedContext:
    """Redact with default settings (see Redactor for options)."""
    return Redactor(**kwargs).redact(ctx)
