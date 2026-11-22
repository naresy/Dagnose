"""Turn raw task logs into numbered lines plus parsed exceptions.

Airflow 3 returns structured events, and exceptions arrive already parsed in
`error_detail`. Airflow 2 returns text, so the traceback is parsed from that.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from airflow_doctor.models import ExceptionInfo, LogLine, StackFrame

_GROUP_MARKERS = ("::group::", "::endgroup::")


@dataclass
class ParsedLog:
    lines: list[LogLine] = field(default_factory=list)
    exceptions: list[ExceptionInfo] = field(default_factory=list)  # root cause first

    def tail(self, n: int) -> list[LogLine]:
        return self.lines[-n:] if n > 0 else []


def parse_log(raw: list[dict[str, Any]] | str) -> ParsedLog:
    return parse_text(raw) if isinstance(raw, str) else parse_structured(raw)


# ---- Airflow 3: structured events ---------------------------------------------------


def parse_structured(events: list[dict[str, Any]]) -> ParsedLog:
    out = ParsedLog()
    for event in events:
        message = str(event.get("event", ""))
        if message.startswith(_GROUP_MARKERS):
            continue  # UI folding markers: noise for diagnosis
        if event.get("logger") is None and event.get("level") is None and _is_log_source(message):
            continue  # "where this log was read from" header
        out.lines.append(
            LogLine(
                n=len(out.lines) + 1,
                timestamp=event.get("timestamp"),
                level=event.get("level"),
                logger=event.get("logger"),
                message=message,
            )
        )
        if event.get("error_detail"):
            # Airflow (structlog) lists the raised exception first and its causes after;
            # the last error event in the log is the one that failed the task.
            out.exceptions = [_exc_from_detail(d) for d in reversed(event["error_detail"])]
    return out


def _is_log_source(message: str) -> bool:
    return message.startswith(("/", "http://", "https://", "s3://", "gs://", "wasb://"))


def _exc_from_detail(detail: dict[str, Any]) -> ExceptionInfo:
    frames = [
        StackFrame(filename=str(f.get("filename", "?")), lineno=f.get("lineno"), name=f.get("name"))
        for f in detail.get("frames") or []
    ]
    return ExceptionInfo(
        type=str(detail.get("exc_type", "Exception")),
        message=str(detail.get("exc_value", "")),
        frames=frames,
        notes=[str(n) for n in detail.get("exc_notes") or []],
    )


# ---- Airflow 2: text ----------------------------------------------------------------

# [2026-11-22, 15:34:54 UTC] {taskinstance.py:3311} ERROR - Task failed with exception
_HEADER = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+(?:\{(?P<src>[^}]*)\}\s+)?(?P<level>[A-Z]+)\s+-\s?(?P<msg>.*)$"
)
_TRACEBACK_START = "Traceback (most recent call last):"
_FRAME = re.compile(r'^\s+File "(?P<file>[^"]+)", line (?P<line>\d+)(?:, in (?P<name>.+))?$')
_EXC_LINE = re.compile(r"^(?P<type>[A-Za-z_][\w.]*)(?::\s?(?P<msg>.*))?$")
_CHAIN_MARKERS = (
    "The above exception was the direct cause of the following exception:",
    "During handling of the above exception, another exception occurred:",
)


def parse_text(text: str) -> ParsedLog:
    out = ParsedLog()
    level: str | None = None
    logger: str | None = None
    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        m = _HEADER.match(raw_line)
        if m:
            level, logger = m["level"].lower(), m["src"]
            ts, msg = m["ts"], m["msg"]
        else:
            ts, msg = None, raw_line  # continuation line (e.g. traceback) keeps the level
        if msg.startswith(_GROUP_MARKERS):
            continue
        out.lines.append(
            LogLine(n=len(out.lines) + 1, timestamp=ts, level=level, logger=logger, message=msg)
        )
    out.exceptions = _last_traceback_chain([line.message for line in out.lines])
    return out


def _last_traceback_chain(lines: list[str]) -> list[ExceptionInfo]:
    """Parse every traceback, then keep the final chain (root cause first)."""
    chains: list[list[ExceptionInfo]] = []
    chained_next = False
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line in _CHAIN_MARKERS:
            chained_next = True
            i += 1
            continue
        if line != _TRACEBACK_START:
            i += 1
            continue
        exc, i = _parse_one_traceback(lines, i + 1)
        if exc is None:
            continue
        if chained_next and chains:
            chains[-1].append(exc)
        else:
            chains.append([exc])
        chained_next = False
    return chains[-1] if chains else []


def _parse_one_traceback(lines: list[str], i: int) -> tuple[ExceptionInfo | None, int]:
    frames: list[StackFrame] = []
    while i < len(lines):
        line = lines[i]
        frame = _FRAME.match(line)
        if frame:
            frames.append(
                StackFrame(filename=frame["file"], lineno=int(frame["line"]), name=frame["name"])
            )
            i += 1
            continue
        if line.startswith((" ", "\t")):  # source line or ^^^ marker under a frame
            i += 1
            continue
        exc = _EXC_LINE.match(line.strip())
        if exc:
            return ExceptionInfo(type=exc["type"], message=exc["msg"] or "", frames=frames), i + 1
        return None, i
    return None, i
