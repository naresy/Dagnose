"""Data contracts passed between pipeline stages.

Each stage reads one of these and writes the next, so every stage can be tested on
its own with JSON fixtures: FailureJob -> FailureContext -> (redactor) -> (agent).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

ApiVersion = Literal["v1", "v2"]

_LIBRARY_PATH_MARKERS = ("/site-packages/", "/dist-packages/", "/lib/python3", "<frozen ")


class FailureJob(BaseModel):
    """What the Airflow listener enqueues: just enough to find the failure again."""

    dag_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    try_number: int | None = Field(default=None, ge=1)  # None = the latest try
    map_index: int = -1
    failed_at: datetime | None = None


class LogLine(BaseModel):
    """One log line. `n` is stable so a diagnosis can cite evidence by line number."""

    n: int
    timestamp: str | None = None
    level: str | None = None
    logger: str | None = None
    message: str


class StackFrame(BaseModel):
    filename: str
    lineno: int | None = None
    name: str | None = None

    @property
    def in_user_code(self) -> bool:
        """True when the frame is in DAG / project code rather than a library."""
        return not any(marker in self.filename for marker in _LIBRARY_PATH_MARKERS)


class ExceptionInfo(BaseModel):
    type: str  # short class name, e.g. "AirflowNotFoundException", same on Airflow 2 and 3
    qualified_type: str | None = None  # with module path, when the log shows it
    message: str
    frames: list[StackFrame] = Field(default_factory=list)  # outermost call first
    notes: list[str] = Field(default_factory=list)


class RunSummary(BaseModel):
    """One previous (or the current) attempt of the same task in another DAG run."""

    run_id: str
    state: str | None
    try_number: int | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration_s: float | None = None
    is_current: bool = False


class Commit(BaseModel):
    sha: str
    author: str
    date: datetime
    subject: str
    diff: str = ""  # diff of the DAG file only
    diff_truncated: bool = False


class FailureContext(BaseModel):
    """Everything collected about one failed task try. Collector output."""

    job: FailureJob
    airflow_version: str | None = None
    api_version: ApiVersion | None = None

    task_state: str | None = None
    operator: str | None = None
    hostname: str | None = None
    duration_s: float | None = None
    task_params: dict[str, object] = Field(default_factory=dict)  # rendered template fields
    run_conf: dict[str, object] = Field(default_factory=dict)

    # Root cause first, the exception that failed the task last.
    exceptions: list[ExceptionInfo] = Field(default_factory=list)
    log_tail: list[LogLine] = Field(default_factory=list)
    log_total_lines: int = 0

    dag_file: str | None = None  # path relative to the DAGs folder when known
    dag_source: str | None = None
    run_history: list[RunSummary] = Field(default_factory=list)  # newest first
    recent_commits: list[Commit] = Field(default_factory=list)  # newest first

    # Collection is best effort: one missing piece must not lose the rest.
    collection_errors: list[str] = Field(default_factory=list)
    collected_at: datetime

    @property
    def exception(self) -> ExceptionInfo | None:
        """The exception that failed the task."""
        return self.exceptions[-1] if self.exceptions else None


class Redaction(BaseModel):
    """One redacted value: what kind it was and where. Never the value itself."""

    kind: str  # e.g. "password", "jwt", "email", "secret_param"
    path: str  # e.g. "log_tail[3].message", "task_params.op_kwargs.api_key"
    placeholder: str  # e.g. "[REDACTED:password#1]"; same value -> same placeholder


class RedactedContext(FailureContext):
    """A FailureContext with secrets replaced by placeholders. The only form that may
    leave the process (LLM calls) or be written to disk."""

    redactions: list[Redaction] = Field(default_factory=list)

    def summary(self) -> dict[str, int]:
        """Number of distinct redacted values per kind."""
        seen: dict[str, set[str]] = {}
        for r in self.redactions:
            seen.setdefault(r.kind, set()).add(r.placeholder)
        return {kind: len(placeholders) for kind, placeholders in sorted(seen.items())}


# ---- diagnosis (agent output) -------------------------------------------------------

DiagnosisCategory = Literal[
    "code_bug",  # the DAG / task code is wrong
    "upstream_data",  # input data missing, late, or in an unexpected shape
    "dependency",  # a package / image / version problem
    "config",  # connections, variables, settings, typos in ids
    "permissions",  # auth or access denied
    "timeout",  # ran longer than allowed
    "infra",  # worker, memory, network, scheduler, database
    "unknown",
]
EvidenceSource = Literal[
    "log", "dag_source", "exception", "task_params", "run_history", "commit", "tool_result"
]


class Evidence(BaseModel):
    """One fact the diagnosis rests on, quoted from the context so it can be checked."""

    source: EvidenceSource
    line: int | None = None  # log line n, or DAG source line number
    quote: str  # exact text from that source
    verified: bool = False  # set by Dagnose, not the model: the quote was found there


class Diagnosis(BaseModel):
    summary: str  # one plain sentence a non-expert can read
    category: DiagnosisCategory
    root_cause: str
    evidence: list[Evidence] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    suggested_fix: str | None = None
    patch: str | None = None  # unified diff against the DAG file, only for code fixes


class ToolCallRecord(BaseModel):
    name: str
    input: dict[str, object] = Field(default_factory=dict)
    is_error: bool = False


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            cache_read_input_tokens=self.cache_read_input_tokens + other.cache_read_input_tokens,
            cache_creation_input_tokens=self.cache_creation_input_tokens
            + other.cache_creation_input_tokens,
        )


class DiagnosisRun(BaseModel):
    """A diagnosis plus how it was made. Saved next to the redacted context."""

    job: FailureJob
    diagnosis: Diagnosis
    provider: str
    model: str  # requested
    served_model: str | None = None  # may differ when a fallback model answered
    turns: int
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    duration_s: float
    evidence_verified: int = 0
    evidence_total: int = 0
    confidence_capped: bool = False  # lowered because the evidence did not check out
    model_confidence: float  # what the model said, before any cap
    created_at: datetime
