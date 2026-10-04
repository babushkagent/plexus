"""Logs, traces, metrics: one shared correlation context.

Shapes follow OpenTelemetry / W3C so the stdlib implementation can be swapped for
an OTLP exporter without touching call sites. Every log line carries trace/span
ids, which is what makes multi-tenant incident triage survivable.
"""

from __future__ import annotations

import json
import logging
import random
import sys
import threading
import time
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

TRACE_VERSION = "00"
FLAG_SAMPLED = "01"
FLAG_NOT_SAMPLED = "00"

_NO_BINDINGS: Mapping[str, str] = MappingProxyType({})

_bindings: ContextVar[Mapping[str, str]] = ContextVar(
    "plexus.log_bindings", default=_NO_BINDINGS
)
_current_span: ContextVar[Span | None] = ContextVar("plexus.current_span", default=None)


def new_trace_id() -> str:
    return f"{random.getrandbits(128):032x}"


def new_span_id() -> str:
    return f"{random.getrandbits(64):016x}"


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    sampled: bool = True

    @classmethod
    def new(cls, *, sampled: bool = True) -> TraceContext:
        return cls(new_trace_id(), new_span_id(), None, sampled)

    @classmethod
    def parse(cls, header: str | None) -> TraceContext | None:
        if not header:
            return None
        parts = header.strip().split("-")
        if len(parts) < 4 or len(parts[0]) != 2 or len(parts[1]) != 32 or len(parts[2]) != 16:
            return None
        trace_id, span_id, flags = parts[1], parts[2], parts[3][:2]
        if set(trace_id) == {"0"} or set(span_id) == {"0"}:
            return None
        return cls(trace_id.lower(), span_id.lower(), None, bool(int(flags, 16) & 1))

    def to_header(self) -> str:
        flags = FLAG_SAMPLED if self.sampled else FLAG_NOT_SAMPLED
        return f"{TRACE_VERSION}-{self.trace_id}-{self.span_id}-{flags}"

    def child(self) -> TraceContext:
        return TraceContext(self.trace_id, new_span_id(), self.span_id, self.sampled)


@dataclass(slots=True)
class Span:
    name: str
    context: TraceContext
    start_unix_ns: int
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    status: str = "ok"
    end_unix_ns: int | None = None

    def set(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, **attrs: Any) -> None:
        self.events.append({"name": name, "time_unix_ns": time.time_ns(), "attributes": attrs})

    def record_error(self, exc: BaseException) -> None:
        self.status = "error"
        self.add_event("exception", type=type(exc).__name__, message=str(exc))

    @property
    def duration_ms(self) -> float:
        end = self.end_unix_ns or time.time_ns()
        return (end - self.start_unix_ns) / 1_000_000

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.context.trace_id,
            "span_id": self.context.span_id,
            "parent_span_id": self.context.parent_span_id,
            "name": self.name,
            "start_time_unix_ns": self.start_unix_ns,
            "duration_ms": round(self.duration_ms, 3),
            "status": self.status,
            "attributes": self.attributes,
            "events": self.events,
        }


class Tracer:
    def __init__(self, service: str, exporters: Sequence[Any] | None = None) -> None:
        self.service = service
        self._exporters: list[Any] = list(exporters or [])
        self._lock = threading.Lock()

    def add_exporter(self, exporter: Any) -> None:
        with self._lock:
            self._exporters.append(exporter)

    @contextmanager
    def start(self, name: str, *, context: TraceContext | None = None, **attrs: Any) -> Iterator[Span]:
        parent = _current_span.get()
        if context is not None:
            ctx = context
        elif parent is not None:
            ctx = parent.context.child()
        else:
            ctx = TraceContext.new()
        span = Span(name, ctx, time.time_ns(), dict(attrs))
        token = _current_span.set(span)
        try:
            yield span
        except Exception as exc:
            span.record_error(exc)
            raise
        finally:
            span.end_unix_ns = time.time_ns()
            _current_span.reset(token)
            for exporter in list(self._exporters):
                try:
                    exporter.export(span)
                except Exception:  # telemetry must never break the request path
                    logging.getLogger("plexus.telemetry").debug("span export failed", exc_info=True)

    def current(self) -> Span | None:
        return _current_span.get()


class InMemorySpanExporter:
    """Test/dev exporter: bounded ring of finished spans."""

    def __init__(self, capacity: int = 1024) -> None:
        self.spans: deque[dict[str, Any]] = deque(maxlen=capacity)

    def export(self, span: Span) -> None:
        self.spans.append(span.to_dict())

    def by_name(self, name: str) -> list[dict[str, Any]]:
        return [s for s in self.spans if s["name"] == name]

    def clear(self) -> None:
        self.spans.clear()


class JsonFormatter(logging.Formatter):
    RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
        "taskName",
        "message",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "stack_info",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        context = _bindings.get()
        if context:
            payload.update(context)
        for key, value in record.__dict__.items():
            if key not in self.RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO", *, json_logs: bool = True, stream: Any = None) -> logging.Logger:
    logger = logging.getLogger("plexus")
    logger.setLevel(level)
    logger.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter() if json_logs else logging.Formatter("%(levelname)s %(name)s %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


@contextmanager
def bind(**values: Any) -> Iterator[None]:
    """Attach structured fields to every log line (and span) in this context."""
    merged = {**_bindings.get(), **{k: str(v) for k, v in values.items() if v is not None}}
    token: Token[Mapping[str, str]] = _bindings.set(merged)
    try:
        yield
    finally:
        _bindings.reset(token)


def current_bindings() -> dict[str, str]:
    return dict(_bindings.get())


class Metrics:
    """Minimal counter/gauge/histogram registry with Prometheus exposition."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._help: dict[str, str] = {}

    @staticmethod
    def _label_key(labels: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((labels or {}).items()))

    def counter(self, name: str, value: float = 1.0, labels: Mapping[str, str] | None = None, help_text: str = "") -> None:
        with self._lock:
            self._help.setdefault(name, help_text or f"{name} counter")
            key = (name, self._label_key(labels))
            self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None, help_text: str = "") -> None:
        with self._lock:
            self._help.setdefault(name, help_text or f"{name} gauge")
            self._gauges[(name, self._label_key(labels))] = value

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None, help_text: str = "") -> None:
        with self._lock:
            self._help.setdefault(name, help_text or f"{name} histogram")
            key = name if not labels else f"{name}:{self._label_key(labels)}"
            self._histograms.setdefault(key, []).append(value)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": {f"{name}|{labels}": v for (name, labels), v in self._counters.items()},
                "gauges": {f"{name}|{labels}": v for (name, labels), v in self._gauges.items()},
                "histograms": {k: list(v) for k, v in self._histograms.items()},
            }

    def render_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            for (name, labels), value in sorted(self._counters.items()):
                lines.append(f"# TYPE {name} counter")
                lines.append(f"{name}{_fmt_labels(labels)} {_fmt_num(value)}")
            for (name, labels), value in sorted(self._gauges.items()):
                lines.append(f"# TYPE {name} gauge")
                lines.append(f"{name}{_fmt_labels(labels)} {_fmt_num(value)}")
            for key, values in sorted(self._histograms.items()):
                base = key.split(":", 1)[0]
                if not values:
                    continue
                ordered = sorted(values)
                lines.append(f"# TYPE {base} summary")
                for quantile in (0.5, 0.95, 0.99):
                    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * quantile)))
                    lines.append(f"{base}_quantile{{quantile=\"{quantile}\"}} {_fmt_num(ordered[idx])}")
                lines.append(f"{base}_count {len(ordered)}")
        return "\n".join(lines) + "\n"


def _fmt_labels(labels: Sequence[tuple[str, str]]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in labels)
    return "{" + inner + "}"


def _fmt_num(value: float) -> str:
    return repr(float(value))


METRICS = Metrics()
TRACER = Tracer("plexus")
