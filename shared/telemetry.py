from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import traceback
from typing import Any, Iterator

try:
    import tracemalloc
except ImportError:  # pragma: no cover
    tracemalloc = None

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


_CURRENT_TELEMETRY: "TelemetryCollector | None" = None


def set_current_telemetry(collector: "TelemetryCollector | None") -> None:
    global _CURRENT_TELEMETRY
    _CURRENT_TELEMETRY = collector


def get_current_telemetry() -> "TelemetryCollector | None":
    return _CURRENT_TELEMETRY


def clear_current_telemetry() -> None:
    set_current_telemetry(None)


def build_default_telemetry_path(base_folder: Path, run_name: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(base_folder) / "telemetry" / f"{run_name}_{timestamp}.json"


class TelemetryCollector:
    def __init__(self, run_name: str, output_path: Path | None = None):
        self.run_name = run_name
        self.output_path = Path(output_path) if output_path else None
        self.started_at = self._utc_now()
        self.ended_at: str | None = None
        self._started_perf = time.perf_counter()
        self.metadata: dict[str, Any] = {}
        self.counters: dict[str, int | float] = {}
        self.gauges: dict[str, Any] = {}
        self.schemas: dict[str, dict[str, Any]] = {}
        self.stages: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []
        self.status = "RUNNING"
        self.max_process_rss_bytes = 0
        self.max_python_heap_bytes = 0
        self._start_memory_tracking()

    def set_metadata(self, **kwargs: Any) -> None:
        self.metadata.update(kwargs)

    def increment(self, name: str, value: int | float = 1, schema: str | None = None) -> None:
        self.counters[name] = self.counters.get(name, 0) + value
        if schema:
            schema_entry = self._get_schema_entry(schema)
            counters = schema_entry.setdefault("counters", {})
            counters[name] = counters.get(name, 0) + value

    def set_gauge(self, name: str, value: Any, schema: str | None = None) -> None:
        self.gauges[name] = value
        if schema:
            schema_entry = self._get_schema_entry(schema)
            gauges = schema_entry.setdefault("gauges", {})
            gauges[name] = value

    def observe_max(self, name: str, value: int | float, schema: str | None = None) -> None:
        current = self.gauges.get(name)
        if current is None or value > current:
            self.gauges[name] = value
        if schema:
            schema_entry = self._get_schema_entry(schema)
            gauges = schema_entry.setdefault("gauges", {})
            current_schema = gauges.get(name)
            if current_schema is None or value > current_schema:
                gauges[name] = value

    @contextmanager
    def stage(
        self,
        name: str,
        *,
        schema: str | None = None,
        table: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        started_at = self._utc_now()
        started_perf = time.perf_counter()
        status = "SUCCESS"
        try:
            yield
        except Exception as exc:
            status = "FAILED"
            self.record_failure(stage=name, exc=exc, schema=schema, table=table, extra=extra)
            raise
        finally:
            duration_ms = round((time.perf_counter() - started_perf) * 1000, 3)
            memory_snapshot = self.capture_memory()
            self.stages.append(
                {
                    "name": name,
                    "schema": schema,
                    "table": table,
                    "status": status,
                    "started_at": started_at,
                    "ended_at": self._utc_now(),
                    "duration_ms": duration_ms,
                    "process_rss_bytes": memory_snapshot["process_rss_bytes"],
                    "python_heap_peak_bytes": memory_snapshot["python_heap_peak_bytes"],
                    "extra": extra or {},
                }
            )
            self.increment(f"stage.{name}.count", schema=schema)
            self.increment(f"stage.{name}.duration_ms_total", duration_ms, schema=schema)
            self.observe_max("max_process_rss_bytes", memory_snapshot["process_rss_bytes"], schema=schema)
            self.observe_max("max_python_heap_bytes", memory_snapshot["python_heap_peak_bytes"], schema=schema)

    def record_failure(
        self,
        *,
        stage: str,
        exc: Exception,
        schema: str | None = None,
        table: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.failures.append(
            {
                "stage": stage,
                "schema": schema,
                "table": table,
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "traceback": "".join(traceback.format_exception_only(type(exc), exc)).strip(),
                "timestamp": self._utc_now(),
                "extra": extra or {},
            }
        )
        self.increment("technical_failures_total", schema=schema)
        self.increment(f"technical_failures.{stage}", schema=schema)

    def capture_memory(self) -> dict[str, int]:
        process_rss_bytes = 0
        if psutil is not None:
            try:
                process_rss_bytes = int(psutil.Process().memory_info().rss)
            except Exception:  # pragma: no cover
                process_rss_bytes = 0

        python_heap_peak_bytes = 0
        if tracemalloc is not None and tracemalloc.is_tracing():
            _, python_heap_peak_bytes = tracemalloc.get_traced_memory()

        self.max_process_rss_bytes = max(self.max_process_rss_bytes, process_rss_bytes)
        self.max_python_heap_bytes = max(self.max_python_heap_bytes, python_heap_peak_bytes)
        return {
            "process_rss_bytes": process_rss_bytes,
            "python_heap_peak_bytes": python_heap_peak_bytes,
        }

    def finalize(self, status: str | None = None) -> dict[str, Any]:
        self.ended_at = self._utc_now()
        self.capture_memory()
        self.status = status or ("FAILED" if self.failures else "SUCCESS")
        self.set_gauge("total_duration_ms", round((time.perf_counter() - self._started_perf) * 1000, 3))
        self.set_gauge("max_process_rss_bytes", self.max_process_rss_bytes)
        self.set_gauge("max_python_heap_bytes", self.max_python_heap_bytes)
        payload = self.to_dict()
        if self.output_path is not None:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return payload

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_name": self.run_name,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "metadata": self.metadata,
            "counters": self.counters,
            "gauges": self.gauges,
            "schemas": self.schemas,
            "stages": self.stages,
            "failures": self.failures,
        }

    def _get_schema_entry(self, schema: str) -> dict[str, Any]:
        key = str(schema).upper()
        if key not in self.schemas:
            self.schemas[key] = {"counters": {}, "gauges": {}}
        return self.schemas[key]

    def _start_memory_tracking(self) -> None:
        if tracemalloc is not None and not tracemalloc.is_tracing():
            tracemalloc.start()

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
