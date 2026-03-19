from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
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

try:
    import resource
except ImportError:  # pragma: no cover
    resource = None


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
        self.metadata: dict[str, Any] = {
            "telemetry_version": "2.0",
            "run_name": run_name,
            "schema_id_convention": "lowercase",
            "schema_name_convention": "uppercase",
            "collector": "TelemetryCollector",
        }
        self.run_summary: dict[str, Any] = {
            "status": "RUNNING",
            "started_at": self.started_at,
            "ended_at": None,
            "total_duration_ms": 0.0,
            "counts": {},
            "totals": {},
            "durations_ms": {},
            "peaks": {},
            "sizes": {},
            "last_observed": {},
        }
        self.schema_summaries: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self._failures: list[dict[str, Any]] = []
        self.status = "RUNNING"
        self.max_process_rss_bytes = 0
        self.max_python_heap_bytes = 0
        self._start_memory_tracking()

    def set_metadata(self, **kwargs: Any) -> None:
        self.metadata.update(kwargs)

    def increment(self, name: str, value: int | float = 1, schema: str | None = None) -> None:
        category, field_name = self._categorize_metric_name(name, is_observation=False)
        self._add_metric(self.run_summary, category, field_name, value)
        if schema:
            schema_entry = self._get_schema_entry(schema)
            self._add_metric(schema_entry, category, field_name, value)

    def set_gauge(self, name: str, value: Any, schema: str | None = None) -> None:
        category, field_name = self._categorize_metric_name(name, is_observation=True)
        if schema:
            schema_entry = self._get_schema_entry(schema)
            self._set_metric(schema_entry, category, field_name, value)
            return

        if category == "last_observed":
            self._set_metric(self.run_summary, "last_observed", name, value)
        else:
            self._set_metric(self.run_summary, category, field_name, value)

    def observe_max(self, name: str, value: int | float, schema: str | None = None) -> None:
        category, field_name = self._categorize_metric_name(name, is_observation=True)
        target_category = "peaks" if category == "last_observed" else category
        self._set_metric_max(self.run_summary, target_category, field_name, value)
        if schema:
            schema_entry = self._get_schema_entry(schema)
            self._set_metric_max(schema_entry, target_category, field_name, value)

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
        schema_ref = self._build_schema_ref(schema)
        try:
            yield
        except Exception as exc:
            status = "FAILED"
            self.record_failure(stage=name, exc=exc, schema=schema, table=table, extra=extra)
            raise
        finally:
            duration_ms = round((time.perf_counter() - started_perf) * 1000, 3)
            memory_snapshot = self.capture_memory()
            self.events.append(
                {
                    "event_type": "stage",
                    "name": name,
                    "schema_id": schema_ref["schema_id"],
                    "schema_name": schema_ref["schema_name"],
                    "table": table,
                    "status": status,
                    "started_at": started_at,
                    "ended_at": self._utc_now(),
                    "duration_ms": duration_ms,
                    "memory": {
                        "process_rss_bytes": memory_snapshot["process_rss_bytes"],
                        "python_heap_peak_bytes": memory_snapshot["python_heap_peak_bytes"],
                    },
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
        schema_ref = self._build_schema_ref(schema)
        failure = {
            "event_type": "failure",
            "name": stage,
            "schema_id": schema_ref["schema_id"],
            "schema_name": schema_ref["schema_name"],
            "table": table,
            "status": "FAILED",
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "traceback": "".join(traceback.format_exception_only(type(exc), exc)).strip(),
            "timestamp": self._utc_now(),
            "extra": extra or {},
        }
        self._failures.append(failure)
        self.events.append(failure)
        self.increment("technical_failures_total", schema=schema)
        self.increment(f"technical_failures.{stage}", schema=schema)
        if schema:
            self._get_schema_entry(schema)["status"] = "FAILED"

    def capture_memory(self) -> dict[str, int]:
        process_rss_bytes = 0
        if psutil is not None:
            try:
                process_rss_bytes = int(psutil.Process().memory_info().rss)
            except Exception:  # pragma: no cover
                process_rss_bytes = 0
        else:
            try:
                if os.name == "nt":
                    import ctypes
                    from ctypes import wintypes

                    class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                        _fields_ = [
                            ("cb", wintypes.DWORD),
                            ("PageFaultCount", wintypes.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t),
                            ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t),
                            ("PeakPagefileUsage", ctypes.c_size_t),
                        ]

                    process = ctypes.windll.kernel32.GetCurrentProcess()
                    counters = PROCESS_MEMORY_COUNTERS()
                    counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                    fn = ctypes.windll.psapi.GetProcessMemoryInfo
                    fn.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESS_MEMORY_COUNTERS), wintypes.DWORD]
                    fn.restype = wintypes.BOOL
                    if fn(process, ctypes.byref(counters), counters.cb):
                        process_rss_bytes = int(counters.WorkingSetSize)
                elif resource is not None:
                    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                    if rss > 1024:
                        process_rss_bytes = rss * 1024
                    else:
                        process_rss_bytes = rss
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
        self.status = status or ("FAILED" if self._failures else "SUCCESS")
        self.run_summary["status"] = self.status
        self.run_summary["ended_at"] = self.ended_at
        self.run_summary["total_duration_ms"] = round((time.perf_counter() - self._started_perf) * 1000, 3)
        self.observe_max("max_process_rss_bytes", self.max_process_rss_bytes)
        self.observe_max("max_python_heap_bytes", self.max_python_heap_bytes)
        self.metadata["event_count"] = len(self.events)
        self.metadata["schema_count"] = len(self.schema_summaries)
        payload = self.to_dict()
        if self.output_path is not None:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return payload

    def to_dict(self) -> dict[str, Any]:
        ordered_schema_summaries = {
            schema_id: self.schema_summaries[schema_id]
            for schema_id in sorted(self.schema_summaries)
        }
        return {
            "metadata": self.metadata,
            "run_summary": self.run_summary,
            "schema_summaries": ordered_schema_summaries,
            "events": self.events,
        }

    def _get_schema_entry(self, schema: str) -> dict[str, Any]:
        schema_ref = self._build_schema_ref(schema)
        schema_id = schema_ref["schema_id"]
        if schema_id is None:
            raise ValueError("schema_id cannot be None for schema-scoped telemetry")
        if schema_id not in self.schema_summaries:
            self.schema_summaries[schema_id] = {
                "schema_id": schema_id,
                "schema_name": schema_ref["schema_name"],
                "status": "SUCCESS",
                "counts": {},
                "totals": {},
                "durations_ms": {},
                "peaks": {},
                "sizes": {},
                "last_observed": {},
            }
        return self.schema_summaries[schema_id]

    def _build_schema_ref(self, schema: str | None) -> dict[str, str | None]:
        if schema is None:
            return {"schema_id": None, "schema_name": None}
        normalized = str(schema).strip()
        if not normalized:
            return {"schema_id": None, "schema_name": None}
        return {
            "schema_id": normalized.lower(),
            "schema_name": normalized.upper(),
        }

    def _categorize_metric_name(self, name: str, *, is_observation: bool) -> tuple[str, str]:
        if name.startswith("stage.") and name.endswith(".count"):
            return "counts", name
        if name.startswith("stage.") and name.endswith(".duration_ms_total"):
            return "durations_ms", name
        if name.endswith("_duration_ms") or name.endswith("_duration_ms_total"):
            return "durations_ms", name
        if name.startswith("max_") or name.endswith("_peak") or name.endswith("_peak_bytes"):
            return "peaks", name
        if name.endswith("_size_bytes") or name.endswith("_file_size_bytes"):
            return "sizes", name
        if (
            name.endswith("_count")
            or name.endswith("_rows")
            or name.endswith("_tables")
            or name.endswith("_columns")
            or name.endswith("_generated")
            or name.endswith("_processed")
            or name.endswith("_loaded")
            or name.endswith("_detected")
            or name.endswith("_issues")
            or name.endswith("_metrics")
        ):
            return "counts", name
        if name.endswith("_total"):
            return "totals", name
        if is_observation and name.startswith("last_"):
            return "last_observed", name
        if is_observation:
            return "last_observed", name
        return "counts", name

    def _add_metric(self, container: dict[str, Any], category: str, name: str, value: int | float) -> None:
        bucket = container.setdefault(category, {})
        bucket[name] = bucket.get(name, 0) + value

    def _set_metric(self, container: dict[str, Any], category: str, name: str, value: Any) -> None:
        bucket = container.setdefault(category, {})
        bucket[name] = value

    def _set_metric_max(self, container: dict[str, Any], category: str, name: str, value: int | float) -> None:
        bucket = container.setdefault(category, {})
        current = bucket.get(name)
        if current is None or value > current:
            bucket[name] = value

    def _start_memory_tracking(self) -> None:
        if tracemalloc is not None and not tracemalloc.is_tracing():
            tracemalloc.start()

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
