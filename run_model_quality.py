from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.use_cases.run_model_quality import RunOptions, run_model_quality
from dataquality.shared.runtime_config import (
    build_model_quality_config_template,
    build_validation_config,
    get_config_value,
    load_json_config,
)
from dataquality.shared.telemetry import (
    TelemetryCollector,
    build_default_telemetry_path,
    clear_current_telemetry,
    set_current_telemetry,
)


def _resolve_base_folder(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    script_dir = Path(__file__).resolve().parent
    fallback = script_dir / "schema"
    if candidate == Path("dataquality\\schema") and fallback.exists():
        return fallback
    return candidate


def _parse_bool(raw_value: str | bool) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "on", "y", "s", "verdade"}:
        return True
    if value in {"0", "false", "no", "off", "n"}, "falso":
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")


def main() -> None:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    bootstrap_args, _ = bootstrap.parse_known_args()
    template_config = build_model_quality_config_template()
    json_config = load_json_config(bootstrap_args.config_json)

    parser = argparse.ArgumentParser(description="Run Data Model Quality validations and metrics.")
    parser.add_argument("--config-json", default=None, type=str, help="Path to a JSON file with input arguments and validation settings.")
    parser.add_argument("--print-config-template", action="store_true", help="Print a JSON template with supported input arguments and exit.")
    parser.add_argument("--base-folder", default=get_config_value(json_config, "base_folder", template_config["base_folder"]), type=str, help="Folder that contains schema subfolders with metadados_*.csv output files.")
    parser.add_argument("--telemetry-output", default=get_config_value(json_config, "telemetry_output", template_config.get("telemetry_output")), type=str, help="Optional path to write telemetry JSON for the execution.")
    parser.add_argument("--telemetry-enabled", default=get_config_value(json_config, "telemetry_enabled", template_config.get("telemetry_enabled", False)), type=_parse_bool, help="Enable or disable telemetry JSON output. Use true/false.")
    parser.add_argument("--delete-cols", nargs="*", default=get_config_value(json_config, "delete_cols", template_config["delete_cols"]), help="Columns to drop after loading.")
    parser.add_argument("--plural-exceptions", nargs="*", default=get_config_value(json_config, "plural_exceptions", template_config["plural_exceptions"]), help="Table names allowed to end with 'S'.")
    parser.add_argument("--db-type", default=get_config_value(json_config, "db_type", template_config["db_type"]), type=str, help="Database type for DDL suggestions (e.g., Oracle).")
    parser.add_argument("--exclude-tables", nargs="*", default=get_config_value(json_config, "exclude_tables", template_config["exclude_tables"]), help="List of OWNER.TABLE or TABLE fragment to exclude from validation/metrics.")
    args = parser.parse_args()  
    if args.print_config_template:
        print(json.dumps(template_config, indent=2))
        return

    base_folder = _resolve_base_folder(args.base_folder)
    validation_config = build_validation_config(get_config_value(json_config, "validation_config", None))

    opts = RunOptions(
        base_folder=base_folder,
        columns_to_delete=args.delete_cols,
        plural_table_exceptions=args.plural_exceptions,
        validation_config=validation_config,
        db_type=args.db_type,
        exclude_tables=args.exclude_tables,
    )
    print("Saving to:", base_folder)
    if args.telemetry_enabled:
        telemetry_folder = Path(__file__).resolve().parent / "app"
        telemetry_path = Path(args.telemetry_output) if args.telemetry_output else build_default_telemetry_path(telemetry_folder, "telemetry_model_quality")
        collector = TelemetryCollector(run_name="model_quality", output_path=telemetry_path)
        collector.set_metadata(
            entrypoint="run_model_quality.py",
            db_type=args.db_type,
            base_folder=str(base_folder),
        )
        set_current_telemetry(collector)
    else:
        telemetry_path = None
        collector = None
        set_current_telemetry(None)
    try:
        run_model_quality(opts)
        if collector is not None:
            payload = collector.finalize("SUCCESS")
        else:
            payload = {"status": "SUCCESS"}
    except Exception:
        if collector is not None:
            payload = collector.finalize("FAILED")
        else:
            payload = {"status": "FAILED"}
        raise
    finally:
        clear_current_telemetry()
    if telemetry_path is not None:
        print("Telemetry saved to:", telemetry_path)
    else:
        print("Telemetry disabled")
    telemetry_status = payload.get("run_summary", {}).get("status", payload.get("status", "UNKNOWN"))
    print("Telemetry status:", telemetry_status)


if __name__ == "__main__":
    main()
