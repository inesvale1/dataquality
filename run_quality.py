from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    package_parent = Path(__file__).resolve().parent.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))

from dataquality.app.use_cases.run_data_quality import RunDataQualityOptions, run_data_quality
from dataquality.app.use_cases.run_model_quality import RunOptions, run_model_quality
from dataquality.domain.config.llm_comment_config import LLMCommentConfig
from dataquality.shared.runtime_config import (
    build_data_quality_config_template,
    build_llm_comment_config,
    build_model_quality_config_template,
    build_quality_config_template,
    build_validation_config,
    get_config_value,
    get_phase_config,
    load_json_config,
)
from dataquality.shared.telemetry import (
    TelemetryCollector,
    build_default_telemetry_path,
    clear_current_telemetry,
    set_current_telemetry,
)


def _resolve_folder(raw_path: str, local_folder_name: str) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    script_dir = Path(__file__).resolve().parent
    fallback = script_dir / local_folder_name
    if fallback.exists():
        return fallback
    return candidate


def _resolve_model_base_folder(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.exists():
        return candidate
    script_dir = Path(__file__).resolve().parent
    inputs_fallback = script_dir / "schema" / "inputs"
    fallback = script_dir / "schema"
    if candidate == Path("dataquality\\schema\\inputs") and inputs_fallback.exists():
        return inputs_fallback
    if candidate == Path("dataquality\\schema") and fallback.exists():
        return fallback
    return candidate


def _parse_bool(raw_value: str | bool) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "on", "y", "s", "verdade"}:
        return True
    if value in {"0", "false", "no", "off", "n", "falso"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw_value}")


def _build_telemetry_output_path(
    raw_output: str | bool | None,
    run_name: str,
    multiple_phases: bool,
) -> Path:
    telemetry_folder = Path(__file__).resolve().parent / "app"
    if not raw_output:
        return build_default_telemetry_path(telemetry_folder, run_name)

    output_path = Path(str(raw_output))
    if not multiple_phases:
        return output_path

    suffix = "_model" if run_name == "telemetry_model_quality" else "_data"
    stem = output_path.stem + suffix
    return output_path.with_name(stem + output_path.suffix)


def _run_model_phase(config: dict[str, object], multiple_phases: bool) -> None:
    template_config = build_model_quality_config_template()
    phase_config = get_phase_config(config, "model_quality")

    base_folder = _resolve_model_base_folder(str(get_config_value(phase_config, "base_folder", template_config["base_folder"])))
    validation_config = build_validation_config(get_config_value(phase_config, "validation_config", None))
    metadata_source = str(get_config_value(phase_config, "metadata_source", template_config.get("metadata_source", "csv")))
    llm_comment_raw = get_config_value(
        phase_config,
        "llm_comment_generation",
        template_config.get("llm_comment_generation"),
    )
    llm_comment_config = build_llm_comment_config(llm_comment_raw)
    telemetry_enabled = _parse_bool(get_config_value(phase_config, "telemetry_enabled", template_config.get("telemetry_enabled", False)))
    telemetry_output = get_config_value(phase_config, "telemetry_output", template_config.get("telemetry_output"))
    save_context_json = _parse_bool(get_config_value(phase_config, "save_context_json", template_config.get("save_context_json", True)))

    opts = RunOptions(
        base_folder=base_folder,
        columns_to_delete=list(get_config_value(phase_config, "delete_cols", template_config["delete_cols"])),
        plural_table_exceptions=list(get_config_value(phase_config, "plural_exceptions", template_config["plural_exceptions"])),
        validation_config=validation_config,
        db_type=str(get_config_value(phase_config, "db_type", template_config["db_type"])),
        exclude_tables=list(get_config_value(phase_config, "exclude_tables", template_config["exclude_tables"])),
        llm_comment_config=llm_comment_config or LLMCommentConfig(),
        context_output_dir=Path(__file__).resolve().parent / "config",
        save_context_json=save_context_json,
        metadata_source_type=metadata_source,
        db_connection_uri=get_config_value(phase_config, "db_connection_uri", template_config.get("db_connection_uri")),
        db_driver_class_name=get_config_value(phase_config, "db_driver_class_name", template_config.get("db_driver_class_name")),
        db_username=get_config_value(phase_config, "db_username", template_config.get("db_username")),
        db_host=get_config_value(phase_config, "db_host", template_config.get("db_host")),
        db_port=_optional_int(get_config_value(phase_config, "db_port", template_config.get("db_port"))),
        db_service_name=get_config_value(phase_config, "db_service_name", template_config.get("db_service_name")),
        db_sid=get_config_value(phase_config, "db_sid", template_config.get("db_sid")),
        db_dsn=get_config_value(phase_config, "db_dsn", template_config.get("db_dsn")),
        db_password_keyring_service=get_config_value(
            phase_config,
            "db_password_keyring_service",
            template_config.get("db_password_keyring_service"),
        ),
        db_password_keyring_username=get_config_value(
            phase_config,
            "db_password_keyring_username",
            template_config.get("db_password_keyring_username"),
        ),
        metadata_db_schemas=list(get_config_value(phase_config, "metadata_db_schemas", template_config.get("metadata_db_schemas", [])) or []),
        metadata_query_template=get_config_value(phase_config, "metadata_query_template", template_config.get("metadata_query_template")),
        metadata_s3_uri=get_config_value(phase_config, "metadata_s3_uri", template_config.get("metadata_s3_uri")),
        s3_storage_options=dict(get_config_value(phase_config, "s3_storage_options", template_config.get("s3_storage_options", {})) or {}),
        include_schemas=list(get_config_value(phase_config, "include_schemas", template_config.get("include_schemas", [])) or []) or None,
        regenerate_context=_parse_bool(get_config_value(phase_config, "regenerate_context", template_config.get("regenerate_context", True))),
    )

    print("=== Model Quality ===")
    print("Saving to:", base_folder)

    if telemetry_enabled:
        telemetry_path = _build_telemetry_output_path(telemetry_output, "telemetry_model_quality", multiple_phases)
        collector = TelemetryCollector(run_name="model_quality", output_path=telemetry_path)
        collector.set_metadata(
            entrypoint="run_quality.py",
            phase="model_quality",
            db_type=opts.db_type,
            base_folder=str(base_folder),
            metadata_source=metadata_source,
        )
        set_current_telemetry(collector)
    else:
        telemetry_path = None
        collector = None
        set_current_telemetry(None)

    try:
        run_model_quality(opts)
        payload = collector.finalize("SUCCESS") if collector is not None else {"status": "SUCCESS"}
    except Exception:
        payload = collector.finalize("FAILED") if collector is not None else {"status": "FAILED"}
        raise
    finally:
        clear_current_telemetry()

    if telemetry_path is not None:
        print("Telemetry saved to:", telemetry_path)
    else:
        print("Telemetry disabled")
    telemetry_status = payload.get("run_summary", {}).get("status", payload.get("status", "UNKNOWN"))
    print("Telemetry status:", telemetry_status)


def _run_data_phase(config: dict[str, object], multiple_phases: bool) -> None:
    template_config = build_data_quality_config_template()
    phase_config = get_phase_config(config, "data_quality")

    metadata_source = str(get_config_value(phase_config, "metadata_source", template_config.get("metadata_source", "csv")))
    sample_source = str(get_config_value(phase_config, "sample_source", template_config["sample_source"]))
    metadata_base_folder = _resolve_folder(
        str(get_config_value(phase_config, "metadata_base_folder", template_config["metadata_base_folder"])),
        "schema",
    )
    sample_base_folder = None
    if sample_source.strip().lower() == "csv":
        sample_base_folder = _resolve_folder(
            str(get_config_value(phase_config, "sample_base_folder", template_config["sample_base_folder"])),
            "samples",
        )

    validation_config = build_validation_config(get_config_value(phase_config, "validation_config", None))
    telemetry_enabled = _parse_bool(get_config_value(phase_config, "telemetry_enabled", template_config.get("telemetry_enabled", False)))
    telemetry_output = get_config_value(phase_config, "telemetry_output", template_config.get("telemetry_output"))

    opts = RunDataQualityOptions(
        metadata_base_folder=metadata_base_folder,
        sample_base_folder=sample_base_folder,
        columns_to_delete=list(get_config_value(phase_config, "delete_cols", template_config["delete_cols"])),
        plural_table_exceptions=list(get_config_value(phase_config, "plural_exceptions", template_config["plural_exceptions"])),
        validation_config=validation_config,
        db_type=str(get_config_value(phase_config, "db_type", template_config["db_type"])),
        exclude_tables=list(get_config_value(phase_config, "exclude_tables", template_config["exclude_tables"])),
        metadata_source_type=metadata_source,
        sample_source_type=sample_source,
        db_connection_uri=get_config_value(phase_config, "db_connection_uri", template_config.get("db_connection_uri")),
        db_authentication_type=str(get_config_value(phase_config, "db_authentication_type", template_config["db_authentication_type"])),
        db_driver_class_name=get_config_value(phase_config, "db_driver_class_name", template_config.get("db_driver_class_name")),
        db_username=get_config_value(phase_config, "db_username", template_config.get("db_username")),
        db_host=get_config_value(phase_config, "db_host", template_config.get("db_host")),
        db_port=_optional_int(get_config_value(phase_config, "db_port", template_config.get("db_port"))),
        db_service_name=get_config_value(phase_config, "db_service_name", template_config.get("db_service_name")),
        db_sid=get_config_value(phase_config, "db_sid", template_config.get("db_sid")),
        db_dsn=get_config_value(phase_config, "db_dsn", template_config.get("db_dsn")),
        db_password_keyring_service=get_config_value(
            phase_config,
            "db_password_keyring_service",
            template_config.get("db_password_keyring_service"),
        ),
        db_password_keyring_username=get_config_value(
            phase_config,
            "db_password_keyring_username",
            template_config.get("db_password_keyring_username"),
        ),
        metadata_db_schemas=list(get_config_value(phase_config, "metadata_db_schemas", template_config.get("metadata_db_schemas", [])) or []),
        metadata_query_template=get_config_value(phase_config, "metadata_query_template", template_config.get("metadata_query_template")),
        metadata_s3_uri=get_config_value(phase_config, "metadata_s3_uri", template_config.get("metadata_s3_uri")),
        sample_s3_uri=get_config_value(phase_config, "sample_s3_uri", template_config.get("sample_s3_uri")),
        s3_storage_options=dict(get_config_value(phase_config, "s3_storage_options", template_config.get("s3_storage_options", {})) or {}),
        sample_query_template=get_config_value(phase_config, "sample_query_template", template_config.get("sample_query_template")),
        sample_limit=int(get_config_value(phase_config, "sample_limit", template_config["sample_limit"])),
        include_schemas=list(get_config_value(phase_config, "include_schemas", template_config.get("include_schemas", [])) or []) or None,
        skip_document_code_analysis=_parse_bool(get_config_value(phase_config, "skip_document_code_analysis", template_config.get("skip_document_code_analysis", False))),
    )

    print("=== Data Quality ===")
    print("Metadata folder:", metadata_base_folder)
    if sample_base_folder is not None:
        print("Sample folder:", sample_base_folder)
    print("Metadata source:", metadata_source)
    print("Sample source:", sample_source)

    if telemetry_enabled:
        telemetry_path = _build_telemetry_output_path(telemetry_output, "telemetry_data_quality", multiple_phases)
        collector = TelemetryCollector(run_name="data_quality", output_path=telemetry_path)
        collector.set_metadata(
            entrypoint="run_quality.py",
            phase="data_quality",
            metadata_source=metadata_source,
            sample_source=sample_source,
            db_type=opts.db_type,
            metadata_base_folder=str(metadata_base_folder),
            sample_base_folder=str(sample_base_folder) if sample_base_folder is not None else None,
        )
        set_current_telemetry(collector)
    else:
        telemetry_path = None
        collector = None
        set_current_telemetry(None)

    try:
        run_data_quality(opts)
        payload = collector.finalize("SUCCESS") if collector is not None else {"status": "SUCCESS"}
    except Exception:
        payload = collector.finalize("FAILED") if collector is not None else {"status": "FAILED"}
        raise
    finally:
        clear_current_telemetry()

    if telemetry_path is not None:
        print("Telemetry saved to:", telemetry_path)
    else:
        print("Telemetry disabled")
    telemetry_status = payload.get("run_summary", {}).get("status", payload.get("status", "UNKNOWN"))
    print("Telemetry status:", telemetry_status)


def main() -> None:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config-json", default=None, type=str, help="Path to a unified JSON file with model/data quality settings.")
    bootstrap_args, _ = bootstrap.parse_known_args()
    template_config = build_quality_config_template()
    json_config = load_json_config(bootstrap_args.config_json)

    parser = argparse.ArgumentParser(description="Run model quality, data quality, or both using a single JSON config.")
    parser.add_argument("--config-json", default=None, type=str, help="Path to a unified JSON file with model/data quality settings.")
    parser.add_argument("--print-config-template", action="store_true", help="Print a JSON template with supported input arguments and exit.")
    parser.add_argument(
        "--run-model-quality",
        default=get_config_value(json_config, "run_model_quality", template_config.get("run_model_quality", True)),
        type=_parse_bool,
        help="Enable or disable the model quality phase. Use true/false.",
    )
    parser.add_argument(
        "--run-data-quality",
        default=get_config_value(json_config, "run_data_quality", template_config.get("run_data_quality", False)),
        type=_parse_bool,
        help="Enable or disable the data quality phase. Use true/false.",
    )
    args = parser.parse_args()

    if args.print_config_template:
        print(json.dumps(template_config, indent=2))
        return

    if not args.run_model_quality and not args.run_data_quality:
        raise ValueError("At least one phase must be enabled: run_model_quality or run_data_quality.")

    multiple_phases = bool(args.run_model_quality and args.run_data_quality)
    if args.run_model_quality:
        _run_model_phase(json_config, multiple_phases)
    if args.run_data_quality:
        _run_data_phase(json_config, multiple_phases)


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


if __name__ == "__main__":
    main()
