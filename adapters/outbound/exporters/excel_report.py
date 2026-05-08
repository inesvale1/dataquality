from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from openpyxl.utils import get_column_letter
from dataquality.shared.telemetry import get_current_telemetry

_EXCEL_MAX_ROWS = 1_048_576

def build_section_df(rows) -> pd.DataFrame:
    """Build a section DataFrame with either 3 or 4 columns."""
    if rows and len(rows[0]) == 4:
        columns = ["Indicator", "Category", "Description", "Value"]
    else:
        columns = ["Indicator", "Description", "Value"]
    return pd.DataFrame(rows, columns=columns)


def save_excel_report(
    base_folder: Path,
    schema_name: str,
    sections: Dict[str, pd.DataFrame],
    file_prefix: str = "issues_metadados",
) -> Path:
    """Write the Excel report for a schema.

    Output file name: issues_metadados_<schema>.xlsx in schema/outputs when the
    input metadata lives in schema/inputs. Otherwise, writes to the provided folder.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_folder = _resolve_output_folder(Path(base_folder))
    file_name_out = (
        output_folder
        / f"{file_prefix}_{schema_name}_{timestamp}.xlsx"
    )

    file_name_out.parent.mkdir(parents=True, exist_ok=True)
    telemetry = get_current_telemetry()

    data_issues_df = sections.pop("DATA_ISSUES", None)
    if data_issues_df is not None:
        csv_path = output_folder / f"{schema_name}_data_issues_{timestamp}.csv"
        data_issues_df.to_csv(csv_path, index=False, sep=";")
        print(f"[csv] Data issues ({len(data_issues_df):,} rows) saved to {csv_path}")

    if telemetry is not None:
        telemetry.increment("excel_reports_generated", schema=schema_name)
        with telemetry.stage("excel.save_report", schema=schema_name, extra={"sheet_count": len(sections)}):
            with pd.ExcelWriter(file_name_out, engine="openpyxl", mode="w") as writer:
                for sheet_name, df in sections.items():
                    df_sheet = _truncate_for_excel(sheet_name, df)
                    with telemetry.stage("excel.write_sheet", schema=schema_name, table=sheet_name, extra={"rows": int(df_sheet.shape[0]), "columns": int(df_sheet.shape[1])}):
                        df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
                    with telemetry.stage("excel.autosize_sheet", schema=schema_name, table=sheet_name):
                        _autosize_worksheet_columns(writer, sheet_name, df_sheet)
    else:
        with pd.ExcelWriter(file_name_out, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in sections.items():
                df_sheet = _truncate_for_excel(sheet_name, df)
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
                _autosize_worksheet_columns(writer, sheet_name, df_sheet)

    if telemetry is not None:
        try:
            telemetry.set_gauge("last_excel_file_size_bytes", int(file_name_out.stat().st_size), schema=schema_name)
        except OSError:
            pass


    return file_name_out


def _truncate_for_excel(sheet_name: str, df: pd.DataFrame) -> pd.DataFrame:
    limit = _EXCEL_MAX_ROWS - 1  # reserve one row for the header
    if len(df) > limit:
        print(
            f"[excel] Sheet '{sheet_name}' has {len(df):,} rows — truncated to "
            f"{limit:,} (Excel limit). Full data saved separately as CSV."
        )
        return df.iloc[:limit]
    return df


def _resolve_output_folder(base_folder: Path) -> Path:
    base_folder = Path(base_folder)
    if base_folder.name.lower() == "inputs":
        return base_folder.parent / "outputs"
    return base_folder


def _autosize_worksheet_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    worksheet = writer.sheets.get(sheet_name)
    if worksheet is None:
        return

    for idx, column_name in enumerate(df.columns, start=1):
        series = df[column_name] if column_name in df.columns else pd.Series(dtype=str)
        max_length = len(str(column_name))
        if not series.empty:
            cell_lengths = series.fillna("").astype(str).map(len)
            if not cell_lengths.empty:
                max_length = max(max_length, int(cell_lengths.max()))

        adjusted_width = min(max(max_length + 2, 10), 80)
        worksheet.column_dimensions[get_column_letter(idx)].width = adjusted_width
