from __future__ import annotations

from contextlib import nullcontext

import pandas as pd

from dataquality.shared.telemetry import get_current_telemetry


class AthenaSampleSource:
    """Load data samples from Athena tables (backed by S3/Glue).

    Issues ``SELECT * FROM "database"."table" LIMIT N`` for each candidate table.

    Requires: pip install awswrangler
    """

    def __init__(
        self,
        workgroup: str = "primary",
        s3_output: str | None = None,
        aws_region: str | None = None,
        sample_limit: int = 1000,
    ):
        self.workgroup = str(workgroup or "primary").strip()
        self.s3_output = s3_output
        self.aws_region = aws_region
        self.sample_limit = int(sample_limit)

    def get_samples_for_schema(
        self, schema_name: str, candidates_df: pd.DataFrame
    ) -> dict[str, pd.DataFrame]:
        try:
            import awswrangler as wr
        except ImportError as exc:
            raise RuntimeError(
                "Athena sample source requires awswrangler. "
                "Install it with: pip install awswrangler"
            ) from exc

        if candidates_df is None or candidates_df.empty or "TABLE_NAME" not in candidates_df.columns:
            return {}

        telemetry = get_current_telemetry()
        session = self._boto3_session()
        samples: dict[str, pd.DataFrame] = {}

        unique_tables = candidates_df["TABLE_NAME"].dropna().unique()
        with (telemetry.stage("samples.athena_load", schema=schema_name) if telemetry is not None else nullcontext()):
            for table_name in unique_tables:
                table_upper = str(table_name).strip().upper()
                sql = (
                    f'SELECT * FROM "{schema_name}"."{table_upper}" '
                    f"LIMIT {self.sample_limit}"
                )
                kwargs: dict = dict(
                    sql=sql,
                    database=schema_name,
                    workgroup=self.workgroup,
                    ctas_approach=False,
                )
                if session is not None:
                    kwargs["boto3_session"] = session
                if self.s3_output:
                    kwargs["s3_output"] = self.s3_output

                try:
                    with (telemetry.stage("athena.sample_query", schema=schema_name, table=table_upper) if telemetry is not None else nullcontext()):
                        df = wr.athena.read_sql_query(**kwargs)
                    df.columns = [str(c).strip().upper() for c in df.columns]
                    samples[table_upper] = df
                    print(f"[athena] Loaded {len(df)} rows from {schema_name}.{table_upper}")
                except Exception as exc:
                    print(f"[athena] Could not load sample for {schema_name}.{table_upper}: {exc}")

        return samples

    def _boto3_session(self):
        if not self.aws_region:
            return None
        try:
            import boto3
            return boto3.Session(region_name=self.aws_region)
        except ImportError:
            return None
