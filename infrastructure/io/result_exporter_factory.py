from __future__ import annotations

from dataquality.infrastructure.io.secure_credentials import DatabaseConnectionSettings


def build_result_exporter(
    connection_settings: DatabaseConnectionSettings,
    schema: str,
    output_type: str | None = None,
):
    """Return an Oracle or PostgreSQL result exporter based on driver class name or output_type.

    Detection priority:
    1. `output_type` explicitly set to "postgresql" / "postgres"
    2. `connection_settings.driver_class_name` contains "postgresql" / "postgres"
    3. `connection_settings.connection_uri` starts with "postgresql" / "postgres"
    4. Falls back to Oracle
    """
    driver = str(connection_settings.driver_class_name or "").lower()
    uri = str(connection_settings.connection_uri or "").lower()
    otype = str(output_type or "").strip().lower()

    is_postgres = (
        "postgres" in otype
        or "postgres" in driver
        or uri.startswith("postgresql")
        or uri.startswith("postgres")
    )

    if is_postgres:
        from dataquality.infrastructure.io.postgresql.result_exporter import PostgreSQLResultExporter
        return PostgreSQLResultExporter(connection_settings, schema=schema)

    from dataquality.infrastructure.io.oracle.result_exporter import OracleResultExporter
    return OracleResultExporter(connection_settings, schema=schema)
