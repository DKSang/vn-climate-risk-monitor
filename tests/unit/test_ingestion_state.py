import duckdb

from vn_climate_risk_monitor.ingestion.state import ensure_ingestion_state


def test_ingestion_state_schema_is_idempotent() -> None:
    connection = duckdb.connect()

    ensure_ingestion_state(connection)
    ensure_ingestion_state(connection)

    tables = {
        row[0]
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'ops'
            """
        ).fetchall()
    }
    assert tables == {"ingestion_files", "pipeline_runs"}
