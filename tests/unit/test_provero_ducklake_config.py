from types import SimpleNamespace

from autoloader import provero_ducklake


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        postgres=SimpleNamespace(
            ducklake_connection_string=(
                "dbname=test_db host=test_host port=5433 user=test_user password=test_pass"
            )
        ),
        minio=SimpleNamespace(
            endpoint="test-minio:9000",
            access_key="access",
            secret_key="secret",
            bucket="test-bucket",
            secure=True,
        ),
    )


def test_connector_uses_shared_settings_when_provero_connection_is_absent(
    monkeypatch,
) -> None:
    monkeypatch.setattr(provero_ducklake, "load_settings", _settings)
    monkeypatch.delenv("DUCKLAKE_DSN", raising=False)
    monkeypatch.delenv("DUCKLAKE_DATA_PATH", raising=False)

    connector = provero_ducklake.DuckLakeConnector()

    assert connector.catalog_dsn == (
        "ducklake:postgres:dbname=test_db host=test_host port=5433 "
        "user=test_user password=test_pass"
    )
    assert connector.data_path == "s3://test-bucket"
    assert connector.minio.endpoint == "test-minio:9000"


def test_connector_prefers_runtime_dsn_over_yaml_connection(monkeypatch) -> None:
    monkeypatch.setattr(provero_ducklake, "load_settings", _settings)
    monkeypatch.setenv("DUCKLAKE_DSN", "ducklake:postgres:runtime")

    connector = provero_ducklake.DuckLakeConnector(
        connection_string="ducklake:postgres:yaml"
    )

    assert connector.catalog_dsn == "ducklake:postgres:runtime"


def test_connector_rejects_invalid_catalog_alias(monkeypatch) -> None:
    monkeypatch.setattr(provero_ducklake, "load_settings", _settings)
    monkeypatch.setenv("DUCKLAKE_ALIAS", "catalog1; DROP TABLE x")

    try:
        provero_ducklake.DuckLakeConnector()
    except ValueError as error:
        assert "alias không hợp lệ" in str(error)
    else:
        raise AssertionError("invalid identifier must be rejected")
