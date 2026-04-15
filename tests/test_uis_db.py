"""Tests for uis_db: DB lifecycle, TTL, and build logic."""

from datetime import datetime, timedelta, timezone

import pandas as pd

from unesco_mcp import uis_db
from unesco_mcp.uis_db import (
    build_db,
    ensure_fresh,
    init_db,
    is_db_fresh,
    query,
    teardown_db,
    _get_connection,
)


# ── teardown_db ──────────────────────────────────────────────────────────────


class TestTeardownDb:
    def test_removes_existing_db(self):
        uis_db.DB_PATH.touch()
        assert uis_db.DB_PATH.exists()
        teardown_db()
        assert not uis_db.DB_PATH.exists()

    def test_no_error_when_missing(self):
        assert not uis_db.DB_PATH.exists()
        teardown_db()  # should not raise


# ── init_db ──────────────────────────────────────────────────────────────────


EXPECTED_TABLES = {
    "indicators",
    "themes",
    "geo_units",
    "geo_units_fts",
    "indicators_fts",
    "db_meta",
    "disaggregation_types",
    "disaggregation_values",
    "indicator_disaggregations",
}


class TestInitDb:
    def test_creates_all_tables(self):
        init_db()
        rows = query(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual table')"
        )
        table_names = {r["name"] for r in rows}
        assert EXPECTED_TABLES.issubset(table_names)

    def test_idempotent(self):
        init_db()
        init_db()  # second call should not raise
        rows = query(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual table')"
        )
        table_names = {r["name"] for r in rows}
        assert EXPECTED_TABLES.issubset(table_names)


# ── is_db_fresh ──────────────────────────────────────────────────────────────


class TestIsDbFresh:
    def test_false_when_no_db_file(self):
        assert not uis_db.DB_PATH.exists()
        assert is_db_fresh() is False

    def test_false_when_no_built_at(self):
        init_db()
        assert is_db_fresh() is False

    def test_true_when_within_ttl(self):
        init_db()
        now = datetime.now(timezone.utc).isoformat()
        with _get_connection() as conn:
            conn.execute(
                "INSERT INTO db_meta (key, value) VALUES (?, ?)",
                ("built_at", now),
            )
        assert is_db_fresh() is True

    def test_false_when_past_ttl(self):
        init_db()
        old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        with _get_connection() as conn:
            conn.execute(
                "INSERT INTO db_meta (key, value) VALUES (?, ?)",
                ("built_at", old),
            )
        assert is_db_fresh() is False

    def test_true_at_boundary(self):
        init_db()
        almost_stale = (datetime.now(timezone.utc) - timedelta(hours=23)).isoformat()
        with _get_connection() as conn:
            conn.execute(
                "INSERT INTO db_meta (key, value) VALUES (?, ?)",
                ("built_at", almost_stale),
            )
        assert is_db_fresh() is True

    def test_false_on_empty_db_file(self):
        uis_db.DB_PATH.touch()  # 0-byte file, no tables
        assert is_db_fresh() is False

    def test_false_on_corrupted_db(self):
        uis_db.DB_PATH.write_bytes(b"not a sqlite database")
        assert is_db_fresh() is False

    def test_false_when_tables_missing(self):
        """DB has db_meta with fresh timestamp but is missing other tables."""
        import sqlite3
        conn = sqlite3.connect(uis_db.DB_PATH)
        conn.execute(
            "CREATE TABLE db_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO db_meta (key, value) VALUES (?, ?)",
            ("built_at", datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
        assert is_db_fresh() is False


# ── build_db ─────────────────────────────────────────────────────────────────


class TestBuildDb:
    def test_full_build_from_scratch(self, mock_uis):
        assert not uis_db.DB_PATH.exists()
        build_db()

        assert uis_db.DB_PATH.exists()

        # Tables are populated
        indicators = query("SELECT * FROM indicators")
        assert len(indicators) == 1
        assert indicators[0]["code"] == "TEST.1"

        themes = query("SELECT * FROM themes")
        assert len(themes) == 1

        geo_units = query("SELECT * FROM geo_units")
        assert len(geo_units) == 1
        assert geo_units[0]["code"] == "KEN"

        # Timestamp recorded
        meta = query("SELECT value FROM db_meta WHERE key = 'built_at'")
        assert len(meta) == 1
        built_at = datetime.fromisoformat(meta[0]["value"])
        assert (datetime.now(timezone.utc) - built_at).total_seconds() < 5

    def test_skips_when_fresh(self, mock_uis, mocker):
        build_db()

        spy = mocker.patch(
            "unesco_mcp.uis_db.store_indicators", wraps=None
        )
        build_db()  # TTL is fresh, should skip
        spy.assert_not_called()

    def test_rebuilds_when_stale(self, mock_uis, mocker):
        build_db()

        # Age the timestamp past the TTL
        old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        with _get_connection() as conn:
            conn.execute(
                "UPDATE db_meta SET value = ? WHERE key = 'built_at'",
                (old,),
            )

        spy = mocker.patch(
            "unesco_mcp.uis_db.store_indicators",
            wraps=None,
        )
        mocker.patch("unesco_mcp.uis_db.store_themes")
        mocker.patch("unesco_mcp.uis_db.store_geo_units")
        mocker.patch("unesco_mcp.uis_db.get_disaggregations", return_value={})
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_types")
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_values")
        mocker.patch("unesco_mcp.uis_db.store_indicator_disaggregations")

        build_db()
        spy.assert_called_once()

    def test_fresh_forces_rebuild(self, mock_uis, mocker):
        build_db()

        spy = mocker.patch(
            "unesco_mcp.uis_db.store_indicators",
            wraps=None,
        )
        mocker.patch("unesco_mcp.uis_db.store_themes")
        mocker.patch("unesco_mcp.uis_db.store_geo_units")
        mocker.patch("unesco_mcp.uis_db.get_disaggregations", return_value={})
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_types")
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_values")
        mocker.patch("unesco_mcp.uis_db.store_indicator_disaggregations")

        build_db(fresh=True)  # should rebuild even though TTL is fresh
        spy.assert_called_once()

    def test_no_stale_data_after_rebuild(self, mock_uis, monkeypatch):
        build_db()
        indicators_before = query("SELECT code FROM indicators")
        assert indicators_before[0]["code"] == "TEST.1"

        # Swap all mocks to a completely different data set
        new_df = pd.DataFrame([
            {
                "indicatorCode": "NEW.1",
                "name": "New Indicator",
                "theme": "SCIENCE",
                "lastDataUpdate": "2025-06-01",
                "timeLine_min": 2010,
                "timeLine_max": 2024,
                "totalRecordCount": 50,
                "geoUnitType": "NATIONAL",
            },
        ])
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.available_indicators", lambda: new_df
        )
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.available_themes",
            lambda raw=False: [{"theme": "SCIENCE", "lastUpdate": "2025-06-01", "lastUpdateDescription": "New"}],
        )
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.api.get_indicators",
            lambda disaggregations=False: [
                {
                    "indicatorCode": "NEW.1",
                    "disaggregations": [
                        {
                            "disaggregationType": {"code": "SEX", "name": "Sex"},
                            "code": "F",
                            "name": "Female",
                            "glossaryTerms": [{"definition": "Female population"}],
                        },
                    ],
                },
            ],
        )

        build_db(fresh=True)

        indicators_after = query("SELECT code FROM indicators")
        codes = [r["code"] for r in indicators_after]
        assert codes == ["NEW.1"]
        assert "TEST.1" not in codes


# ── ensure_fresh ─────────────────────────────────────────────────────────────


class TestEnsureFresh:
    def test_skips_when_fresh(self, mock_uis, mocker):
        build_db(fresh=True)

        spy = mocker.patch("unesco_mcp.uis_db.store_indicators", wraps=None)
        ensure_fresh()  # DB was just built, should be within TTL
        spy.assert_not_called()

    def test_rebuilds_when_stale(self, mock_uis, mocker):
        build_db(fresh=True)

        # Age the timestamp past the TTL
        old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        with _get_connection() as conn:
            conn.execute(
                "UPDATE db_meta SET value = ? WHERE key = 'built_at'",
                (old,),
            )

        spy = mocker.patch("unesco_mcp.uis_db.store_indicators", wraps=None)
        mocker.patch("unesco_mcp.uis_db.store_themes")
        mocker.patch("unesco_mcp.uis_db.store_geo_units")
        mocker.patch("unesco_mcp.uis_db.get_disaggregations", return_value={})
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_types")
        mocker.patch("unesco_mcp.uis_db.store_disaggregation_values")
        mocker.patch("unesco_mcp.uis_db.store_indicator_disaggregations")

        ensure_fresh()
        spy.assert_called_once()
