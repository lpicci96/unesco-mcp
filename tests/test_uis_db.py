"""Tests for uis_db: DB lifecycle, TTL, build logic, search, and query functions."""

from datetime import datetime, timedelta, timezone

import pandas as pd

from unesco_mcp import uis_db
from unesco_mcp.uis_db import (
    build_db,
    count_indicators,
    ensure_fresh,
    get_indicator_summaries,
    get_themes,
    init_db,
    is_db_fresh,
    query,
    search_geo_units,
    search_indicators,
    store_geo_units,
    store_indicators,
    store_themes,
    teardown_db,
    _get_connection,
)


# ── Helper: populate a built DB for query tests ─────────────────────────────


def _build_test_db(mock_uis):
    """Build a fully populated test DB using the mock_uis fixture's data."""
    build_db(fresh=True)


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


# ── get_themes ───────────────────────────────────────────────────────────────


class TestGetThemes:
    def test_returns_stored_themes(self, mock_uis):
        build_db(fresh=True)
        themes = get_themes()
        assert len(themes) == 1
        assert themes[0]["code"] == "EDUCATION"
        assert "name" in themes[0]

    def test_empty_when_no_themes(self):
        init_db()
        assert get_themes() == []

    def test_ordered_by_name(self, monkeypatch, mock_uis):
        """Themes should come back sorted by name."""
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.available_themes",
            lambda raw=False: [
                {"theme": "SCIENCE_TECHNOLOGY", "lastUpdate": None, "lastUpdateDescription": None},
                {"theme": "EDUCATION", "lastUpdate": None, "lastUpdateDescription": None},
            ],
        )
        build_db(fresh=True)
        themes = get_themes()
        names = [t["name"] for t in themes]
        assert names == sorted(names)


# ── store_themes name derivation ─────────────────────────────────────────────


class TestStoreThemesNameDerivation:
    """Test the theme code → human-readable name logic."""

    def test_single_word_theme(self, mock_uis):
        build_db(fresh=True)
        themes = get_themes()
        # "EDUCATION" → "Education"
        edu = [t for t in themes if t["code"] == "EDUCATION"]
        assert edu[0]["name"] == "Education"

    def test_multi_word_theme(self, monkeypatch, mock_uis):
        """SCIENCE_TECHNOLOGY → 'Science & Technology'"""
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.available_themes",
            lambda raw=False: [
                {"theme": "SCIENCE_TECHNOLOGY", "lastUpdate": None, "lastUpdateDescription": None},
            ],
        )
        build_db(fresh=True)
        themes = get_themes()
        assert themes[0]["name"] == "Science & Technology"

    def test_three_word_theme(self, monkeypatch, mock_uis):
        """CULTURE_COMMUNICATION_MEDIA → 'Culture, Communication & Media'"""
        monkeypatch.setattr(
            "unesco_mcp.uis_db.uis.available_themes",
            lambda raw=False: [
                {"theme": "CULTURE_COMMUNICATION_MEDIA", "lastUpdate": None, "lastUpdateDescription": None},
            ],
        )
        build_db(fresh=True)
        themes = get_themes()
        assert themes[0]["name"] == "Culture, Communication & Media"

    def test_indicator_count_matches(self, mock_uis):
        """Theme indicator_count should match actual indicators in that theme."""
        build_db(fresh=True)
        themes = get_themes()
        edu = [t for t in themes if t["code"] == "EDUCATION"][0]
        actual = query("SELECT COUNT(*) as cnt FROM indicators WHERE theme = 'EDUCATION'")
        assert edu["indicator_count"] == actual[0]["cnt"]


# ── search_geo_units ─────────────────────────────────────────────────────────


class TestSearchGeoUnits:
    def _populate_geo_units(self):
        """Insert test geo data directly for search tests."""
        init_db()
        with _get_connection() as conn:
            rows = [
                ("KEN", "Kenya", "NATIONAL", None),
                ("ZWE", "Zimbabwe", "NATIONAL", None),
                ("SSA_SDG", "Sub-Saharan Africa", "REGIONAL", "SDG"),
                ("SSA_WB", "Sub-Saharan Africa", "REGIONAL", "WB"),
                ("NAF_SDG", "Northern Africa", "REGIONAL", "SDG"),
            ]
            conn.executemany(
                "INSERT INTO geo_units (code, name, type, region_group) VALUES (?, ?, ?, ?)",
                rows,
            )
            conn.execute("DELETE FROM geo_units_fts")
            conn.executemany(
                "INSERT INTO geo_units_fts (code, name) VALUES (?, ?)",
                [(code, name) for code, name, *_ in rows],
            )

    def test_exact_code_match(self):
        self._populate_geo_units()
        results = search_geo_units("KEN")
        assert len(results) >= 1
        assert results[0]["code"] == "KEN"

    def test_exact_code_match_case_insensitive(self):
        self._populate_geo_units()
        results = search_geo_units("ken")
        assert len(results) >= 1
        assert results[0]["code"] == "KEN"

    def test_fts_name_search(self):
        self._populate_geo_units()
        results = search_geo_units("Kenya")
        assert any(r["code"] == "KEN" for r in results)

    def test_fts_partial_name(self):
        self._populate_geo_units()
        results = search_geo_units("Zimbabwe")
        assert any(r["code"] == "ZWE" for r in results)

    def test_regional_search_returns_multiple_groupings(self):
        self._populate_geo_units()
        results = search_geo_units("Sub-Saharan Africa")
        codes = {r["code"] for r in results}
        assert "SSA_SDG" in codes
        assert "SSA_WB" in codes

    def test_type_filter_national(self):
        self._populate_geo_units()
        results = search_geo_units("a", type_filter="NATIONAL")
        # Should only return national units, not regional
        for r in results:
            assert r["type"] == "NATIONAL"

    def test_type_filter_regional(self):
        self._populate_geo_units()
        results = search_geo_units("Africa", type_filter="REGIONAL")
        for r in results:
            assert r["type"] == "REGIONAL"

    def test_region_group_filter(self):
        self._populate_geo_units()
        results = search_geo_units("Sub-Saharan Africa", region_group="SDG")
        assert all(
            r["region_group"] == "SDG" or r["type"] == "NATIONAL"
            for r in results
        )
        assert any(r["code"] == "SSA_SDG" for r in results)
        assert not any(r["code"] == "SSA_WB" for r in results)

    def test_no_results(self):
        self._populate_geo_units()
        results = search_geo_units("Nonexistent Country XYZ")
        assert results == []

    def test_no_duplicates(self):
        """Code match and FTS match for the same unit should not produce duplicates."""
        self._populate_geo_units()
        results = search_geo_units("KEN")
        codes = [r["code"] for r in results]
        assert len(codes) == len(set(codes))

    def test_fts_injection_safe(self):
        """Quotes in query should not break FTS5 syntax."""
        self._populate_geo_units()
        # Should not raise — quotes are stripped from the FTS query
        results = search_geo_units('"Kenya" OR "Zimbabwe"')
        # May return results or empty, but should not error
        assert isinstance(results, list)


# ── search_indicators ────────────────────────────────────────────────────────


class TestSearchIndicators:
    def test_text_search(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(query_term="Test")
        assert total >= 1
        assert results[0]["code"] == "TEST.1"

    def test_theme_filter(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(theme="EDUCATION")
        assert total >= 1
        assert all(r["theme"] == "EDUCATION" for r in results)

    def test_theme_filter_no_match(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(theme="NONEXISTENT")
        assert total == 0
        assert results == []

    def test_limit(self, mock_uis, monkeypatch):
        """Limit should cap returned results but total_count reflects all matches."""
        df = pd.DataFrame([
            {"indicatorCode": f"IND.{i}", "name": f"Indicator {i}", "theme": "EDUCATION",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2000, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"}
            for i in range(5)
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        results, total = search_indicators(theme="EDUCATION", limit=2)
        assert total == 5
        assert len(results) == 2

    def test_no_limit_returns_all(self, mock_uis, monkeypatch):
        df = pd.DataFrame([
            {"indicatorCode": f"IND.{i}", "name": f"Indicator {i}", "theme": "EDUCATION",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2000, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"}
            for i in range(5)
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        results, total = search_indicators(theme="EDUCATION", limit=None)
        assert total == 5
        assert len(results) == 5

    def test_disaggregation_type_filter(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(disaggregation_types=["SEX"])
        assert total >= 1
        assert results[0]["code"] == "TEST.1"

    def test_disaggregation_type_filter_no_match(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(disaggregation_types=["NONEXISTENT"])
        assert total == 0

    def test_disaggregation_value_filter(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(disaggregation_values=["M"])
        assert total >= 1

    def test_combined_filters(self, mock_uis):
        build_db(fresh=True)
        results, total = search_indicators(
            theme="EDUCATION", disaggregation_types=["SEX"], query_term="Test"
        )
        assert total >= 1

    def test_no_filters_returns_all(self, mock_uis):
        """No filters should return everything."""
        build_db(fresh=True)
        results, total = search_indicators()
        assert total >= 1

    def test_fts_stemming(self, mock_uis, monkeypatch):
        """Porter stemmer should match 'completing' to 'Completion'."""
        df = pd.DataFrame([
            {"indicatorCode": "STEM.1", "name": "Completion rate indicator",
             "theme": "EDUCATION", "lastDataUpdate": "2025-01-01",
             "timeLine_min": 2000, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        results, total = search_indicators(query_term="completing")
        # "completing" stems to "complet" which should match "Completion"
        assert total >= 1


# ── count_indicators ─────────────────────────────────────────────────────────


class TestCountIndicators:
    def test_count_all(self, mock_uis):
        build_db(fresh=True)
        assert count_indicators() >= 1

    def test_count_by_theme(self, mock_uis):
        build_db(fresh=True)
        assert count_indicators(theme="EDUCATION") >= 1
        assert count_indicators(theme="NONEXISTENT") == 0

    def test_coverage_year_filters(self, mock_uis, monkeypatch):
        df = pd.DataFrame([
            {"indicatorCode": "A.1", "name": "A", "theme": "T",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2000, "timeLine_max": 2020,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
            {"indicatorCode": "B.1", "name": "B", "theme": "T",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2010, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        # coverage_start_year: timeLine_min <= year → data starts by that year
        assert count_indicators(coverage_start_year=2005) == 1  # only A (min=2000)
        assert count_indicators(coverage_start_year=2015) == 2  # both (2000<=2015 and 2010<=2015)

        # coverage_end_year: timeLine_max >= year → data extends through that year
        assert count_indicators(coverage_end_year=2022) == 1  # only B (max=2023)
        assert count_indicators(coverage_end_year=2019) == 2  # both

    def test_updated_since_filter(self, mock_uis, monkeypatch):
        df = pd.DataFrame([
            {"indicatorCode": "OLD.1", "name": "Old", "theme": "T",
             "lastDataUpdate": "2020-01-01", "timeLine_min": 2000, "timeLine_max": 2020,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
            {"indicatorCode": "NEW.1", "name": "New", "theme": "T",
             "lastDataUpdate": "2025-06-01", "timeLine_min": 2000, "timeLine_max": 2024,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        assert count_indicators(updated_since="2024-01-01") == 1  # only NEW.1
        assert count_indicators(updated_since="2019-01-01") == 2  # both


# ── get_indicator_summaries ──────────────────────────────────────────────────


class TestGetIndicatorSummaries:
    def test_returns_summary(self, mock_uis):
        build_db(fresh=True)
        summaries = get_indicator_summaries(["TEST.1"])
        assert len(summaries) == 1
        s = summaries[0]
        assert s["code"] == "TEST.1"
        assert "name" in s
        assert "theme" in s
        assert "disaggregation_types" in s
        assert isinstance(s["disaggregation_types"], list)

    def test_includes_disaggregation_types(self, mock_uis):
        build_db(fresh=True)
        summaries = get_indicator_summaries(["TEST.1"])
        # Mock data has SEX disaggregation
        assert "Sex" in summaries[0]["disaggregation_types"]

    def test_missing_code_excluded(self, mock_uis):
        build_db(fresh=True)
        summaries = get_indicator_summaries(["TEST.1", "NONEXISTENT.99"])
        assert len(summaries) == 1
        assert summaries[0]["code"] == "TEST.1"

    def test_empty_list(self, mock_uis):
        build_db(fresh=True)
        assert get_indicator_summaries([]) == []

    def test_all_missing(self, mock_uis):
        build_db(fresh=True)
        assert get_indicator_summaries(["NOPE.1", "NOPE.2"]) == []

    def test_multiple_indicators(self, mock_uis, monkeypatch):
        df = pd.DataFrame([
            {"indicatorCode": "A.1", "name": "Alpha", "theme": "T",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2000, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
            {"indicatorCode": "B.1", "name": "Beta", "theme": "T",
             "lastDataUpdate": "2025-01-01", "timeLine_min": 2000, "timeLine_max": 2023,
             "totalRecordCount": 10, "geoUnitType": "NATIONAL"},
        ])
        monkeypatch.setattr("unesco_mcp.uis_db.uis.available_indicators", lambda: df)
        monkeypatch.setattr("unesco_mcp.uis_db.uis.api.get_indicators", lambda disaggregations=False: [])
        build_db(fresh=True)

        summaries = get_indicator_summaries(["A.1", "B.1"])
        codes = {s["code"] for s in summaries}
        assert codes == {"A.1", "B.1"}


# ── query (generic) ─────────────────────────────────────────────────────────


class TestQuery:
    def test_returns_list_of_dicts(self):
        init_db()
        results = query("SELECT name FROM sqlite_master LIMIT 1")
        assert isinstance(results, list)
        if results:
            assert isinstance(results[0], dict)

    def test_parameterized_query(self, mock_uis):
        build_db(fresh=True)
        results = query("SELECT code FROM indicators WHERE theme = ?", ("EDUCATION",))
        assert len(results) >= 1

    def test_empty_result(self):
        init_db()
        results = query("SELECT * FROM indicators")
        assert results == []
