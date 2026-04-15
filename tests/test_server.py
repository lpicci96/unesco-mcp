"""Tests for server.py MCP tool functions."""

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from unesco_mcp.uis_db import build_db, init_db, query, _get_connection
from unesco_mcp.server import (
    server_status,
    list_themes,
    list_disaggregation_types,
    get_disaggregation_values,
    search_indicators,
    count_indicators,
    get_indicator_metadata,
    get_indicator_summary,
    search_geo_units,
    get_time_series,
    get_country_ranking,
    compare_geographies,
    get_latest_value,
    _resolve_geo_unit,
    _safe_qualifier,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


@dataclass
class _ElicitResult:
    action: str
    data: object = None


def _make_ctx(elicit_side_effect=None):
    """Create a mock Context with configurable elicit behavior."""
    ctx = MagicMock()
    ctx.elicit = AsyncMock(side_effect=elicit_side_effect)
    return ctx


def _make_data_df(rows):
    """Build a DataFrame mimicking uis.get_data() output."""
    return pd.DataFrame(rows)


SAMPLE_DATA_ROW = {
    "indicatorId": "CR.1",
    "name": "Completion rate",
    "geoUnit": "KEN",
    "geoUnitName": "Kenya",
    "year": 2020,
    "value": 85.5,
    "qualifier": None,
}


# ── server_status ────────────────────────────────────────────────────────────


class TestServerStatus:
    def test_returns_ok(self):
        result = server_status()
        assert result["status"] == "ok"
        assert result["server"] == "unesco-mcp"
        assert "utc_time" in result


# ── list_themes ──────────────────────────────────────────────────────────────


class TestListThemes:
    @pytest.mark.asyncio
    async def test_returns_themes(self, mock_uis):
        build_db(fresh=True)
        result = await list_themes()
        assert result["theme count"] == 1
        assert isinstance(result["theme count"], int)
        assert len(result["theme information"]) == 1


# ── list_disaggregation_types ────────────────────────────────────────────────


class TestListDisaggregationTypes:
    @pytest.mark.asyncio
    async def test_returns_types(self, mock_uis):
        build_db(fresh=True)
        result = await list_disaggregation_types()
        assert result["count"] >= 1
        assert any(d["type_code"] == "SEX" for d in result["disaggregation_types"])


# ── get_disaggregation_values ────────────────────────────────────────────────


class TestGetDisaggregationValues:
    @pytest.mark.asyncio
    async def test_valid_type(self, mock_uis):
        build_db(fresh=True)
        result = await get_disaggregation_values("SEX")
        assert result["type_code"] == "SEX"
        assert result["count"] >= 1
        assert any(v["code"] == "M" for v in result["values"])

    @pytest.mark.asyncio
    async def test_invalid_type(self, mock_uis):
        build_db(fresh=True)
        result = await get_disaggregation_values("NONEXISTENT")
        assert "error" in result


# ── search_indicators (tool) ─────────────────────────────────────────────────


class TestSearchIndicatorsTool:
    @pytest.mark.asyncio
    async def test_no_filters_returns_error(self, mock_uis):
        build_db(fresh=True)
        result = await search_indicators()
        assert "error" in result

    @pytest.mark.asyncio
    async def test_text_query(self, mock_uis):
        build_db(fresh=True)
        result = await search_indicators(query="Test")
        assert result["query_matches"] >= 1
        assert result["returned"] >= 1

    @pytest.mark.asyncio
    async def test_limit_clamping(self, mock_uis):
        """Limit below 1 should clamp to 1, above 50 to 50."""
        build_db(fresh=True)
        result = await search_indicators(query="Test", limit=0)
        # Should not error — limit clamped to 1
        assert "indicators" in result

        result = await search_indicators(query="Test", limit=999)
        # Should not error — limit clamped to 50
        assert "indicators" in result


# ── count_indicators (tool) ──────────────────────────────────────────────────


class TestCountIndicatorsTool:
    @pytest.mark.asyncio
    async def test_no_filters_counts_all(self, mock_uis):
        build_db(fresh=True)
        result = await count_indicators()
        assert result["count"] >= 1
        assert result["filters_applied"] == "none (total across all indicators)"

    @pytest.mark.asyncio
    async def test_with_theme_filter(self, mock_uis):
        build_db(fresh=True)
        result = await count_indicators(theme="EDUCATION")
        assert result["count"] >= 1
        assert result["filters_applied"]["theme"] == "EDUCATION"


# ── get_indicator_metadata ───────────────────────────────────────────────────


class TestGetIndicatorMetadata:
    @pytest.mark.asyncio
    async def test_valid_indicator(self, monkeypatch):
        monkeypatch.setattr(
            "unesco_mcp.server.uis.get_metadata",
            lambda code, **kw: [{
                "indicatorCode": "CR.1",
                "name": "Completion rate",
                "theme": "EDUCATION",
                "lastDataUpdate": "2025-01-01",
                "lastDataUpdateDescription": "Annual update",
                "dataAvailability": {
                    "timeLine": {"min": 2000, "max": 2023},
                    "totalRecordCount": 500,
                    "geoUnits": {"types": ["NATIONAL"]},
                },
                "glossaryTerms": [
                    {"name": "CR", "definition": "The completion rate", "purpose": "Measures..."}
                ],
                "disaggregations": [
                    {"code": "M", "name": "Male", "disaggregationType": {"code": "SEX", "name": "Sex"}}
                ],
            }],
        )
        result = await get_indicator_metadata("CR.1")
        assert result["code"] == "CR.1"
        assert result["definition"]["definition"] == "The completion rate"
        assert len(result["disaggregations"]) == 1

    @pytest.mark.asyncio
    async def test_not_found(self, monkeypatch):
        monkeypatch.setattr("unesco_mcp.server.uis.get_metadata", lambda code, **kw: [])
        result = await get_indicator_metadata("NONEXISTENT")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_no_glossary_terms(self, monkeypatch):
        """Metadata with no glossary terms should not crash."""
        monkeypatch.setattr(
            "unesco_mcp.server.uis.get_metadata",
            lambda code, **kw: [{
                "indicatorCode": "X.1",
                "name": "Some indicator",
                "theme": "T",
                "lastDataUpdate": None,
                "lastDataUpdateDescription": None,
                "dataAvailability": {"timeLine": {}, "geoUnits": {}},
                "glossaryTerms": [],
                "disaggregations": [],
            }],
        )
        result = await get_indicator_metadata("X.1")
        assert result["code"] == "X.1"
        assert "definition" not in result  # no glossary → no definition key


# ── get_indicator_summary (tool) ─────────────────────────────────────────────


class TestGetIndicatorSummaryTool:
    @pytest.mark.asyncio
    async def test_valid_codes(self, mock_uis):
        build_db(fresh=True)
        result = await get_indicator_summary(["TEST.1"])
        assert result["returned"] == 1
        assert result["not_found"] == []

    @pytest.mark.asyncio
    async def test_empty_list(self, mock_uis):
        build_db(fresh=True)
        result = await get_indicator_summary([])
        assert "error" in result

    @pytest.mark.asyncio
    async def test_exceeds_max(self, mock_uis):
        build_db(fresh=True)
        result = await get_indicator_summary([f"X.{i}" for i in range(20)])
        assert "error" in result

    @pytest.mark.asyncio
    async def test_partial_not_found(self, mock_uis):
        build_db(fresh=True)
        result = await get_indicator_summary(["TEST.1", "NOPE.1"])
        assert result["returned"] == 1
        assert "NOPE.1" in result["not_found"]


# ── search_geo_units (tool) ──────────────────────────────────────────────────


class TestSearchGeoUnitsTool:
    def _populate_geo(self):
        init_db()
        with _get_connection() as conn:
            rows = [
                ("KEN", "Kenya", "NATIONAL", None),
                ("SSA_SDG", "Sub-Saharan Africa", "REGIONAL", "SDG"),
                ("SSA_WB", "Sub-Saharan Africa", "REGIONAL", "WB"),
            ]
            conn.executemany(
                "INSERT INTO geo_units (code, name, type, region_group) VALUES (?, ?, ?, ?)",
                rows,
            )
            conn.execute("DELETE FROM geo_units_fts")
            conn.executemany(
                "INSERT INTO geo_units_fts (code, name) VALUES (?, ?)",
                [(c, n) for c, n, *_ in rows],
            )
            # Need db_meta with timestamp for ensure_fresh to pass
            from datetime import datetime, timezone
            conn.execute(
                "INSERT INTO db_meta (key, value) VALUES (?, ?)",
                ("built_at", datetime.now(timezone.utc).isoformat()),
            )

    @pytest.mark.asyncio
    async def test_simple_country_search(self):
        self._populate_geo()
        ctx = _make_ctx()
        result = await search_geo_units(ctx, "Kenya")
        assert result["count"] >= 1
        assert any(g["code"] == "KEN" for g in result["geo_units"])

    @pytest.mark.asyncio
    async def test_disambiguation_with_elicitation_accept(self):
        """When user accepts a grouping, results should be filtered."""
        self._populate_geo()
        ctx = _make_ctx(elicit_side_effect=[_ElicitResult("accept", "SDG")])
        result = await search_geo_units(ctx, "Sub-Saharan Africa")
        assert "geo_units" in result
        assert all(
            g["region_group"] == "SDG" or g["type"] == "NATIONAL"
            for g in result["geo_units"]
        )

    @pytest.mark.asyncio
    async def test_disambiguation_elicitation_fails(self):
        """When elicitation fails, should return disambiguation error without data."""
        self._populate_geo()
        ctx = _make_ctx(elicit_side_effect=RuntimeError("not supported"))
        result = await search_geo_units(ctx, "Sub-Saharan Africa")
        assert result["error"] == "geography_disambiguation_required"
        assert "geo_units" not in result
        assert set(result["available_groupings"]) == {"SDG", "WB"}

    @pytest.mark.asyncio
    async def test_disambiguation_user_declines(self):
        """When user declines, should return disambiguation error."""
        self._populate_geo()
        ctx = _make_ctx(elicit_side_effect=[_ElicitResult("decline")])
        result = await search_geo_units(ctx, "Sub-Saharan Africa")
        assert result["error"] == "geography_disambiguation_required"

    @pytest.mark.asyncio
    async def test_no_disambiguation_for_single_group(self):
        """When only one grouping exists, no elicitation needed."""
        init_db()
        with _get_connection() as conn:
            conn.execute(
                "INSERT INTO geo_units VALUES (?, ?, ?, ?)",
                ("NAF_SDG", "Northern Africa", "REGIONAL", "SDG"),
            )
            conn.execute("DELETE FROM geo_units_fts")
            conn.execute(
                "INSERT INTO geo_units_fts (code, name) VALUES (?, ?)",
                ("NAF_SDG", "Northern Africa"),
            )
            from datetime import datetime, timezone
            conn.execute(
                "INSERT INTO db_meta (key, value) VALUES (?, ?)",
                ("built_at", datetime.now(timezone.utc).isoformat()),
            )
        ctx = _make_ctx()
        result = await search_geo_units(ctx, "Northern Africa")
        assert "geo_units" in result
        # elicit should NOT have been called
        ctx.elicit.assert_not_called()


# ── _resolve_geo_unit ────────────────────────────────────────────────────────


class TestResolveGeoUnit:
    @pytest.mark.asyncio
    async def test_single_result(self):
        ctx = _make_ctx()
        result = await _resolve_geo_unit(
            ctx,
            [{"code": "KEN", "name": "Kenya", "type": "NATIONAL", "region_group": None}],
            "Kenya",
        )
        assert result["code"] == "KEN"
        ctx.elicit.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_results(self):
        ctx = _make_ctx()
        result = await _resolve_geo_unit(ctx, [], "Nothing")
        assert result is None

    @pytest.mark.asyncio
    async def test_disambiguation_accept(self):
        ctx = _make_ctx(elicit_side_effect=[_ElicitResult("accept", "SDG")])
        results = [
            {"code": "SSA_SDG", "name": "Sub-Saharan Africa", "type": "REGIONAL", "region_group": "SDG"},
            {"code": "SSA_WB", "name": "Sub-Saharan Africa", "type": "REGIONAL", "region_group": "WB"},
        ]
        result = await _resolve_geo_unit(ctx, results, "Sub-Saharan Africa")
        assert result["code"] == "SSA_SDG"

    @pytest.mark.asyncio
    async def test_disambiguation_fails(self):
        ctx = _make_ctx(elicit_side_effect=RuntimeError("no elicitation"))
        results = [
            {"code": "SSA_SDG", "name": "Sub-Saharan Africa", "type": "REGIONAL", "region_group": "SDG"},
            {"code": "SSA_WB", "name": "Sub-Saharan Africa", "type": "REGIONAL", "region_group": "WB"},
        ]
        result = await _resolve_geo_unit(ctx, results, "Sub-Saharan Africa")
        assert result["_disambiguation_required"] is True
        assert set(result["available_groupings"]) == {"SDG", "WB"}


# ── _safe_qualifier ──────────────────────────────────────────────────────────


class TestSafeQualifier:
    def test_none(self):
        assert _safe_qualifier({"qualifier": None}) is None

    def test_nan_string(self):
        # The literal string "nan" is a valid non-null value — pd.notna("nan") is True.
        # Only float("nan") / numpy.nan should be treated as missing.
        assert _safe_qualifier({"qualifier": "nan"}) == "nan"

    def test_nan_float(self):
        assert _safe_qualifier({"qualifier": float("nan")}) is None

    def test_valid(self):
        assert _safe_qualifier({"qualifier": "<"}) == "<"

    def test_missing_key(self):
        assert _safe_qualifier({}) is None


# ── get_time_series ──────────────────────────────────────────────────────────


class TestGetTimeSeries:
    @pytest.mark.asyncio
    async def test_basic_time_series(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "year": 2018, "value": 80.0},
            {**SAMPLE_DATA_ROW, "year": 2019, "value": 82.5},
            {**SAMPLE_DATA_ROW, "year": 2020, "value": 85.5},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_time_series(ctx, "CR.1", geo_unit_code="KEN")
        assert result["indicator_code"] == "CR.1"
        assert len(result["data_points"]) == 3
        assert result["data_points"][0]["year"] == 2018
        assert result["summary"]["total_data_points"] == 3

    @pytest.mark.asyncio
    async def test_no_data_error(self, monkeypatch):
        from unesco_reader.exceptions import NoDataError
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", MagicMock(side_effect=NoDataError()))
        ctx = _make_ctx()

        result = await get_time_series(ctx, "CR.1", geo_unit_code="KEN")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_nan_values_filtered(self, monkeypatch):
        """Rows with NaN values should be excluded from data_points."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "year": 2019, "value": 80.0},
            {**SAMPLE_DATA_ROW, "year": 2020, "value": float("nan")},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_time_series(ctx, "CR.1", geo_unit_code="KEN")
        assert len(result["data_points"]) == 1
        assert result["data_points"][0]["year"] == 2019

    @pytest.mark.asyncio
    async def test_elicitation_when_geo_omitted(self, monkeypatch, mock_uis):
        """When geo_unit_code is omitted, should elicit from user."""
        build_db(fresh=True)
        df = _make_data_df([SAMPLE_DATA_ROW])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        ctx = _make_ctx(elicit_side_effect=[
            _ElicitResult("accept", "Get global value (World)"),
        ])
        result = await get_time_series(ctx, "CR.1")
        # Should have called elicit at least once
        assert ctx.elicit.call_count >= 1


# ── get_country_ranking ──────────────────────────────────────────────────────


class TestGetCountryRanking:
    @pytest.mark.asyncio
    async def test_basic_ranking(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "geoUnitName": "Kenya", "value": 85.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "TZA", "geoUnitName": "Tanzania", "value": 75.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "UGA", "geoUnitName": "Uganda", "value": 70.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await get_country_ranking("CR.1", year=2020, strict_year=False)
        assert result["year_used"] == 2020
        assert result["top"][0]["code"] == "KEN"
        assert result["top"][0]["rank"] == 1

    @pytest.mark.asyncio
    async def test_strict_year_requires_year(self):
        result = await get_country_ranking("CR.1", year=None, strict_year=True)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_auto_year_selection(self, monkeypatch):
        """With strict_year=False and no year, should pick year with most coverage."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 85.0, "year": 2019},
            {**SAMPLE_DATA_ROW, "geoUnit": "TZA", "value": 75.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "UGA", "value": 70.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "ZWE", "value": 88.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await get_country_ranking("CR.1", year=None, strict_year=False)
        assert result["year_used"] == 2020  # 3 countries vs 1

    @pytest.mark.asyncio
    async def test_small_dataset_all_in_top(self, monkeypatch):
        """When total countries <= top_n + bottom_n, all go in 'top', bottom is empty."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 85.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "TZA", "value": 75.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await get_country_ranking("CR.1", year=2020, top_n=10, bottom_n=10, strict_year=False)
        assert len(result["top"]) == 2
        assert result["bottom"] == []

    @pytest.mark.asyncio
    async def test_dense_ranking_ties(self, monkeypatch):
        """Tied values should share the same rank."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 90.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "TZA", "value": 90.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "UGA", "value": 80.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await get_country_ranking("CR.1", year=2020, strict_year=False)
        ranks = [r["rank"] for r in result["top"]]
        assert ranks[0] == ranks[1] == 1  # tied
        assert ranks[2] == 2  # dense ranking, not 3

    @pytest.mark.asyncio
    async def test_no_data(self, monkeypatch):
        from unesco_reader.exceptions import NoDataError
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", MagicMock(side_effect=NoDataError()))
        result = await get_country_ranking("NOPE", year=2020, strict_year=False)
        assert "error" in result


# ── compare_geographies ──────────────────────────────────────────────────────


class TestCompareGeographies:
    @pytest.mark.asyncio
    async def test_basic_comparison(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "geoUnitName": "Kenya", "value": 85.0, "year": 2020},
            {**SAMPLE_DATA_ROW, "geoUnit": "TZA", "geoUnitName": "Tanzania", "value": 75.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await compare_geographies("CR.1", ["KEN", "TZA"], year=2020, strict_year=False)
        assert len(result["comparison"]) == 2
        assert result["comparison"][0]["code"] == "KEN"  # highest value first
        assert result["missing_codes"] == []

    @pytest.mark.asyncio
    async def test_empty_codes(self):
        result = await compare_geographies("CR.1", [], year=2020)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_too_many_codes(self):
        result = await compare_geographies("CR.1", [f"X{i}" for i in range(25)], year=2020)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_strict_year_requires_year(self):
        result = await compare_geographies("CR.1", ["KEN"], year=None, strict_year=True)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_missing_codes_reported(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 85.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await compare_geographies("CR.1", ["KEN", "ZZZ"], year=2020, strict_year=False)
        assert "ZZZ" in result["missing_codes"]
        assert len(result["comparison"]) == 1

    @pytest.mark.asyncio
    async def test_deduplication(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 85.0, "year": 2020},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await compare_geographies("CR.1", ["KEN", "KEN", "KEN"], year=2020, strict_year=False)
        assert len(result["comparison"]) == 1

    @pytest.mark.asyncio
    async def test_year_fallback_when_not_strict(self, monkeypatch):
        """With strict_year=False, should fall back to nearest year."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "geoUnit": "KEN", "value": 85.0, "year": 2019},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)

        result = await compare_geographies("CR.1", ["KEN"], year=2020, strict_year=False)
        assert len(result["comparison"]) == 1
        assert result["comparison"][0]["year"] == 2019
        assert result["note"] is not None  # should mention substitution


# ── get_latest_value ─────────────────────────────────────────────────────────


class TestGetLatestValue:
    @pytest.mark.asyncio
    async def test_basic_latest(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "year": 2019, "value": 80.0},
            {**SAMPLE_DATA_ROW, "year": 2020, "value": 85.5},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_latest_value(ctx, "CR.1", geo_unit_code="KEN")
        assert result["year"] == 2020
        assert result["value"] == 85.5

    @pytest.mark.asyncio
    async def test_specific_year(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "year": 2019, "value": 80.0},
            {**SAMPLE_DATA_ROW, "year": 2020, "value": 85.5},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_latest_value(ctx, "CR.1", geo_unit_code="KEN", year=2019)
        assert result["year"] == 2019
        assert result["value"] == 80.0

    @pytest.mark.asyncio
    async def test_year_fallback_nearest(self, monkeypatch):
        """When requested year has no data, should fall back to nearest."""
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "year": 2018, "value": 78.0},
            {**SAMPLE_DATA_ROW, "year": 2020, "value": 85.5},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_latest_value(ctx, "CR.1", geo_unit_code="KEN", year=2019)
        assert result["year"] in (2018, 2020)  # nearest to 2019
        assert "nearest" in result["note"].lower()

    @pytest.mark.asyncio
    async def test_no_data_error(self, monkeypatch):
        from unesco_reader.exceptions import NoDataError
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", MagicMock(side_effect=NoDataError()))
        ctx = _make_ctx()

        result = await get_latest_value(ctx, "CR.1", geo_unit_code="KEN")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_qualifier_included(self, monkeypatch):
        df = _make_data_df([
            {**SAMPLE_DATA_ROW, "qualifier": "<"},
        ])
        monkeypatch.setattr("unesco_mcp.server.uis.get_data", lambda **kw: df)
        ctx = _make_ctx()

        result = await get_latest_value(ctx, "CR.1", geo_unit_code="KEN")
        assert result["qualifier"] == "<"

    @pytest.mark.asyncio
    async def test_elicitation_error_when_geo_omitted(self):
        """When geo_unit_code is omitted and elicitation fails, should return error."""
        ctx = _make_ctx(elicit_side_effect=RuntimeError("not supported"))
        result = await get_latest_value(ctx, "CR.1")
        assert "error" in result
        assert "elicitation_error" in result
