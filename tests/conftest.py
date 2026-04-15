"""Shared fixtures for unesco-mcp tests."""

import pandas as pd
import pytest

from unesco_mcp import uis_db


# ── Minimal mock data matching the structures expected by store_* functions ──

MOCK_INDICATORS_DF = pd.DataFrame([
    {
        "indicatorCode": "TEST.1",
        "name": "Test Indicator One",
        "theme": "EDUCATION",
        "lastDataUpdate": "2025-01-01",
        "timeLine_min": 2000,
        "timeLine_max": 2023,
        "totalRecordCount": 100,
        "geoUnitType": "NATIONAL",
    },
])

MOCK_THEMES_RAW = [
    {"theme": "EDUCATION", "lastUpdate": "2025-01-01", "lastUpdateDescription": "Updated"},
]

MOCK_GEO_UNITS_DF = pd.DataFrame([
    {"id": "KEN", "name": "Kenya", "type": "NATIONAL", "regionGroup": None},
])

MOCK_API_INDICATORS = [
    {
        "indicatorCode": "TEST.1",
        "disaggregations": [
            {
                "disaggregationType": {"code": "SEX", "name": "Sex"},
                "code": "M",
                "name": "Male",
                "glossaryTerms": [{"definition": "Male population"}],
            },
        ],
    },
]


@pytest.fixture(autouse=True)
def tmp_db(tmp_path, monkeypatch):
    """Redirect uis_db.DB_PATH to a temp file for every test."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr(uis_db, "DB_PATH", db_path)
    yield db_path


@pytest.fixture()
def mock_uis(monkeypatch):
    """Patch all unesco_reader calls so no network requests are made.

    Returns a namespace with the mock objects so tests can swap return values.
    """
    mock_indicators = monkeypatch.setattr(
        "unesco_mcp.uis_db.uis.available_indicators",
        lambda: MOCK_INDICATORS_DF,
    )
    monkeypatch.setattr(
        "unesco_mcp.uis_db.uis.available_themes",
        lambda raw=False: MOCK_THEMES_RAW,
    )
    monkeypatch.setattr(
        "unesco_mcp.uis_db.uis.available_geo_units",
        lambda: MOCK_GEO_UNITS_DF,
    )
    monkeypatch.setattr(
        "unesco_mcp.uis_db.uis.api.get_indicators",
        lambda disaggregations=False: MOCK_API_INDICATORS,
    )
