"""SQLite database for caching UNESCO UIS indicators and disaggregation metadata."""

import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import unesco_reader as uis

from unesco_mcp.config import DB_TTL_HOURS

DB_PATH = Path(tempfile.gettempdir()) / "unesco_mcp" / "uis.db"


@contextmanager
def _get_connection():
    """Yield a SQLite connection with foreign keys enabled, auto-commit/rollback, and guaranteed close."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _indicators_table():
    """Return CREATE TABLE statement for the indicators table."""

    return """ CREATE TABLE IF NOT EXISTS indicators
                   (code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    theme TEXT NOT NULL,
                    lastDataUpdate TEXT,
                    timeLine_min INTEGER,
                    timeLine_max INTEGER,
                    totalRecordCount INTEGER,
                    geoUnitType TEXT
                   )
                   """


def _disaggregations_type_table():
    """Return CREATE TABLE statement for the disaggregation_types table."""

    return '''
                   CREATE TABLE IF NOT EXISTS disaggregation_types
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type_code TEXT NOT NULL UNIQUE,
                    type_name TEXT NOT NULL UNIQUE
                   )
                   '''


def _disaggregations_values_table():
    """Return CREATE TABLE statement for the disaggregation_values table."""

    return '''
           CREATE TABLE IF NOT EXISTS disaggregation_values
           (id INTEGER PRIMARY KEY AUTOINCREMENT,
            type_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            FOREIGN KEY (type_id) REFERENCES disaggregation_types (id),
            UNIQUE (type_id, code)
            )
           '''

def _indicator_disaggregations_table():
    """Return CREATE TABLE statement for the indicator_disaggregations table."""

    return '''
           CREATE TABLE IF NOT EXISTS indicator_disaggregations
           (indicator_code TEXT NOT NULL,
            disaggregation_id INTEGER NOT NULL,
            FOREIGN KEY (indicator_code) REFERENCES indicators (code),
            FOREIGN KEY (disaggregation_id) REFERENCES disaggregation_values (id),
            PRIMARY KEY (indicator_code, disaggregation_id)
           )
           '''


def _geo_units_table():
    """Return CREATE TABLE statement for the geo_units table."""

    return """
           CREATE TABLE IF NOT EXISTS geo_units
           (code         TEXT PRIMARY KEY,
            name         TEXT NOT NULL,
            type         TEXT NOT NULL,
            region_group TEXT
           )
           """


def _geo_units_fts_table():
    """Return CREATE VIRTUAL TABLE statement for FTS5 full-text search on geo unit names."""

    return """
           CREATE VIRTUAL TABLE IF NOT EXISTS geo_units_fts
           USING fts5(code UNINDEXED, name, tokenize='porter unicode61')
           """


def _themes_table():
    """Return CREATE TABLE statement for the themes table."""

    return """
           CREATE TABLE IF NOT EXISTS themes
           (code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            last_update TEXT,
            last_update_description TEXT,
            indicator_count INTEGER
           )
           """


def _db_meta_table():
    """Return CREATE TABLE statement for the db_meta key-value table."""

    return """
           CREATE TABLE IF NOT EXISTS db_meta (
               key TEXT PRIMARY KEY,
               value TEXT NOT NULL
           )
           """


def _fts_table():
    """Return CREATE VIRTUAL TABLE statement for FTS5 full-text search on indicator names."""

    return """
           CREATE VIRTUAL TABLE IF NOT EXISTS indicators_fts
           USING fts5(code UNINDEXED, name, tokenize='porter unicode61')
           """


def _indexes():
    """Return index creation statements for fast querying."""

    return [
        # Indicators: filter by theme, search by name
        "CREATE INDEX IF NOT EXISTS idx_indicators_theme ON indicators (theme)",
        "CREATE INDEX IF NOT EXISTS idx_indicators_name ON indicators (name)",

        # Disaggregation values: lookup by type_id, by name
        "CREATE INDEX IF NOT EXISTS idx_disaggregation_values_type_id ON disaggregation_values (type_id)",
        "CREATE INDEX IF NOT EXISTS idx_disaggregation_values_name ON disaggregation_values (name)",

        # Geo units: filter by type (NATIONAL/REGIONAL) and region_group
        "CREATE INDEX IF NOT EXISTS idx_geo_units_type ON geo_units (type)",
        "CREATE INDEX IF NOT EXISTS idx_geo_units_region_group ON geo_units (region_group)",
    ]


def init_db():
    """Create all tables and indexes if they don't already exist."""

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(_indicators_table())
        cursor.execute(_themes_table())
        cursor.execute(_geo_units_table())
        cursor.execute(_geo_units_fts_table())
        cursor.execute(_fts_table())
        cursor.execute(_db_meta_table())
        cursor.execute(_disaggregations_type_table())
        cursor.execute(_disaggregations_values_table())
        cursor.execute(_indicator_disaggregations_table())

        for idx in _indexes():
            cursor.execute(idx)


def store_indicators():
    """Store indicators in the database."""

    indicators_df = uis.available_indicators()

    rows = [
        (row["indicatorCode"], row["name"], row["theme"],
         str(row["lastDataUpdate"]),
         int(row["timeLine_min"]), int(row["timeLine_max"]),
         int(row["totalRecordCount"]), row["geoUnitType"])
        for _, row in indicators_df.iterrows()
    ]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO indicators
            (code, name, theme, lastDataUpdate,
             timeLine_min, timeLine_max, totalRecordCount, geoUnitType)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

        # Populate FTS index
        conn.execute("DELETE FROM indicators_fts")
        conn.executemany(
            "INSERT INTO indicators_fts (code, name) VALUES (?, ?)",
            [(code, name) for code, name, *_ in rows],
        )


def store_themes():
    """Fetch theme metadata from the UIS API and store in the themes table.

    Derives human-readable names from theme codes and computes indicator counts
    from the indicators table (which must be populated first).
    """
    themes = uis.available_themes(raw=True)

    rows = []
    for item in themes:
        code = item["theme"]
        parts = code.lower().split("_")
        if len(parts) == 1:
            name = parts[0]
        else:
            name = ", ".join(parts[:-1]) + " & " + parts[-1]
        name = name.title()

        # Count indicators for this theme from the already-populated indicators table
        count_rows = query(
            "SELECT COUNT(*) as cnt FROM indicators WHERE theme = ?", (code,)
        )
        indicator_count = count_rows[0]["cnt"] if count_rows else 0

        rows.append((
            code,
            name,
            item.get("lastUpdate"),
            item.get("lastUpdateDescription"),
            indicator_count,
        ))

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO themes
            (code, name, last_update, last_update_description, indicator_count)
            VALUES (?, ?, ?, ?, ?)
            """, rows)


def store_geo_units():
    """Fetch all geographic units from the UIS API and store in the geo_units table."""

    df = uis.available_geo_units()

    rows = [
        (
            row["id"],
            row["name"],
            row["type"],
            row["regionGroup"] if row["regionGroup"] else None,
        )
        for _, row in df.iterrows()
    ]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO geo_units (code, name, type, region_group)
            VALUES (?, ?, ?, ?)
            """, rows)

        conn.execute("DELETE FROM geo_units_fts")
        conn.executemany(
            "INSERT INTO geo_units_fts (code, name) VALUES (?, ?)",
            [(code, name) for code, name, *_ in rows],
        )


def search_geo_units(
    query_term: str,
    type_filter: str | None = None,
    region_group: str | None = None,
) -> list[dict]:
    """Search geo units by name or exact code.

    Tries an exact code match first (for ISO3 lookups like "ZWE"), then falls
    back to FTS5 name search. Results from both are merged and deduplicated.

    Args:
        query_term: A country/region name or ISO3 code to search for.
        type_filter: Optional. "NATIONAL" or "REGIONAL" to restrict results.
        region_group: Optional. Filter to a specific region group (e.g. "WB", "SDG").

    Returns:
        List of matching geo unit dicts with code, name, type, region_group.
    """
    conditions: list[str] = []
    params: list[str] = []

    if type_filter is not None:
        conditions.append("type = ?")
        params.append(type_filter.upper())

    if region_group is not None:
        conditions.append("region_group = ?")
        params.append(region_group.upper())

    where_suffix = (" AND " + " AND ".join(conditions)) if conditions else ""

    seen: set[str] = set()
    results: list[dict] = []

    # Exact code match (handles ISO3 like "ZWE" or full regional codes)
    exact_rows = query(
        f"SELECT code, name, type, region_group FROM geo_units WHERE code = ?{where_suffix}",
        tuple([query_term.upper()] + params),
    )
    for row in exact_rows:
        seen.add(row["code"])
        results.append(row)

    # FTS name search — wrap in double quotes to treat as phrase and avoid
    # FTS5 interpreting hyphens or spaces as operators (e.g. "Sub-Saharan Africa")
    fts_query = '"' + query_term.replace('"', '') + '"'
    fts_rows = query(
        f"SELECT g.code, g.name, g.type, g.region_group "
        f"FROM geo_units g "
        f"JOIN geo_units_fts fts ON fts.code = g.code "
        f"WHERE geo_units_fts MATCH ?{where_suffix} "
        f"ORDER BY fts.rank",
        tuple([fts_query] + params),
    )
    for row in fts_rows:
        if row["code"] not in seen:
            seen.add(row["code"])
            results.append(row)

    return results


def get_themes() -> list[dict]:
    """Return all themes from the database."""
    return query(
        "SELECT code, name, last_update, last_update_description, indicator_count "
        "FROM themes ORDER BY name"
    )


def get_disaggregations() -> dict:
    """Fetch all disaggregations from the UIS API, grouped by type code."""

    disaggregations = {}

    for i in uis.api.get_indicators(disaggregations=True):

        if "disaggregations" not in i:
            continue

        for j in i["disaggregations"]:
            dis_type_code = j["disaggregationType"]["code"]
            dis_type_name = j["disaggregationType"]["name"]

            dis_code = j["code"]
            dis_name = j["name"]

            dis_definition = None
            if "glossaryTerms" in j and len(j["glossaryTerms"]) > 0:
                term = j["glossaryTerms"][0]
                if "definition" in term:
                    dis_definition = term["definition"]

            if dis_type_code not in disaggregations:
                disaggregations[dis_type_code] = {"name": dis_type_name, "disaggregations": {}}

            if dis_code not in disaggregations[dis_type_code]["disaggregations"]:
                disaggregations[dis_type_code]["disaggregations"][dis_code] = {"name": dis_name,
                                                                               "definition": dis_definition}
    return disaggregations


def store_disaggregation_types(disaggregations: dict):
    """Store disaggregation types in the database."""

    rows = [(code, item["name"]) for code, item in disaggregations.items()]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO disaggregation_types
            (type_code, type_name)
            VALUES (?, ?)
            """, rows)


def store_disaggregation_values(disaggregations: dict):
    """Store disaggregation values in the database."""

    rows = [
        (type_code, dis_code, vals["name"], vals["definition"])
        for type_code, values in disaggregations.items()
        for dis_code, vals in values["disaggregations"].items()
    ]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO disaggregation_values
            (type_id, code, name, description)
            VALUES (
                (SELECT id FROM disaggregation_types WHERE type_code = ?),
                ?, ?, ?
               )
            """, rows)


def store_indicator_disaggregations():
    """Fetch indicator-disaggregation mappings from the UIS API and store them in the database."""

    rows = [
        (i["indicatorCode"], disaggregation["disaggregationType"]["code"], disaggregation["code"])
        for i in uis.api.get_indicators(disaggregations=True)
        for disaggregation in i.get("disaggregations", [])
    ]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO indicator_disaggregations
            (indicator_code, disaggregation_id)
            SELECT ?, dv.id
            FROM disaggregation_values dv
            JOIN disaggregation_types dt ON dt.id = dv.type_id
            WHERE dt.type_code = ? AND dv.code = ?
            AND EXISTS (SELECT 1 FROM indicators WHERE code = ?)
            """, [(ic, tc, dc, ic) for ic, tc, dc in rows])


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query and return results as a list of dicts."""
    with _get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def _build_indicator_conditions(
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    coverage_start_year: int | None = None,
    coverage_end_year: int | None = None,
    updated_since: str | None = None,
) -> tuple[list[str], list[str | int]]:
    """Build WHERE conditions and params for indicator filters (no FTS)."""
    conditions: list[str] = []
    params: list[str | int] = []

    if theme is not None:
        conditions.append("i.theme = ?")
        params.append(theme)

    if coverage_start_year is not None:
        conditions.append("i.timeLine_min <= ?")
        params.append(coverage_start_year)

    if coverage_end_year is not None:
        conditions.append("i.timeLine_max >= ?")
        params.append(coverage_end_year)

    if updated_since is not None:
        conditions.append("i.lastDataUpdate >= ?")
        params.append(updated_since)

    if disaggregation_types:
        for type_code in disaggregation_types:
            conditions.append("""EXISTS (
                SELECT 1 FROM indicator_disaggregations id_dt
                JOIN disaggregation_values dv_dt ON dv_dt.id = id_dt.disaggregation_id
                JOIN disaggregation_types dt ON dt.id = dv_dt.type_id
                WHERE id_dt.indicator_code = i.code AND dt.type_code = ?
            )""")
            params.append(type_code)

    if disaggregation_values:
        placeholders = ", ".join("?" for _ in disaggregation_values)
        conditions.append(f"""(
            SELECT COUNT(DISTINCT dv.id)
            FROM indicator_disaggregations id_dv
            JOIN disaggregation_values dv ON dv.id = id_dv.disaggregation_id
            WHERE id_dv.indicator_code = i.code AND dv.code IN ({placeholders})
        ) = {len(disaggregation_values)}""")
        params.extend(disaggregation_values)

    return conditions, params


def search_indicators(
    query_term: str | None = None,
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    limit: int | None = None,
) -> tuple[list[dict], int]:
    """Search indicators with structured filters and optional FTS5 text matching.

    Structured filters (theme, disaggregation_types, disaggregation_values) are applied
    in SQL. If query_term is provided, it is matched against indicator names using SQLite
    FTS5 full-text search with Porter stemming, and results are ranked by relevance.

    The two disaggregation filters are independent:
    - disaggregation_types: indicators must have at least one value from EACH listed type
    - disaggregation_values: indicators must have ALL listed specific value codes

    Args:
        query_term: FTS5 text search on indicator name, with stemming and ranking.
        theme: Exact match on theme code.
        disaggregation_types: Filter for broad disaggregation types (e.g. ["SEX", "AGE"]).
        disaggregation_values: Filter for specific disaggregation value codes (e.g. ["M", "F"]).
        limit: Maximum number of results to return. None means no limit.

    Returns:
        Tuple of (results, total_count) where results is a list of matching indicator
        dicts and total_count is the total number of matches before limiting.
    """
    conditions, params = _build_indicator_conditions(
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
    )
    joins: list[str] = []
    order_by = ""

    if query_term is not None:
        joins.append("JOIN indicators_fts fts ON fts.code = i.code")
        conditions.append("indicators_fts MATCH ?")
        params.append(query_term)
        order_by = "ORDER BY fts.rank"

    join_clause = " ".join(joins)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Count total matches first
    count_sql = f"SELECT COUNT(*) as cnt FROM indicators i {join_clause} {where}"
    count_rows = query(count_sql, tuple(params))
    total_count = count_rows[0]["cnt"] if count_rows else 0

    # Fetch data with optional LIMIT
    data_sql = f"SELECT i.code, i.name, i.theme, i.timeLine_min, i.timeLine_max FROM indicators i {join_clause} {where} {order_by}"
    data_params = list(params)
    if limit is not None:
        data_sql += " LIMIT ?"
        data_params.append(limit)

    results = query(data_sql, tuple(data_params))
    return results, total_count


def get_indicator_summaries(codes: list[str]) -> list[dict]:
    """Return lightweight summaries for the given indicator codes.

    Fetches base fields from the indicators table and distinct disaggregation
    type names for each indicator. Much lighter than a full metadata API call.

    Args:
        codes: List of indicator codes (max MAX_SUMMARY_CODES).

    Returns:
        List of indicator summary dicts, each with disaggregation_types list.
    """
    if not codes:
        return []

    placeholders = ", ".join("?" for _ in codes)
    indicators = query(
        f"SELECT code, name, theme, timeLine_min, timeLine_max, "
        f"totalRecordCount, geoUnitType, lastDataUpdate "
        f"FROM indicators WHERE code IN ({placeholders})",
        tuple(codes),
    )

    if not indicators:
        return []

    # Get distinct disaggregation type names per indicator
    found_codes = [ind["code"] for ind in indicators]
    found_placeholders = ", ".join("?" for _ in found_codes)
    disagg_rows = query(
        f"SELECT id_map.indicator_code, dt.type_name "
        f"FROM indicator_disaggregations id_map "
        f"JOIN disaggregation_values dv ON dv.id = id_map.disaggregation_id "
        f"JOIN disaggregation_types dt ON dt.id = dv.type_id "
        f"WHERE id_map.indicator_code IN ({found_placeholders}) "
        f"GROUP BY id_map.indicator_code, dt.type_name",
        tuple(found_codes),
    )

    # Group disaggregation type names by indicator code
    disagg_by_code: dict[str, list[str]] = {}
    for row in disagg_rows:
        disagg_by_code.setdefault(row["indicator_code"], []).append(row["type_name"])

    for ind in indicators:
        ind["disaggregation_types"] = disagg_by_code.get(ind["code"], [])

    return indicators


def count_indicators(
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    coverage_start_year: int | None = None,
    coverage_end_year: int | None = None,
    updated_since: str | None = None,
) -> int:
    """Count indicators matching the given filters.

    Args:
        theme: Exact match on theme code.
        disaggregation_types: Indicators must support ALL listed disaggregation type codes.
        disaggregation_values: Indicators must have ALL listed specific value codes.
        coverage_start_year: Indicator data must start by this year (timeLine_min <= year).
        coverage_end_year: Indicator data must extend through this year (timeLine_max >= year).
        updated_since: ISO date string. Only indicators updated on or after this date.

    Returns:
        Count of matching indicators.
    """
    conditions, params = _build_indicator_conditions(
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
        coverage_start_year=coverage_start_year,
        coverage_end_year=coverage_end_year,
        updated_since=updated_since,
    )
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT COUNT(*) as cnt FROM indicators i {where}"
    rows = query(sql, tuple(params))
    return rows[0]["cnt"] if rows else 0


# ── Build ──────────────────────────────────────────────────────────────────


def db_built_at() -> str | None:
    """Return the ISO timestamp of the last DB build, or None if unavailable."""
    try:
        rows = query("SELECT value FROM db_meta WHERE key = 'built_at'")
        return rows[0]["value"] if rows else None
    except Exception:
        return None


_REQUIRED_TABLES = {"indicators", "themes", "geo_units", "db_meta", "disaggregation_types"}


def is_db_fresh() -> bool:
    """Check whether the cached database exists, is complete, and is within the TTL."""
    if not DB_PATH.exists():
        return False
    try:
        # Verify core tables exist (catches empty or partially-built DBs)
        rows = query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            f"({', '.join('?' for _ in _REQUIRED_TABLES)})",
            tuple(_REQUIRED_TABLES),
        )
        if {r["name"] for r in rows} != _REQUIRED_TABLES:
            return False

        # Check TTL
        rows = query("SELECT value FROM db_meta WHERE key = 'built_at'")
        if not rows:
            return False
        built_at = datetime.fromisoformat(rows[0]["value"])
        age = datetime.now(timezone.utc) - built_at
        return age.total_seconds() < DB_TTL_HOURS * 3600
    except Exception:
        return False


def build_db(fresh: bool = False):
    """Initialize the database and populate all tables.

    Args:
        fresh: If True, delete the existing database and rebuild from scratch.
               If False (default), skip the rebuild when the DB is within TTL.
    """

    if not fresh and is_db_fresh():
        return

    teardown_db()
    init_db()
    store_indicators()
    store_themes()
    store_geo_units()

    disaggregations = get_disaggregations()
    store_disaggregation_types(disaggregations)
    store_disaggregation_values(disaggregations)
    store_indicator_disaggregations()

    with _get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)",
            ("built_at", datetime.now(timezone.utc).isoformat()),
        )


def ensure_fresh():
    """Rebuild the DB if the TTL has expired (for long-running servers)."""
    if not is_db_fresh():
        build_db(fresh=True)


def teardown_db():
    """Remove the database file if it exists."""
    DB_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    build_db()