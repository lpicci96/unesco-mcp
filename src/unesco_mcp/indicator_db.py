"""SQLite database for caching UNESCO UIS indicators and disaggregation metadata."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import unesco_reader as uis

DB_PATH = Path(__file__).parent / "uis.db"
MAX_RESULTS = 20
MAX_RESULTS_CAP = 50


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
    ]


def init_db():
    """Create all tables and indexes if they don't already exist."""

    with _get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(_indicators_table())
        cursor.execute(_fts_table())
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
            VALUES (
                ?,
                (SELECT dv.id FROM disaggregation_values dv
                 JOIN disaggregation_types dt ON dt.id = dv.type_id
                 WHERE dt.type_code = ? AND dv.code = ?)
               )
            """, rows)


# ── Query helpers ──────────────────────────────────────────────────────────
#
#
def query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query and return results as a list of dicts."""
    with _get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def search_indicators(
    query_term: str | None = None,
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
    updated_since: str | None = None,
) -> list[dict]:
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
        min_year: Earliest year the indicator must cover (timeLine_min <= min_year).
        max_year: Latest year the indicator must cover (timeLine_max >= max_year).
        updated_since: ISO date string (e.g. "2024-01-01"). Only indicators updated on or after this date.

    Returns:
        List of matching indicator dicts, ranked by FTS5 relevance when query_term is used.
    """
    conditions: list[str] = []
    params: list[str | int] = []
    joins: list[str] = []
    order_by = ""

    if query_term is not None:
        joins.append("JOIN indicators_fts fts ON fts.code = i.code")
        conditions.append("indicators_fts MATCH ?")
        params.append(query_term)
        order_by = "ORDER BY fts.rank"

    if theme is not None:
        conditions.append("i.theme = ?")
        params.append(theme)

    if min_year is not None:
        conditions.append("i.timeLine_min <= ?")
        params.append(min_year)

    if max_year is not None:
        conditions.append("i.timeLine_max >= ?")
        params.append(max_year)

    if updated_since is not None:
        conditions.append("i.lastDataUpdate >= ?")
        params.append(updated_since)

    # Each type gets its own EXISTS subquery — indicator must have at least one value per type
    if disaggregation_types:
        for type_code in disaggregation_types:
            conditions.append("""EXISTS (
                SELECT 1 FROM indicator_disaggregations id_dt
                JOIN disaggregation_values dv_dt ON dv_dt.id = id_dt.disaggregation_id
                JOIN disaggregation_types dt ON dt.id = dv_dt.type_id
                WHERE id_dt.indicator_code = i.code AND dt.type_code = ?
            )""")
            params.append(type_code)

    # Values filter: indicator must have ALL listed value codes
    # Join through type_id to avoid ambiguity when different types share a value code
    if disaggregation_values:
        placeholders = ", ".join("?" for _ in disaggregation_values)
        conditions.append(f"""(
            SELECT COUNT(DISTINCT dv.id)
            FROM indicator_disaggregations id_dv
            JOIN disaggregation_values dv ON dv.id = id_dv.disaggregation_id
            WHERE id_dv.indicator_code = i.code AND dv.code IN ({placeholders})
        ) = {len(disaggregation_values)}""")
        params.extend(disaggregation_values)

    join_clause = " ".join(joins)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT i.code, i.name, i.theme, i.timeLine_min, i.timeLine_max, i.totalRecordCount FROM indicators i {join_clause} {where} {order_by}"
    return query(sql, tuple(params))


# ── Build ──────────────────────────────────────────────────────────────────


def build_db(fresh: bool = False):
    """Initialize the database and populate all tables.

    Args:
        fresh: If True, delete the existing database and rebuild from scratch.
    """

    if fresh:
        teardown_db()
    init_db()
    store_indicators()

    disaggregations = get_disaggregations()
    store_disaggregation_types(disaggregations)
    store_disaggregation_values(disaggregations)
    store_indicator_disaggregations()


def teardown_db():
    """Remove the database file if it exists."""
    DB_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    build_db()