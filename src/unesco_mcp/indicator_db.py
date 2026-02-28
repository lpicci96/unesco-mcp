"""SQLite database for caching UNESCO UIS indicators and disaggregation metadata."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import unesco_reader as uis

DB_PATH = Path(__file__).parent / "uis.db"


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
        (i["indicatorCode"], disaggregation["code"])
        for i in uis.api.get_indicators(disaggregations=True)
        for disaggregation in i.get("disaggregations", [])
    ]

    with _get_connection() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO indicator_disaggregations
            (indicator_code, disaggregation_id)
            VALUES (
                ?,
                (SELECT id FROM disaggregation_values WHERE code = ?)
               )
            """, rows)


# ── Query helpers ──────────────────────────────────────────────────────────


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query and return results as a list of dicts."""
    with _get_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_indicators(theme: str | None = None) -> list[dict]:
    """Return indicators, optionally filtered by theme code."""
    if theme:
        return query("SELECT * FROM indicators WHERE theme = ?", (theme,))
    return query("SELECT * FROM indicators")


def get_indicator(code: str) -> dict | None:
    """Return a single indicator by code, or None."""
    rows = query("SELECT * FROM indicators WHERE code = ?", (code,))
    return rows[0] if rows else None


def search_indicators(term: str) -> list[dict]:
    """Search indicators by name (case-insensitive LIKE)."""
    return query("SELECT * FROM indicators WHERE name LIKE ?", (f"%{term}%",))


def get_disaggregations_for_indicator(indicator_code: str) -> list[dict]:
    """Return disaggregation values linked to a given indicator."""
    return query("""
        SELECT dv.code, dv.name, dv.description, dt.type_code, dt.type_name
        FROM indicator_disaggregations id
        JOIN disaggregation_values dv ON dv.id = id.disaggregation_id
        JOIN disaggregation_types dt ON dt.id = dv.type_id
        WHERE id.indicator_code = ?
        """, (indicator_code,))


# ── Build ──────────────────────────────────────────────────────────────────


def ensure_db():
    """Build the database if it doesn't exist or is empty."""
    if not DB_PATH.exists():
        build_db()
        return
    with _get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM indicators").fetchone()[0]
    if count == 0:
        build_db()


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