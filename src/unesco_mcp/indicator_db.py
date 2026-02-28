"""SQLite database for caching UNESCO UIS indicators and disaggregation metadata."""

import sqlite3
from contextlib import contextmanager
import json
import unesco_reader as uis

DB_PATH = "uis.db"


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

    return ('''
                   CREATE TABLE IF NOT EXISTS disaggregation_types
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type_code TEXT NOT NULL UNIQUE,
                    type_name TEXT NOT NULL UNIQUE
                   )
                   ''')


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

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(_indicators_table()) # Indicators table
    cursor.execute(_disaggregations_type_table()) # Disaggregation types table
    cursor.execute(_disaggregations_values_table()) # Disaggregation values table
    cursor.execute(_indicator_disaggregations_table()) # Indicator-disaggregation mappings

    for idx in _indexes():
        cursor.execute(idx)

    conn.commit()
    conn.close()

def store_indicators():
    """Store indicators in the database."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    indicators_df = uis.available_indicators()

    for _, row in indicators_df.iterrows():
        cursor.execute("""
                       INSERT OR REPLACE INTO indicators
                       (code, name, theme, lastDataUpdate,
                        timeLine_min, timeLine_max, totalRecordCount, geoUnitType)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       """, (row["indicatorCode"], row["name"], row["theme"],
                             str(row["lastDataUpdate"]),
                             int(row["timeLine_min"]), int(row["timeLine_max"]),
                             int(row["totalRecordCount"]), row["geoUnitType"]))

    conn.commit()


def get_disaggregations() -> dict:
    """Fetch all disaggregations from the UIS API, grouped by type code."""

    disaggregations = {}

    for i in uis.api.get_indicators(disaggregations=True):

        if "disaggregations" in i:

            for j in i["disaggregations"]:
                dis_type_code = j["disaggregationType"]["code"]  # disaggregation type code
                dis_type_name = j["disaggregationType"]["name"]  # disaggregation type name

                dis_code = j["code"]
                dis_name = j["name"]

                if "glossaryTerms" in j and len(j["glossaryTerms"]) > 0:
                    dis_definition = j["glossaryTerms"][0]
                    if "definition" in dis_definition:
                        dis_definition = dis_definition["definition"]
                    else:
                        dis_definition = None
                else:
                    dis_definition = None

                if dis_type_code not in disaggregations:
                    disaggregations[dis_type_code] = {"name": dis_type_name, "disaggregations": {}}

                if dis_code not in disaggregations[dis_type_code]["disaggregations"]:
                    disaggregations[dis_type_code]["disaggregations"][dis_code] = {"name": dis_name,
                                                                                   "definition": dis_definition}
    return disaggregations


def store_disaggregation_types():
    """Fetch disaggregation types from the UIS API and store them in the database."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    disaggregations = [{"code": code, "name": item["name"]}
                       for code, item in get_disaggregations().items()]

    for dis in disaggregations:
        cursor.execute("""
                       INSERT OR REPLACE INTO disaggregation_types
                       (type_code, type_name)
                       VALUES (?, ?)
                       """, (dis["code"], dis["name"]))

    conn.commit()


def store_disaggregation_values():
    """Fetch disaggregation values from the UIS API and store them in the database."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    disaggregations = []
    for type_code, values in get_disaggregations().items():
        for dis_code in values["disaggregations"]:
            disaggregations.append({
                "type_code": type_code,
                "dis_code": dis_code,
                "dis_name": values["disaggregations"][dis_code]["name"],
                "dis_definition": values["disaggregations"][dis_code]["definition"]
            })

    for dis in disaggregations:
        cursor.execute("""
                       INSERT OR REPLACE INTO disaggregation_values
                       (type_id, code, name, description)
                       VALUES (
                           (SELECT id FROM disaggregation_types WHERE type_code = ?),
                           ?, ?, ?
                          )
                          """, (dis["type_code"], dis["dis_code"], dis["dis_name"], dis["dis_definition"]))

    conn.commit()


def store_indicator_disaggregations():
    """Fetch indicator-disaggregation mappings from the UIS API and store them in the database."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    indicators = []
    for i in uis.api.get_indicators(disaggregations=True):
        for disaggregation in i["disaggregations"]:
            indicators.append({
                "indicator_code": i["indicatorCode"],
                "dis_code": disaggregation["code"],
            })

    for item in indicators:
        cursor.execute("""
                       INSERT OR REPLACE INTO indicator_disaggregations
                       (indicator_code, disaggregation_id)
                       VALUES (
                           ?,
                           (SELECT id FROM disaggregation_values WHERE code = ?)
                          )
                          """, (item["indicator_code"], item["dis_code"]))

    conn.commit()


def build_db():
    """Initialize the database and populate all tables."""

    init_db()
    store_indicators()
    store_disaggregation_types()
    store_disaggregation_values()
    store_indicator_disaggregations()


if __name__ == "__main__":
    build_db()
