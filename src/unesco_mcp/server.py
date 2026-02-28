
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastmcp import FastMCP
import unesco_reader as uis

from unesco_mcp.indicator_db import build_db, teardown_db, query


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Build the indicator DB on startup, tear it down on shutdown."""
    build_db(fresh=True)
    try:
        yield
    finally:
        teardown_db()


#TODO: Add instructions
mcp = FastMCP("unesco-mcp", lifespan=lifespan)



@mcp.tool()
def server_status() -> dict[str, str]:
    """Return basic runtime metadata for smoke tests and client wiring."""
    return {
        "server": "unesco-mcp",
        "status": "ok",
        "utc_time": datetime.now(timezone.utc).isoformat(),
    }


@mcp.tool()
async def list_themes() -> dict:
    """Find all available UNESCO data themes and basic information about them.

    UIS themes are the highest-level topical groups used to organize indicators.

    Returns:
        A dictionary with three keys:
            - "theme information": A list containing each theme code, name, last update date, description of the update, and the count of indicators in that theme.
            - "theme count": The total number of themes available.
            - "hint": A string providing guidance on how to use the theme codes for searching indicators
    """

    themes = uis.available_themes(raw=True)
    indicators = uis.available_indicators()

    for item in themes:
        parts = item["theme"].lower().split("_")

        if len(parts) == 1:
            name = parts[0]
        else:
            name = ", ".join(parts[:-1]) + " & " + parts[-1]

        item["name"] = name.title()
        item["code"] = item.pop("theme")  # rename key

        item["indicator_count"] = len(indicators.loc[lambda d: d.theme == item["code"]])

    return {
        "theme information": themes,
        "theme count": str(len(themes)),
        "hint": "The theme code is useful for searching indicators, but does not need to be shown to the user"
    }


@mcp.tool()
async def list_disaggregation_types() -> dict:
    """Find all available disaggregation types for UNESCO UIS indicators.

    Disaggregation types describe how indicator data can be broken down
    (e.g. by sex, age group, education level, wealth quintile, etc.).

    Returns:
        A dictionary with:
            - "disaggregation_types": A list of dicts, each with 'type_code' and 'type_name'.
            - "count": The total number of disaggregation types.
            - "hint": Guidance on how to use the type codes.
    """

    rows = query("SELECT type_code, type_name FROM disaggregation_types ORDER BY type_name")

    return {
        "disaggregation_types": rows,
        "count": len(rows),
        "hint": "Use disaggregation type codes to find disaggregation values and to search for indicators with specific disaggregations.",
    }


@mcp.tool()
async def get_disaggregation_values(type_code: str) -> dict:
    """Get the available values for a specific disaggregation type.

    Given a disaggregation type code (e.g. 'SEX', 'AGE', 'EDU_LEVEL'),
    return all possible values that indicators can be broken down by.

    Args:
        type_code: The disaggregation type code (from list_disaggregation_types).

    Returns:
        A dictionary with:
            - "type_code": The requested type code.
            - "type_name": The human-readable name of the disaggregation type.
            - "values": A list of dicts, each with 'code', 'name', and 'description'.
            - "count": The total number of values.
    """

    # Verify the type exists
    type_rows = query(
        "SELECT type_code, type_name FROM disaggregation_types WHERE type_code = ?",
        (type_code,),
    )

    if not type_rows:
        return {"error": f"Disaggregation type '{type_code}' not found. Use list_disaggregation_types to see available types."}

    type_info = type_rows[0]

    values = query(
        """
        SELECT dv.code, dv.name, dv.description
        FROM disaggregation_values dv
        JOIN disaggregation_types dt ON dt.id = dv.type_id
        WHERE dt.type_code = ?
        ORDER BY dv.name
        """,
        (type_code,),
    )

    return {
        "type_code": type_info["type_code"],
        "type_name": type_info["type_name"],
        "values": values,
        "count": len(values),
    }


def main() -> None:
    """Run the MCP server over stdio by default."""
    mcp.run()


if __name__ == "__main__":
    main()
