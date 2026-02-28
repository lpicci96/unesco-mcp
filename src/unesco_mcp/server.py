
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastmcp import FastMCP
import unesco_reader as uis

from unesco_mcp.indicator_db import build_db, teardown_db, query as db_query, search_indicators as db_search_indicators, MAX_RESULTS


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Build the indicator DB on startup, tear it down on shutdown."""
    build_db(fresh=True)
    try:
        yield
    finally:
        teardown_db()


mcp = FastMCP(
    "unesco-mcp",
    lifespan=lifespan,
    instructions="""You are a UNESCO UIS data assistant. Follow this workflow strictly:

1. DISCOVER FIRST: When a user asks about indicators, ALWAYS call list_themes and
   list_disaggregation_types BEFORE searching. If the user mentions a concept like
   "primary education", "gender", "age group", or "wealth" — these are almost certainly
   disaggregation types or values. Call list_disaggregation_types to find the matching
   type code, then get_disaggregation_values to find the exact value codes for 
   each concept, e.g. once for education level to find "primary education and
   once for sex

2. MAP USER CONCEPTS TO CODES: Common mappings include education levels, sex, age groups,
   wealth quintiles, and geographic regions. Never guess codes — always look them up.

3. SEARCH WITH STRUCTURED FILTERS: search_indicators has two independent disaggregation filters:
   - disaggregation_types: a list of type codes (e.g. ["SEX", "EDU_LEVEL"]). Indicators must
     support ALL listed types. Use this to ensure data can be broken down in the ways the user needs.
   - disaggregation_values: a list of specific value codes (e.g. ["M", "L1"]).  Indicators must
     have ALL listed values. Use this to pin results to exact categories the user asked about.
   These filters are independent and can be used separately or together.
   Only use the query parameter as a secondary refinement for indicator name matching, never as
   the primary filter for concepts that map to disaggregation types or values.

Example: "Show me primary education completion indicators by sex"
  → list_disaggregation_types → find "education level" type code (e.g. "EDU_LEVEL") and "sex" type code (e.g. "SEX")
  → get_disaggregation_values("EDU_LEVEL") → find "primary education" value code (e.g. "L1")
  → search_indicators(disaggregation_types=["EDU_LEVEL", "SEX"], disaggregation_values=["L1"], query="completion")
""",
)



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

    rows = db_query("SELECT type_code, type_name FROM disaggregation_types ORDER BY type_name")

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
    type_rows = db_query(
        "SELECT type_code, type_name FROM disaggregation_types WHERE type_code = ?",
        (type_code,),
    )

    if not type_rows:
        return {"error": f"Disaggregation type '{type_code}' not found. Use list_disaggregation_types to see available types."}

    type_info = type_rows[0]

    values = db_query(
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

# TODO: add additional filters eg time coverage, geo units, latest update date etc
@mcp.tool()
async def search_indicators(
    query: str | None = None,
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
) -> dict:
    """Search UNESCO UIS indicators using structured filters and optional text matching.

    IMPORTANT — REQUIRED WORKFLOW:
    1. Call list_disaggregation_types and get_disaggregation_values to map user concepts
       (e.g. "primary education", "female", "rural") to exact codes BEFORE using this tool.
    2. Call list_themes if the user mentions a thematic area (e.g. "SDG", "education").
    3. Pass the discovered codes as structured filters (theme, disaggregation_types, disaggregation_values).
    4. Only use the query parameter for additional name-based narrowing AFTER applying structured filters,
       or when the user's request genuinely has no matching disaggregation type or theme.

    DO NOT pass concepts like "primary education" or "female" as the query parameter —
    these are disaggregation values and must be looked up and passed as disaggregation_values.

    The two disaggregation filters are independent:
    - disaggregation_types: indicators must have at least one value from EACH listed type
    - disaggregation_values: indicators must have ALL listed specific value codes

    All provided filters are combined with AND logic. At least one parameter must be provided.
    Results are capped at 20. If more exist, tell the user the total count and suggest
    narrowing with additional filters rather than trying to show all results.

    Args:
        query: Fuzzy match on indicator name. Secondary refinement only — do not use for concepts that map to themes or disaggregations.
        theme: Exact theme code (from list_themes).
        disaggregation_types: List of disaggregation type codes (from list_disaggregation_types). Indicators must support ALL listed types.
        disaggregation_values: List of disaggregation value codes (from get_disaggregation_values). Indicators must match ALL listed values.

    Returns:
        A dictionary with:
            - "indicators": List of matching indicators (code, name, theme, timeLine_min, timeLine_max, totalRecordCount).
            - "total": Total number of matching indicators.
            - "returned": Number of indicators returned (capped at 20).
            - "hint": Guidance on next steps.
    """
    if not any([query, theme, disaggregation_types, disaggregation_values]):
        return {"error": "At least one parameter must be provided. Use theme, disaggregation_types, or disaggregation_values to filter, and optionally query to search by name."}

    all_results = db_search_indicators(
        query_term=query,
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
    )

    total = len(all_results)
    truncated = total > MAX_RESULTS
    results = all_results[:MAX_RESULTS]

    hint = "Use indicator codes to retrieve data."
    if truncated:
        hint += (
            f" Showing {MAX_RESULTS} of {total} total matches."
            " Tell the user how many exist and suggest narrowing with additional filters"
            " (theme, disaggregation_types, disaggregation_values, query) rather than listing all results."
        )

    return {
        "indicators": results,
        "total": total,
        "returned": len(results),
        "hint": hint,
    }


def main() -> None:
    """Run the MCP server over stdio by default."""
    mcp.run()


if __name__ == "__main__":
    main()
