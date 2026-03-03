
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastmcp import FastMCP
import unesco_reader as uis

from unesco_mcp.indicator_db import (
    build_db,
    teardown_db,
    query as db_query,
    search_indicators as db_search_indicators,
    count_indicators as db_count_indicators,
    get_themes as db_get_themes,
    get_indicator_summaries as db_get_indicator_summaries,
    get_export_rows as db_get_export_rows,
    write_export_csv,
    MAX_RESULTS,
    MAX_RESULTS_CAP,
    MAX_SUMMARY_CODES,
)


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

4. USE count_indicators FOR COUNTING: If the user asks how many indicators exist for a given
   combination of criteria (especially with year range or date filters), use count_indicators.
   It accepts coverage_start_year, coverage_end_year, and updated_since — filters not available
   in search_indicators. search_indicators is for discovery; count_indicators is for counting.

Example: "Show me primary education completion indicators by sex"
  → list_disaggregation_types → find "education level" type code (e.g. "EDU_LEVEL") and "sex" type code (e.g. "SEX")
  → get_disaggregation_values("EDU_LEVEL") → find "primary education" value code (e.g. "L1")
  → search_indicators(disaggregation_types=["EDU_LEVEL", "SEX"], disaggregation_values=["L1"], query="completion")

5. USE get_indicator_summary FOR QUICK OVERVIEWS: When the user needs a quick comparison
   of several indicators (e.g. after a search), use get_indicator_summary with their codes.
   It returns key fields and disaggregation type names from the local database — much faster
   than get_indicator_metadata. Reserve get_indicator_metadata for when the user needs full
   definitions, methodology, or detailed disaggregation breakdowns for a single indicator.

6. USE export_indicators FOR CSV EXPORTS: When the user wants to save, download, export,
   or get a full/complete list of indicators, use export_indicators — NOT search_indicators.
   Trigger words include: "save", "download", "export", "give me all", "full list", "complete
   list". export_indicators has no result cap and always writes a CSV file to ~/Downloads/.
   After calling it, ALWAYS tell the user exactly: "Saved {row_count} indicators to: {saved_to}"

Example: "How many education indicators have data from 2010 to 2020?"
  → list_themes → find education theme code
  → count_indicators(theme="EDUCATION", coverage_start_year=2010, coverage_end_year=2020)
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

    themes = db_get_themes()

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

@mcp.tool()
async def search_indicators(
    query: str | None = None,
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    limit: int = MAX_RESULTS,
) -> dict:
    """Search UNESCO UIS indicators by relevance using text and structured filters.

    Use this tool to discover which indicators exist for a topic. Results are capped
    (default 20, max 50) and intended for interactive exploration only — do NOT use
    this to save, download, or give the user a full list of indicators. For that,
    use export_indicators instead. For counting indicators with precise year or date
    filters, use count_indicators.

    IMPORTANT - SUGGESTED WORKFLOW:
    1. Call list_themes if the user mentions a thematic area (e.g. "education", "culture") to find the exact theme code.
    2. Call list_disaggregation_types to find relevant disaggregation type codes for user concepts like "sex" or
       "education level". Then call get_disaggregation_values for each type to find exact value codes for concepts
       like "female" or "primary education". DO NOT pass these as the query parameter — they are disaggregation
       values and must be looked up for accurate results.
    3. Pass discovered codes as structured filters. Only use query for additional name-based narrowing AFTER
       applying structured filters, or when the user's request has no matching disaggregation type or theme.

    All provided filters are combined with AND logic. At least one filter must be provided.
    Results default to 20. If more exist, suggest narrowing with additional filters rather than increasing the limit.

    Args:
        query: Full-text search on indicator name (supports stemming, e.g. "completing" matches "completion").
               Secondary refinement only — do not use for concepts that map to themes or disaggregations.
        theme: Exact theme code (from list_themes).
        disaggregation_types: List of disaggregation type codes (from list_disaggregation_types). Indicators must support ALL listed types.
        disaggregation_values: List of disaggregation value codes (from get_disaggregation_values). Indicators must match ALL listed values.
        limit: Maximum number of results to return (default 20, max 50). Prefer narrowing filters over increasing limit.

    Returns:
        A dictionary with:
            - "indicators": List of matching indicators, each with: code, name, theme, timeLine_min, timeLine_max.
            - "query_matches": Number of indicators matched by this query. This is NOT the total count of all
              UNESCO indicators — it only reflects how many matched these specific filters. Some relevant
              indicators may be missing if the query is too narrow or the text search didn't capture them.
            - "returned": Number of indicators included in this response (may be less than query_matches if truncated).
            - "hint": Guidance on next steps.
    """
    if not any([query, theme, disaggregation_types, disaggregation_values]):
        return {"error": "At least one filter parameter must be provided."}

    effective_limit = min(max(limit, 1), MAX_RESULTS_CAP)

    results, total = db_search_indicators(
        query_term=query,
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
        limit=effective_limit,
    )

    truncated = total > effective_limit

    hint = "Use indicator codes to retrieve data."
    if truncated:
        hint += (
            f" Showing {effective_limit} of {total} query matches."
            " Tell the user how many were found and suggest narrowing with additional filters"
            " (theme, disaggregation_types, disaggregation_values, query)"
            " rather than increasing the limit."
        )

    return {
        "indicators": results,
        "query_matches": total,
        "returned": len(results),
        "hint": hint,
    }


@mcp.tool()
async def count_indicators(
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
    coverage_start_year: int | None = None,
    coverage_end_year: int | None = None,
    updated_since: str | None = None,
) -> dict:
    """Count UNESCO UIS indicators matching precise filter criteria.

    Use this tool when the user wants to know how many indicators exist for a given
    combination of filters, including year coverage or update date constraints.
    Unlike search_indicators, this tool returns an exact count and accepts year range filters.

    All provided filters are combined with AND logic. If no filters are provided, returns
    the total count of all indicators in the database.

    Args:
        theme: Exact theme code (from list_themes).
        disaggregation_types: List of disaggregation type codes. Indicators must support ALL listed types.
        disaggregation_values: List of disaggregation value codes. Indicators must match ALL listed values.
        coverage_start_year: Only count indicators whose data begins by this year (i.e. timeLine_min <= year).
        coverage_end_year: Only count indicators whose data extends through this year (i.e. timeLine_max >= year).
        updated_since: ISO date string (e.g. "2024-01-01"). Only count indicators updated on or after this date.

    Returns:
        A dictionary with:
            - "count": The exact number of indicators matching all provided filters.
            - "filters_applied": A summary of which filters were used.
    """
    count = db_count_indicators(
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
        coverage_start_year=coverage_start_year,
        coverage_end_year=coverage_end_year,
        updated_since=updated_since,
    )

    filters_applied = {k: v for k, v in {
        "theme": theme,
        "disaggregation_types": disaggregation_types,
        "disaggregation_values": disaggregation_values,
        "coverage_start_year": coverage_start_year,
        "coverage_end_year": coverage_end_year,
        "updated_since": updated_since,
    }.items() if v is not None}

    return {
        "count": count,
        "filters_applied": filters_applied or "none (total across all indicators)",
    }


@mcp.tool()
async def get_indicator_metadata(indicator_code: str) -> dict:
    """Get detailed metadata for a specific UNESCO UIS indicator.

    Returns definitional and methodological information for an indicator, including
    its glossary definition, purpose, calculation method, data sources, and available
    disaggregations. Use this after finding indicator codes via search_indicators.

    Args:
        indicator_code: The indicator code (e.g. "CR.1", "ROFST.1.cp").

    Returns:
        A dictionary with:
            - "code": Indicator code.
            - "name": Full indicator name.
            - "theme": Theme code.
            - "last_update": Date and description of the most recent data release.
            - "data_availability": Time coverage (min/max year), total record count, and geographic unit types.
            - "definition": Glossary entry with definition, purpose, calculation method, data source,
              interpretation, and limitations (if available).
            - "disaggregations": List of available disaggregation breakdowns (code, name, type).
    """
    results = uis.get_metadata(indicator_code, glossaryTerms=True, disaggregations=True)

    if not results:
        return {"error": f"No metadata found for indicator '{indicator_code}'."}

    raw = results[0]

    # Core identity
    out: dict = {
        "code": raw.get("indicatorCode"),
        "name": raw.get("name"),
        "theme": raw.get("theme"),
        "last_update": {
            "date": raw.get("lastDataUpdate"),
            "description": raw.get("lastDataUpdateDescription"),
        },
    }

    # Data availability
    avail = raw.get("dataAvailability", {})
    timeline = avail.get("timeLine", {})
    geo = avail.get("geoUnits", {})
    out["data_availability"] = {
        "year_min": timeline.get("min"),
        "year_max": timeline.get("max"),
        "total_records": avail.get("totalRecordCount"),
        "geo_unit_types": geo.get("types", []),
    }

    # Glossary — take the first term's key fields
    glossary_terms = raw.get("glossaryTerms", [])
    if glossary_terms:
        term = glossary_terms[0]
        out["definition"] = {k: v for k, v in {
            "name": term.get("name"),
            "definition": term.get("definition"),
            "purpose": term.get("purpose"),
            "calculation_method": term.get("calculationMethod"),
            "data_source": term.get("dataSource"),
            "interpretation": term.get("interpretation"),
            "limitations": term.get("limitations"),
        }.items() if v}

    # Disaggregations — code, name, type only (skip nested glossary terms)
    disaggregations = raw.get("disaggregations", [])
    out["disaggregations"] = [
        {
            "code": d.get("code"),
            "name": d.get("name"),
            "type_code": d.get("disaggregationType", {}).get("code"),
            "type_name": d.get("disaggregationType", {}).get("name"),
        }
        for d in disaggregations
    ]

    return out


@mcp.tool()
async def get_indicator_summary(indicator_codes: list[str]) -> dict:
    """Get a lightweight summary for one or more UNESCO UIS indicators.

    Returns key fields from the local database without making API calls.
    Much faster than get_indicator_metadata — use this when you need a quick
    overview of multiple indicators (e.g. after a search) rather than full
    definitional detail.

    Use get_indicator_metadata when you need glossary definitions, methodology,
    or detailed disaggregation breakdowns for a single indicator.

    Args:
        indicator_codes: List of indicator codes (1–10). Use codes from search_indicators results.

    Returns:
        A dictionary with:
            - "indicators": List of summaries, each with code, name, theme, timeLine_min/max,
              totalRecordCount, geoUnitType, lastDataUpdate, and disaggregation_types (list of type names).
            - "returned": Number of indicators found.
            - "not_found": List of requested codes that were not in the database.
    """
    if not indicator_codes:
        return {"error": "At least one indicator code must be provided."}

    if len(indicator_codes) > MAX_SUMMARY_CODES:
        return {"error": f"Maximum {MAX_SUMMARY_CODES} indicator codes allowed per request."}

    summaries = db_get_indicator_summaries(indicator_codes)
    found_codes = {s["code"] for s in summaries}
    not_found = [c for c in indicator_codes if c not in found_codes]

    return {
        "indicators": summaries,
        "returned": len(summaries),
        "not_found": not_found,
    }


@mcp.tool()
async def export_indicators(
    query: str | None = None,
    theme: str | None = None,
    disaggregation_types: list[str] | None = None,
    disaggregation_values: list[str] | None = None,
) -> dict:
    """Export matching UNESCO UIS indicators to a CSV file.

    Use this tool whenever the user wants to save, download, export, or get a full
    list of indicators — even if they just say "give me all education indicators" or
    "save the results". Unlike search_indicators (which caps results at 50), this
    fetches every match with no limit and writes a CSV file to ~/Downloads/.

    At least one filter must be provided.

    After calling this tool, ALWAYS tell the user exactly:
    "Saved {row_count} indicators to: {saved_to}"

    Args:
        query: Full-text search on indicator name (FTS5 with stemming).
        theme: Exact theme code (from list_themes).
        disaggregation_types: List of disaggregation type codes. Indicators must support ALL listed types.
        disaggregation_values: List of disaggregation value codes. Indicators must match ALL listed values.

    Returns:
        {"saved_to": "/absolute/path/to/file.csv", "row_count": N}
    """
    if not any([query, theme, disaggregation_types, disaggregation_values]):
        return {"error": "At least one filter parameter must be provided."}

    rows = db_get_export_rows(
        query_term=query,
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
    )

    file_path = write_export_csv(rows)
    return {
        "saved_to": file_path,
        "row_count": len(rows),
    }


def main() -> None:
    """Run the MCP server over stdio by default."""
    mcp.run()


if __name__ == "__main__":
    main()
