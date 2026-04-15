
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastmcp import FastMCP, Context
import unesco_reader as uis
from unesco_reader.exceptions import NoDataError, TooManyRecordsError

from unesco_mcp.config import MAX_RESULTS, MAX_RESULTS_CAP, MAX_SUMMARY_CODES
from unesco_mcp import uis_db


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Build the indicator DB on startup, tear it down on shutdown."""
    uis_db.build_db(fresh=True)
    try:
        yield
    finally:
        uis_db.teardown_db()


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

DISAGGREGATIONS ARE SEPARATE INDICATORS: Each disaggregated data series has its own
indicator code. "Literacy rate (female)" and "Literacy rate (total)" are different indicator
codes. The disaggregation filters in search_indicators are for *discovery* — finding the
right indicator code for the breakdown the user wants. Once you have the right indicator code,
pass it directly to get_latest_value / get_time_series / etc. There is no disaggregation
parameter on the data retrieval tools. The indicator code itself determines what breakdown
is returned.

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

6. GEOGRAPHY RESOLUTION — single clear rule for get_latest_value AND get_time_series:
   - OMIT geo_unit_code (do not pass it) in ALL cases EXCEPT when you already hold an
     exact, previously-confirmed code from a prior tool result (e.g. a code returned by
     search_geo_units after the user confirmed a grouping, or a raw ISO3 code the user
     typed directly like "KEN" or "ZWE"). When geo_unit_code is omitted the tool handles
     geography lookup and grouping disambiguation automatically.
   - NEVER guess, construct, or look up a geo_unit_code yourself — not even for regions
     the user named explicitly (e.g. "Sub-Saharan Africa", "East Asia"). Always omit
     geo_unit_code and let the tool do the lookup. Regional names map to multiple codes
     depending on grouping system (WB, SDG, UNICEF, etc.), and the tool must ask the
     user to choose. If you construct a code you will silently pick the wrong grouping.
   - If the tool returns "geography_disambiguation_required": ask the user which
     grouping system to use (present the available_groupings list), then call
     search_geo_units(query=region_name, region_group=<chosen>) to get the exact code,
     then retry the tool with that geo_unit_code.
   - NEVER call search_geo_units as a pre-step before get_latest_value or get_time_series
     unless the user explicitly asks to explore or list geographies.

7. DATA RETRIEVAL — choose the right tool:
   - Single data point (latest or specific year) → get_latest_value
   - Trend over time / full time series → get_time_series
     (same geo elicitation pattern as get_latest_value; accepts start_year / end_year)
   - Best/worst countries globally → get_country_ranking
     (no geography argument needed; returns top-N and bottom-N countries only, not regions)
   - Compare a specific set of countries or regions → compare_geographies
     (pass pre-confirmed geo_unit_codes; supports up to 20 codes; ranks by value)

8. CSV EXPORTS — choose the right export tool:
   - export_indicators: Exports the indicator *catalog* (names, codes, themes, coverage).
     Use when the user wants to save or download a list of indicators. At least one filter
     required. After calling, ALWAYS tell the user: "Saved {row_count} indicators to: {saved_to}"
   - export_data: Exports actual *numeric data values* (time series) to CSV.
     TWO PATTERNS:
     (a) ALL INDICATORS FOR A GEOGRAPHY: omit indicator_codes, provide geo_unit_codes or
         geo_unit_type. E.g. export_data(geo_unit_codes=["KEN"]) fetches every indicator
         for Kenya. This will NOT hit API limits.
     (b) SPECIFIC INDICATORS: provide indicator_codes (from search_indicators), optionally
         scoped to a geography. To find the right codes, use search_indicators FIRST —
         prioritise structured filters (theme, disaggregation_types, disaggregation_values)
         over the text query parameter, as they are far more reliable for topic discovery.
     At least one of indicator_codes or a geography filter is required.
     After calling, ALWAYS tell the user: "Saved {row_count} data rows to: {saved_to}"
   Trigger words for either: "save", "download", "export", "give me all", "full list", "complete
   list". Choose based on whether the user wants indicator metadata or actual data values.

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

    uis_db.ensure_fresh()
    themes = uis_db.get_themes()

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

    uis_db.ensure_fresh()
    rows = uis_db.query("SELECT type_code, type_name FROM disaggregation_types ORDER BY type_name")

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

    uis_db.ensure_fresh()

    # Verify the type exists
    type_rows = uis_db.query(
        "SELECT type_code, type_name FROM disaggregation_types WHERE type_code = ?",
        (type_code,),
    )

    if not type_rows:
        return {"error": f"Disaggregation type '{type_code}' not found. Use list_disaggregation_types to see available types."}

    type_info = type_rows[0]

    values = uis_db.query(
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

    uis_db.ensure_fresh()
    effective_limit = min(max(limit, 1), MAX_RESULTS_CAP)

    results, total = uis_db.search_indicators(
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
    uis_db.ensure_fresh()
    count = uis_db.count_indicators(
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

    uis_db.ensure_fresh()

    summaries = uis_db.get_indicator_summaries(indicator_codes)
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

    uis_db.ensure_fresh()
    rows = uis_db.get_export_rows(
        query_term=query,
        theme=theme,
        disaggregation_types=disaggregation_types,
        disaggregation_values=disaggregation_values,
    )

    file_path = uis_db.write_export_csv(rows)
    return {
        "saved_to": file_path,
        "row_count": len(rows),
    }


@mcp.tool()
async def export_data(
    indicator_codes: list[str] | None = None,
    geo_unit_codes: list[str] | None = None,
    geo_unit_type: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """Export actual numeric data values for UNESCO UIS indicators to a CSV file.

    Use this tool when the user wants to download or save the data itself (time series
    values by year and geography) — not the indicator catalog. For exporting the indicator
    catalog (names, codes, themes), use export_indicators instead.

    TWO USAGE PATTERNS:

    1. ALL INDICATORS FOR A GEOGRAPHY — omit indicator_codes, provide geography:
       export_data(geo_unit_codes=["KEN"])  →  all indicators for Kenya
       export_data(geo_unit_type="NATIONAL")  →  all indicators for all countries
       This does NOT hit API limits; the API returns one geography's full dataset.

    2. SPECIFIC INDICATORS ACROSS GEOGRAPHIES — provide indicator_codes, omit or specify geography:
       export_data(indicator_codes=["LR.1", "LR.2"])  →  those indicators for all geographies
       export_data(indicator_codes=["LR.1"], geo_unit_type="NATIONAL")  →  national only
       To find indicator codes for a topic, call search_indicators FIRST using structured
       filters (theme, disaggregation_types, disaggregation_values) — these are far more
       reliable than the text query. Only use query as a secondary refinement. Then pass the
       returned codes here.

    At least one of indicator_codes or a geography filter must be provided.
    geo_unit_codes and geo_unit_type are mutually exclusive (max 20 geo_unit_codes).

    After calling this tool, ALWAYS tell the user:
    "Saved {row_count} data rows to: {saved_to}"

    Args:
        indicator_codes: Optional list of indicator codes. Omit to fetch all indicators for
                         the given geography. Use search_indicators to find codes first.
        geo_unit_codes: Optional list of specific geo unit codes (max 20).
                        Mutually exclusive with geo_unit_type.
        geo_unit_type: Optional. "NATIONAL" for all countries, "REGIONAL" for all regions.
                       Mutually exclusive with geo_unit_codes.
        start_year: Optional. First year to include (inclusive).
        end_year: Optional. Last year to include (inclusive).

    Returns:
        {"saved_to": "/absolute/path/to/file.csv", "row_count": N}
    """
    has_indicators = bool(indicator_codes)
    has_geography = bool(geo_unit_codes) or geo_unit_type is not None
    if not has_indicators and not has_geography:
        return {"error": "At least one of indicator_codes or a geography filter (geo_unit_codes or geo_unit_type) must be provided. Fetching all UNESCO data at once is not supported."}

    if geo_unit_codes and geo_unit_type:
        return {"error": "geo_unit_codes and geo_unit_type are mutually exclusive. Provide one or the other, not both."}

    if geo_unit_codes and len(geo_unit_codes) > 20:
        return {"error": f"geo_unit_codes is capped at 20 entries. {len(geo_unit_codes)} were provided. Use geo_unit_type='NATIONAL' to fetch all countries instead."}

    try:
        df = uis.get_data(
            indicator=indicator_codes,
            geoUnit=geo_unit_codes,
            geoUnitType=geo_unit_type,
            start=start_year,
            end=end_year,
            labels=True,
        )
    except NoDataError:
        return {
            "error": (
                "No data found for the given filters. "
                "Check that the indicator codes and geography codes are valid and that data "
                "exists for the specified combination and year range."
            )
        }
    except TooManyRecordsError:
        return {
            "error": (
                "Too many records returned by the API. Narrow the request by passing "
                "fewer indicator_codes, using geo_unit_type instead of no geography filter, "
                "passing fewer geo_unit_codes, or adding a tighter year range."
            )
        }

    rows = [
        {
            "indicator_code": str(row["indicatorId"]),
            "indicator_name": str(row["name"]),
            "geo_unit_code": str(row["geoUnit"]),
            "geo_unit_name": str(row["geoUnitName"]),
            "year": int(row["year"]),
            "value": round(float(row["value"]), 6) if str(row.get("value", "nan")) != "nan" else None,
            "qualifier": _safe_qualifier(row),
        }
        for _, row in df.iterrows()
    ]

    file_path = uis_db.write_data_csv(rows)
    return {
        "saved_to": file_path,
        "row_count": len(rows),
    }


_AI_CHOOSE = "Let AI choose the most appropriate"

# Priority order for AI-recommended grouping when the user defers the choice.
# SDG and UIS align most closely with UNESCO's reporting frameworks; WB and ECA
# are broader economic groupings; GPE is education-finance-specific.
_GROUP_PRIORITY = ["SDG", "UIS", "UNICEF", "MDG", "UN", "WB", "ECA", "GPE"]


def _pick_recommended_group(available: list[str]) -> str:
    """Return the highest-priority group present in *available*."""
    for g in _GROUP_PRIORITY:
        if g in available:
            return g
    return available[0]


@mcp.tool()
async def search_geo_units(
    ctx: Context,
    query: str,
    type_filter: str | None = None,
    region_group: str | None = None,
) -> dict:
    """Search for UNESCO UIS geographic units (countries and regions) by name or code.

    Use this to find the geo unit code needed for get_latest_value. Accepts either
    a country/region name (e.g. "Kenya", "Sub-Saharan Africa") or an ISO3 code
    directly (e.g. "KEN", "ZWE"). When multiple grouping systems match a region
    (WB, SDG, UNICEF, etc.), the tool will elicit a choice from the user directly.

    Args:
        query: Country or region name, or ISO3 code (e.g. "Kenya", "ZWE", "Sub-Saharan Africa").
        type_filter: Optional. "NATIONAL" to show only countries, "REGIONAL" for aggregates only.
        region_group: Optional. Restrict to a specific grouping system (e.g. "WB", "SDG", "UNICEF").

    Returns:
        A dictionary with:
            - "geo_units": List of matches, each with code, name, type, and region_group.
            - "count": Number of results returned.
            - "hint": Guidance on next steps.
    """
    uis_db.ensure_fresh()
    results = uis_db.search_geo_units(
        query_term=query,
        type_filter=type_filter,
        region_group=region_group,
    )

    # Detect ambiguity: regional results with more than one distinct grouping system.
    regional = [r for r in results if r["type"] == "REGIONAL"]
    unique_groups: list[str] = []
    seen_groups: set[str] = set()
    for r in regional:
        g = r.get("region_group") or ""
        if g and g not in seen_groups:
            seen_groups.add(g)
            unique_groups.append(g)

    if len(unique_groups) > 1:
        # Try to elicit a grouping choice from the user.
        choices = unique_groups + [_AI_CHOOSE]
        # Identify the single region name for the prompt (use the first regional result's name).
        region_name = regional[0]["name"] if regional else query
        try:
            elicit_result = await ctx.elicit(
                f"'{region_name}' exists in multiple regional grouping systems: "
                f"{', '.join(unique_groups)}.\n"
                f"Which grouping would you like to use? "
                f"If unsure, choose '{_AI_CHOOSE}'.",
                response_type=choices,
            )
        except Exception as exc:  # noqa: BLE001 — elicitation failure is non-fatal
            # Client does not support MCP elicitation (or it failed) — fall back to text.
            rec = _pick_recommended_group(unique_groups)
            return {
                "geo_units": results,
                "count": len(results),
                "requires_user_choice": True,
                "available_groupings": unique_groups,
                "ai_recommended_group": rec,
                "elicitation_error": f"{type(exc).__name__}: {exc}",
                "hint": (
                    f"Elicitation failed ({type(exc).__name__}: {exc}). "
                    f"IMPORTANT: Before calling get_latest_value, you MUST ask the user "
                    f"which grouping system to use: {', '.join(unique_groups)}. "
                    f"If the user is unsure, suggest '{rec}' as it aligns best with "
                    f"UNESCO's reporting frameworks. Do NOT proceed without confirming."
                ),
            }

        if elicit_result is not None and elicit_result.action == "accept":
            chosen = elicit_result.data
            if chosen == _AI_CHOOSE:
                # Return all results but flag the recommended grouping.
                rec = _pick_recommended_group(unique_groups)
                return {
                    "geo_units": results,
                    "count": len(results),
                    "ai_recommended_group": rec,
                    "hint": (
                        f"Use the 'code' field as geo_unit_code in get_latest_value. "
                        f"The AI-recommended grouping is '{rec}' as it aligns best with "
                        f"UNESCO's reporting frameworks."
                    ),
                }
            else:
                # Filter to the chosen grouping only.
                filtered = [r for r in results if r.get("region_group") == chosen or r["type"] == "NATIONAL"]
                return {
                    "geo_units": filtered,
                    "count": len(filtered),
                    "hint": (
                        f"Filtered to '{chosen}' grouping. "
                        f"Use the 'code' field as geo_unit_code in get_latest_value."
                    ),
                }
        # User declined/cancelled — return everything and prompt manual choice.

    hint = "Use the 'code' field as the geo_unit_code in get_latest_value."
    if len(unique_groups) > 1:
        rec = _pick_recommended_group(unique_groups)
        hint += (
            f" Multiple regional grouping systems found ({', '.join(unique_groups)}). "
            f"Ask the user which grouping to use, or suggest '{rec}' for UNESCO education data."
        )

    return {
        "geo_units": results,
        "count": len(results),
        "hint": hint,
    }

def _rows_to_ranking(subset) -> list[dict]:
    """Format a ranked DataFrame slice into a list of dicts."""
    out = []
    for _, row in subset.iterrows():
        out.append({
            "rank": int(row["rank"]),
            "code": str(row["geoUnit"]),
            "name": str(row["geoUnitName"]),
            "value": round(float(row["value"]), 6),
        })
    return out


def _safe_qualifier(row) -> str | None:
    """Return the qualifier string, or None if absent / NaN."""
    q = row.get("qualifier")
    if q and str(q) != "nan":
        return str(q)
    return None


@mcp.tool()
async def get_time_series(
    ctx: Context,
    indicator_code: str,
    geo_unit_code: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """Get the full time series for a UNESCO UIS indicator for one country or region.

    Returns all data points for the indicator × geography combination, optionally
    filtered to a year range. Ideal for trend analysis and time-series visualisation.

    IMPORTANT — when to omit geo_unit_code:
    Omit geo_unit_code in ALL cases except when you hold an exact, previously-confirmed
    code from a prior tool result. NEVER construct or guess a code yourself — regional
    names (e.g. "Sub-Saharan Africa") map to multiple codes across grouping systems
    (WB, SDG, UNICEF, etc.) and the wrong one gives wrong data. When omitted, the tool
    handles geography lookup and grouping disambiguation automatically.

    If the tool returns error="geography_disambiguation_required", ask the user which
    grouping system to use, then call search_geo_units(query=region_name,
    region_group=<chosen>) to get the exact code, and retry with that geo_unit_code.

    Args:
        indicator_code: The indicator code (e.g. "LR.AG15T99").
        geo_unit_code: Optional. Only pass a confirmed code from a prior tool result.
                       Omit to trigger geography lookup and disambiguation.
        start_year: Optional. First year to include (inclusive).
        end_year: Optional. Last year to include (inclusive).

    Returns:
        A dictionary with:
            - "indicator_code", "indicator_name": Indicator identity.
            - "geo_unit_code", "geo_unit_name": Geography identity.
            - "data_points": Chronological list of {year, value, qualifier}.
            - "summary": {total_data_points, min_value, max_value, latest_year, note}.
    """
    if geo_unit_code is None:
        try:
            scope_result = await ctx.elicit(
                "No geography was specified. Would you like to look up data for a specific "
                "country or region, or get the global value (World)?",
                response_type=["Specify a country or region", "Get global value (World)"],
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "error": "Geography not specified and elicitation is unavailable. "
                         "Please provide a geo_unit_code.",
                "elicitation_error": f"{type(exc).__name__}: {exc}",
            }

        if scope_result.action != "accept":
            return {"error": "No geography provided. Please specify a geo_unit_code."}

        if scope_result.data == "Get global value (World)":
            search_query = "World"
        else:
            try:
                name_result = await ctx.elicit(
                    "Enter the name of the country or region (e.g. 'Kenya', 'Sub-Saharan Africa'):",
                    response_type=str,
                )
            except Exception as exc:  # noqa: BLE001
                return {
                    "error": "Could not elicit geography name. Please provide a geo_unit_code directly.",
                    "elicitation_error": f"{type(exc).__name__}: {exc}",
                }

            if name_result.action != "accept":
                return {"error": "No geography provided. Please specify a geo_unit_code."}

            search_query = name_result.data.strip()

        geo_results = uis_db.search_geo_units(query_term=search_query)
        if not geo_results:
            return {
                "error": f"No geographic unit found matching '{search_query}'. "
                         "Use search_geo_units to explore available geographies."
            }

        resolved = await _resolve_geo_unit(ctx, geo_results, search_query)
        if resolved is None:
            return {"error": "Could not resolve geography. Please provide a geo_unit_code directly."}
        if resolved.get("_disambiguation_required"):
            groups = resolved["available_groupings"]
            rec = resolved["ai_recommended_group"]
            name = resolved["region_name"]
            return {
                "error": "geography_disambiguation_required",
                "region_name": name,
                "available_groupings": groups,
                "recommended_grouping": rec,
                "instruction": (
                    f"'{name}' exists in multiple regional grouping systems: "
                    f"{', '.join(groups)}. You MUST ask the user which grouping to use. "
                    f"Suggest '{rec}' as a sensible default (aligns with UNESCO/SDG reporting). "
                    f"Once the user chooses, call search_geo_units(query='{name}', "
                    f"region_group='<chosen>') to get the exact code, then retry this tool "
                    f"with geo_unit_code set to that code."
                ),
            }

        geo_unit_code = resolved["code"]

    try:
        df = uis.get_data(
            indicator=indicator_code,
            geoUnit=geo_unit_code,
            start=start_year,
            end=end_year,
            labels=True,
        )
    except NoDataError:
        return {
            "error": f"No data found for indicator '{indicator_code}' and geography '{geo_unit_code}'. "
                     "Check that both codes are valid and that this indicator covers this geography."
        }
    except TooManyRecordsError:
        return {
            "error": "Too many records returned. Try narrowing with start_year and/or end_year."
        }

    df = df.sort_values("year", ascending=True)

    data_points = [
        {
            "year": int(row["year"]),
            "value": round(float(row["value"]), 6),
            "qualifier": _safe_qualifier(row),
        }
        for _, row in df.iterrows()
        if not (str(row.get("value", "nan")) == "nan")
    ]

    values = [p["value"] for p in data_points]
    first_row = df.iloc[0]

    return {
        "indicator_code": str(first_row["indicatorId"]),
        "indicator_name": str(first_row["name"]),
        "geo_unit_code": str(first_row["geoUnit"]),
        "geo_unit_name": str(first_row["geoUnitName"]),
        "data_points": data_points,
        "summary": {
            "total_data_points": len(data_points),
            "min_value": round(min(values), 6) if values else None,
            "max_value": round(max(values), 6) if values else None,
            "latest_year": data_points[-1]["year"] if data_points else None,
            "note": (
                f"Data from {data_points[0]['year']} to {data_points[-1]['year']}."
                if len(data_points) >= 2
                else "Single data point available."
            ),
        },
    }


@mcp.tool()
async def get_country_ranking(
    indicator_code: str,
    year: int | None = None,
    top_n: int = 10,
    bottom_n: int = 10,
    strict_year: bool = True,
) -> dict:
    """Rank countries (not regions) by their value for a UNESCO UIS indicator.

    Returns the top-N and bottom-N countries for a given indicator in a specific year.
    Uses dense ranking (tied countries share the same rank).

    Year handling:
    - strict_year=True (default): year must be provided explicitly.
    - strict_year=False and year omitted: uses the year with the most country coverage.

    Args:
        indicator_code: The indicator code (e.g. "LR.AG15T99").
        year: Optional. The year to rank countries for. If omitted, the year with
              the most data points is used only when strict_year=False.
        top_n: Number of top-ranked countries to return (default 10, max 200).
        bottom_n: Number of bottom-ranked countries to return (default 10, max 200).
        strict_year: If True, require an explicit year to avoid implicit year selection.

    Returns:
        A dictionary with:
            - "indicator_code", "indicator_name": Indicator identity.
            - "year_used": The year the ranking is based on.
            - "total_countries_with_data": Countries with non-null values that year.
            - "top": [{rank, code, name, value}, ...] — highest-value countries.
            - "bottom": [{rank, code, name, value}, ...] — lowest-value countries.
            - "note": Context (e.g. year selection rationale, overlap explanation).
    """
    top_n = max(1, min(top_n, 200))
    bottom_n = max(1, min(bottom_n, 200))

    if strict_year and year is None:
        return {
            "error": (
                "strict_year=True requires an explicit 'year'. "
                "Provide year=<YYYY>, or set strict_year=False to auto-select the year "
                "with the most country coverage."
            )
        }

    try:
        df = uis.get_data(
            indicator=indicator_code,
            geoUnitType="NATIONAL",
            start=year,
            end=year,
            labels=True,
        )
    except NoDataError:
        return {
            "error": f"No national-level data found for indicator '{indicator_code}'"
                     + (f" in {year}." if year else ".")
        }
    except TooManyRecordsError:
        return {
            "error": "Too many records returned. Try specifying a year."
        }

    df = df.dropna(subset=["value"])

    if df.empty:
        return {"error": f"No non-null data found for indicator '{indicator_code}'."}

    if year is None:
        year_counts = df["year"].value_counts()
        year_used = int(year_counts.index[0])
        note = f"No year specified — using {year_used} (year with most country coverage)."
    else:
        year_used = year
        note = f"Showing data for {year_used}."

    df = df[df["year"] == year_used].copy()

    if df.empty:
        return {"error": f"No data for indicator '{indicator_code}' in {year_used}."}

    df = df.sort_values("value", ascending=False).reset_index(drop=True)
    df["rank"] = df["value"].rank(method="dense", ascending=False).astype(int)
    total = len(df)

    indicator_name = str(df.iloc[0]["name"])

    if total <= top_n + bottom_n:
        return {
            "indicator_code": indicator_code,
            "indicator_name": indicator_name,
            "year_used": year_used,
            "total_countries_with_data": total,
            "top": _rows_to_ranking(df),
            "bottom": [],
            "note": (
                note + f" Only {total} countries have data — all are shown in 'top'; "
                "'bottom' is empty to avoid duplication."
            ),
        }

    top_df = df.head(top_n)
    bottom_df = df.tail(bottom_n)

    return {
        "indicator_code": indicator_code,
        "indicator_name": indicator_name,
        "year_used": year_used,
        "total_countries_with_data": total,
        "top": _rows_to_ranking(top_df),
        "bottom": _rows_to_ranking(bottom_df),
        "note": note,
    }


@mcp.tool()
async def compare_geographies(
    indicator_code: str,
    geo_unit_codes: list[str],
    year: int | None = None,
    strict_year: bool = True,
) -> dict:
    """Compare a UNESCO UIS indicator across a specific list of countries or regions.

    Retrieves the indicator value for each supplied geo unit code and ranks them
    by value. Use this to directly compare a set of countries or regions you already
    know the codes for (e.g. from prior searches or elicitations).

    Year handling:
    - strict_year=True (default): year must be provided; if a geography has no value in
      that year, it is reported in missing_codes (no fallback year used).
    - strict_year=False:
      - year provided: falls back to the nearest available year per geography.
      - year omitted: uses the most recent available year per geography (mixed years possible).

    Args:
        indicator_code: The indicator code (e.g. "LR.AG15T99").
        geo_unit_codes: List of geo unit codes to compare (max 20, e.g. ["KEN", "TZA", "UGA"]).
                        Codes must already be known — use search_geo_units to find them.
        year: Optional. The year to compare. If omitted, the most recent available
              value for each geography is used only when strict_year=False.
        strict_year: If True, require an explicit year and disallow fallback years.

    Returns:
        A dictionary with:
            - "indicator_code", "indicator_name": Indicator identity.
            - "comparison": [{rank, code, name, value, year, qualifier}, ...] sorted by value desc.
            - "missing_codes": Geo unit codes for which no data was found.
            - "note": Context (e.g. mixed years warning, missing codes).
    """
    if not geo_unit_codes:
        return {"error": "geo_unit_codes must be a non-empty list."}

    if strict_year and year is None:
        return {
            "error": (
                "strict_year=True requires an explicit 'year'. "
                "Provide year=<YYYY>, or set strict_year=False to allow per-geography "
                "fallback years."
            )
        }

    # Deduplicate preserving order, cap at 20.
    seen: set[str] = set()
    unique_codes: list[str] = []
    for c in geo_unit_codes:
        if c not in seen:
            seen.add(c)
            unique_codes.append(c)

    if len(unique_codes) > 20:
        return {"error": "Maximum 20 geo unit codes allowed per request."}

    try:
        df = uis.get_data(
            indicator=indicator_code,
            geoUnit=unique_codes,
            start=year,
            end=year,
            labels=True,
        )
    except NoDataError:
        return {
            "error": f"No data found for indicator '{indicator_code}' with the provided geo unit codes."
        }
    except TooManyRecordsError:
        return {
            "error": "Too many records returned. Try specifying a year."
        }

    indicator_name = str(df.iloc[0]["name"]) if not df.empty else indicator_code

    # For each requested code, pick the relevant row.
    rows: list[dict] = []
    missing_codes: list[str] = []
    substituted_years: list[str] = []

    for code in unique_codes:
        sub = df[df["geoUnit"] == code]
        if sub.empty:
            missing_codes.append(code)
            continue

        if year is not None:
            filtered = sub[sub["year"] == year]
            if not filtered.empty:
                row = filtered.iloc[0]
            elif strict_year:
                missing_codes.append(code)
                continue
            else:
                nearest = min(sub["year"].tolist(), key=lambda y: abs(int(y) - year))
                row = sub[sub["year"] == nearest].iloc[0]
                substituted_years.append(f"{code}:{year}->{int(nearest)}")
        else:
            row = sub.sort_values("year").iloc[-1]

        val = row.get("value")
        if val is None or str(val) == "nan":
            missing_codes.append(code)
            continue

        rows.append({
            "code": str(row["geoUnit"]),
            "name": str(row["geoUnitName"]),
            "value": round(float(val), 6),
            "year": int(row["year"]),
            "qualifier": _safe_qualifier(row),
        })

    # Sort descending and assign dense ranks in pure Python.
    rows.sort(key=lambda r: r["value"], reverse=True)
    rank = 1
    for i, r in enumerate(rows):
        if i > 0 and r["value"] < rows[i - 1]["value"]:
            rank = i + 1
        r["rank"] = rank

    # Build note.
    notes: list[str] = []
    if missing_codes:
        notes.append(f"No data found for: {', '.join(missing_codes)}.")
    if substituted_years:
        notes.append(
            "Requested year substitutions used: " + ", ".join(substituted_years) + "."
        )
    if year is None and rows:
        years_used = {r["year"] for r in rows}
        if len(years_used) > 1:
            notes.append(
                f"Mixed years used (most recent per geography): "
                + ", ".join(f"{r['code']}={r['year']}" for r in rows)
                + "."
            )

    return {
        "indicator_code": indicator_code,
        "indicator_name": indicator_name,
        "comparison": rows,
        "missing_codes": missing_codes,
        "note": " ".join(notes) if notes else None,
    }


async def _resolve_geo_unit(ctx: Context, results: list[dict], query: str) -> dict | None:
    """Resolve a list of geo unit search results to a single unit.

    When multiple grouping systems are present (e.g. WB, SDG, UNICEF for the same
    region name), elicits a choice from the user. On elicitation failure or cancel,
    falls back to the highest-priority recommended group.

    Returns the resolved geo unit dict, or None if there are no results.
    """
    uis_db.ensure_fresh()
    if not results:
        return None
    if len(results) == 1:
        return results[0]

    # Collect distinct grouping systems from regional results, preserving encounter order.
    regional = [r for r in results if r["type"] == "REGIONAL"]
    unique_groups: list[str] = []
    seen: set[str] = set()
    for r in regional:
        g = r.get("region_group") or ""
        if g and g not in seen:
            seen.add(g)
            unique_groups.append(g)

    chosen_group: str | None = None

    if len(unique_groups) > 1:
        region_name = regional[0]["name"] if regional else query
        try:
            elicit_result = await ctx.elicit(
                f"'{region_name}' exists in multiple regional grouping systems: "
                f"{', '.join(unique_groups)}.\n"
                f"Which grouping would you like to use? "
                f"If unsure, choose '{_AI_CHOOSE}'.",
                response_type=unique_groups + [_AI_CHOOSE],
            )
            if elicit_result.action == "accept":
                chosen_group = elicit_result.data
        except Exception:  # noqa: BLE001
            # Client doesn't support elicitation — signal callers to surface the
            # ambiguity themselves rather than silently picking a group.
            rec = _pick_recommended_group(unique_groups)
            return {
                "_disambiguation_required": True,
                "region_name": region_name,
                "available_groupings": unique_groups,
                "ai_recommended_group": rec,
            }

        if chosen_group is None:
            # User cancelled the elicitation dialog.
            rec = _pick_recommended_group(unique_groups)
            return {
                "_disambiguation_required": True,
                "region_name": region_name,
                "available_groupings": unique_groups,
                "ai_recommended_group": rec,
            }

        if chosen_group == _AI_CHOOSE:
            chosen_group = _pick_recommended_group(unique_groups)

        filtered = [r for r in results if r.get("region_group") == chosen_group]
        if filtered:
            # Within the chosen group, prefer an exact name match over a partial one.
            exact = [r for r in filtered if r["name"].lower() == query.lower()]
            return exact[0] if exact else filtered[0]

    # Single grouping or purely national results — prefer exact name match, else first.
    exact = [r for r in results if r["name"].lower() == query.lower()]
    return exact[0] if exact else results[0]


@mcp.tool()
async def get_latest_value(
    ctx: Context,
    indicator_code: str,
    geo_unit_code: str | None = None,
    year: int | None = None,
) -> dict:
    """Get the value of a UNESCO UIS indicator for a specific country or region.

    Returns a single data point — either the most recent available value, or the
    value for a specific year. Use this for answering questions like:
    "What is the literacy rate in Kenya?" or "What was the completion rate in
    Sub-Saharan Africa in 2015?"

    IMPORTANT — when to omit geo_unit_code:
    Omit geo_unit_code in ALL cases except when you hold an exact, previously-confirmed
    code from a prior tool result (e.g. "KEN" typed directly by the user, or a code
    returned by search_geo_units after the user confirmed a grouping). NEVER construct or
    guess a code yourself, even for regions the user named explicitly. When omitted, the
    tool handles lookup and grouping disambiguation automatically.

    If the tool returns error="geography_disambiguation_required", ask the user which
    grouping system to use (show available_groupings), then call
    search_geo_units(query=region_name, region_group=<chosen>) to get the exact code,
    and retry this tool with geo_unit_code set to that code.

    To find indicator codes, use search_indicators. Always show the user the year
    alongside the value, since data is not always available for the most recent years.

    Args:
        indicator_code: The indicator code (e.g. "CR.1", "LR.AG15T99").
        geo_unit_code: Optional. Only pass a confirmed code from a prior tool result
                       (e.g. "KEN"). Omit to trigger geography lookup and disambiguation.
        year: Optional. The specific year to retrieve. If omitted, returns the
              most recent available value. If no data exists for the requested
              year, returns the nearest available year instead, with a note.

    Returns:
        A dictionary with:
            - "indicator_code": The indicator code.
            - "indicator_name": Full name of the indicator.
            - "geo_unit_code": The geo unit code.
            - "geo_unit_name": Human-readable geography name.
            - "year": The year of the returned value.
            - "value": The numeric data value.
            - "qualifier": Data quality flag if present (e.g. "<", "~"), else null.
            - "note": Context about the data point (e.g. year range, year substitution).
    """
    if geo_unit_code is None:
        # Step 1 — ask the user whether to specify a geography or use the global value.
        try:
            scope_result = await ctx.elicit(
                "No geography was specified. Would you like to look up data for a specific "
                "country or region, or get the global value (World)?",
                response_type=["Specify a country or region", "Get global value (World)"],
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "error": "Geography not specified and elicitation is unavailable. "
                         "Please provide a geo_unit_code. Use search_geo_units to find the correct code.",
                "elicitation_error": f"{type(exc).__name__}: {exc}",
            }

        if scope_result.action != "accept":
            return {"error": "No geography provided. Please specify a geo_unit_code."}

        if scope_result.data == "Get global value (World)":
            search_query = "World"
        else:
            # Step 2 — elicit the country or region name.
            try:
                name_result = await ctx.elicit(
                    "Enter the name of the country or region (e.g. 'Kenya', 'Sub-Saharan Africa'):",
                    response_type=str,
                )
            except Exception as exc:  # noqa: BLE001
                return {
                    "error": "Could not elicit geography name. Please provide a geo_unit_code directly.",
                    "elicitation_error": f"{type(exc).__name__}: {exc}",
                }

            if name_result.action != "accept":
                return {"error": "No geography provided. Please specify a geo_unit_code."}

            search_query = name_result.data.strip()

        # Step 3 — look up the geography, resolving grouping ambiguity via elicitation.
        geo_results = uis_db.search_geo_units(query_term=search_query)
        if not geo_results:
            return {
                "error": f"No geographic unit found matching '{search_query}'. "
                         "Use search_geo_units to explore available geographies."
            }

        resolved = await _resolve_geo_unit(ctx, geo_results, search_query)
        if resolved is None:
            return {"error": "Could not resolve geography. Please provide a geo_unit_code directly."}
        if resolved.get("_disambiguation_required"):
            groups = resolved["available_groupings"]
            rec = resolved["ai_recommended_group"]
            name = resolved["region_name"]
            return {
                "error": "geography_disambiguation_required",
                "region_name": name,
                "available_groupings": groups,
                "recommended_grouping": rec,
                "instruction": (
                    f"'{name}' exists in multiple regional grouping systems: "
                    f"{', '.join(groups)}. You MUST ask the user which grouping to use. "
                    f"Suggest '{rec}' as a sensible default (aligns with UNESCO/SDG reporting). "
                    f"Once the user chooses, call search_geo_units(query='{name}', "
                    f"region_group='<chosen>') to get the exact code, then retry this tool "
                    f"with geo_unit_code set to that code."
                ),
            }

        geo_unit_code = resolved["code"]

    try:
        df = uis.get_data(indicator=indicator_code, geoUnit=geo_unit_code, labels=True)
    except NoDataError:
        return {
            "error": f"No data found for indicator '{indicator_code}' and geography '{geo_unit_code}'. "
                     "Check that both codes are valid and that this indicator covers this geography type."
        }

    year_min = int(df["year"].min())
    year_max = int(df["year"].max())
    available_years = sorted(df["year"].tolist())

    if year is not None:
        row = df[df["year"] == year]
        if row.empty:
            # Find the nearest available year to the requested one
            nearest = min(available_years, key=lambda y: abs(y - year))
            row = df[df["year"] == nearest]
            note = (
                f"No data available for {year}. Showing nearest available year ({nearest}). "
                f"Data exists from {year_min} to {year_max}."
            )
        else:
            note = f"Data exists from {year_min} to {year_max}."
    else:
        row = df[df["year"] == year_max]
        note = f"Most recent available year. Data exists from {year_min} to {year_max}."

    record = row.iloc[0]
    qualifier = record["qualifier"] if record["qualifier"] and str(record["qualifier"]) != "nan" else None

    return {
        "indicator_code": record["indicatorId"],
        "indicator_name": record["name"],
        "geo_unit_code": record["geoUnit"],
        "geo_unit_name": record["geoUnitName"],
        "year": int(record["year"]),
        "value": round(float(record["value"]), 6),
        "qualifier": qualifier,
        "note": note,
    }


def main() -> None:
    """Run the MCP server over stdio by default."""
    mcp.run()


if __name__ == "__main__":
    main()
