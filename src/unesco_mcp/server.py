from datetime import datetime, timezone
from typing import Literal

from fastmcp import FastMCP
import unesco_reader as uis
import pandas as pd

#TODO: Add instructions
mcp = FastMCP("unesco-mcp")



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
    """Find all available UNESCO data themes.

    UIS themes are the highest-level topical groups used to organize indicators.
    Call this first to see what thematic areas exist before searching indicators.

    Returns:
        Dictionary with list with a dictionary of list of themes, the total count of themes, and
        a hint on how to use the theme codes to search for indicators in a specific theme.
        The list of themes includes the theme code, the date the theme was last updated,
        and a description of the update provided by UNESCO. Theme codes are generally uppercase and names seperated
        by a "_". For example "DEMOGRAPHIC_SOCIOECONOMIC" refers to the theme "Demographic & Socio-Economic"
    """

    themes = uis.available_themes(raw=True)

    return {
        "theme information": themes,
        "theme count": str(len(themes)),
        "hint": "Use the theme code with search_indicators to find indicators in a specific theme"
    }


def _parse_date(value: str | None) -> datetime | None:
    """Parse an ISO-8601 date string into a datetime, or return None."""
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _get_filter_indicators(theme_codes: str | list[str] = None,
                     min_start_year: int = None,
                     min_end_year: int = None,
                     geo_unit_type: Literal["NATIONAL", "REGIONAL", "ALL"] = None,
                     min_latest_update: str = None) -> pd.DataFrame:
    """Helper function to fetch indicators with specific filters"""

    # get count of indicators matching filters
    df = uis.available_indicators(theme=theme_codes, minStart=min_start_year, geoUnitType=geo_unit_type)
    if min_end_year is not None:
        df = df[df['timeLine_max'] >= min_end_year]
    if min_latest_update is not None:
        dt = _parse_date(min_latest_update)
        df = df[df['lastDataUpdate'] >= dt]

    return df


@mcp.tool()
async def count_indicators(theme_codes: str | list[str] = None,
                     min_start_year: int = None,
                     min_end_year: int = None,
                     geo_unit_type: Literal["NATIONAL", "REGIONAL", "ALL"] = None,
                     min_latest_update: str = None
                     ) -> dict:
    """Count stats for available indicators with filters. Use this tool to get a quick tally of available indicators
    with some basic filters including theme, minimum start year, minimum end year, geographical unit type,
    and minimum latest update date.

    This tool will return the count of indicators that match the specified filters, the total count of all indicators
    in the database, and the percentage of indicators that match the filters compared to the total.

    This can be useful to get a sense of the breadth of data available for a specific topic or time period,
    and to understand how many indicators meet certain criteria before diving into more detailed exploration.

    Args:
        theme_codes: A single theme code or a list of theme codes to filter indicators (optional).
           Use the list_themes tool to see available themes and their codes. Use the
           theme codes for this parameter. If not provided, counts all indicators across all themes.
        min_start_year: Minimum start year for indicators to be counted (optional). If not provided, no minimum start
            year filter is applied.
        min_end_year: Minimum end year for indicators to be counted (optional). If not provided, no minimum end year
            filter is applied.
        geo_unit_type: The geographical unit type to filter indicators by (optional).
            Indicators may contain data for one or more geographical units. Available values:
            - "NATIONAL": National/country level indicators
            - "REGIONAL": Regional level indicators (e.g. Sub-Saharan Africa, Latin America)
            - "ALL": Both national and regional indicators
            If not provided, no geographical unit type filter is applied.
        min_latest_update: Minimum latest update date for indicators to be counted (optional).
            If not provided, no filter on latest update

    Returns:
        Dictionary with the count stats for indicators matching the specified filters, including:
        - "count of selected indicators": The number of indicators that match the specified filters.
        - "count of all indicators": The total number of indicators available in the database without any filters.
        - "percentage of selected indicators compared to all indicators": The percentage of indicators that match
    """

    # get total count of indicators
    count_total = len(uis.available_indicators())

    count_i = len(_get_filter_indicators(theme_codes=theme_codes,
                           min_start_year=min_start_year,
                           min_end_year=min_end_year,
                           geo_unit_type=geo_unit_type,
                           min_latest_update=min_latest_update
                                         )
                  )

    # get percentage of indicators matching filters compared to total count
    pct = (count_i / count_total) * 100 if count_total > 0 else 0

    return {"count of selected indicators": count_i,
            "count of all indicators": count_total,
            "percentage of selected indicators compared to all indicators": f"{pct:.2f}%",

            # "hint": "" #TODO: Add hint
            }


@mcp.tool()
async def search_indicators(query: str = None,
                      theme_codes: str | list[str] = None,
                      min_start_year: int = None,
                      min_end_year: int = None,
                      geo_unit_type: Literal["NATIONAL", "REGIONAL", "ALL"] = None,
                      min_latest_update: str = None,
                      *,
                      limit: int = 50,
                      offset: int = 0) -> dict:
    """Search and discover UNESCO UIS indicators by keyword, theme, or other filters.

    Use this tool to find indicator codes you can use with other data-retrieval tools, as well as other
    basic information about the indicators. For more detailed metadata, use the get_indicator_metadata tool.
    Call list_themes first if you need valid theme codes. All parameters are optional, but
    using the filters as often as necessary will help you find relevant indicators faster.

    Filters are applied in this order: theme_codes, min_start_year, geo_unit_type
    (handled by the API), then min_end_year and min_latest_update (post-filter),
    and finally the query substring match. This means query only searches within
    indicators that already passed the other filters.

    Results are sorted by indicator code and paginated. If "has more results" is
    "True", pass the returned "next offset" value as the offset parameter to
    retrieve the next page.

    Typical workflows:
      - Browse a theme: search_indicators(theme_codes="EDUCATION") (this will return many results)
      - Keyword search: search_indicators(query="literacy")
      - Combined: search_indicators(query="enrollment", theme_codes="EDUCATION", min_end_year=2020)
      - Paginate: search_indicators(query="gender", offset=50)

    Args:
        query: Case-insensitive substring to match against indicator names.
            Example: "literacy", "enrollment", "GDP". Omit to skip name filtering.
        theme_codes: One or more theme codes to restrict the search.
            Pass a single string like "EDUCATION" or a list like ["EDUCATION", "SCIENCE"].
            Call list_themes to discover valid codes. Omit to search across all themes.
        min_start_year: Only return indicators whose data starts at or before this year.
            Example: 2000. Omit to skip this filter.
        min_end_year: Only return indicators whose data extends to at least this year.
            Example: 2023. Useful for finding indicators with recent data. Omit to skip.
        geo_unit_type: Filter by geographic coverage.
            "NATIONAL" for country-level data, "REGIONAL" for aggregated regions
            (e.g. Sub-Saharan Africa), "ALL" for both. Omit to skip.
        min_latest_update: Only return indicators updated on or after this date.
            ISO-8601 format, e.g. "2024-01-01". Useful for finding recently refreshed data. Omit to skip.
        limit: Page size — number of indicators to return (default 50, max 200).
        offset: Number of results to skip for pagination (default 0).
            Use the "next offset" value from a previous response to get the next page.

    Returns:
        Dictionary with:
        - "indicators": list of indicator objects, each containing indicatorCode, name,
          theme, lastDataUpdate, timeLine_min, timeLine_max, totalRecordCount, and geoUnitType.
        - "total count": total number of indicators matching all filters (before pagination).
        - "limit": the page size used.
        - "offset": the offset used.
        - "has more results": "True" or "False".
        - "next offset": the offset to use for the next page, or null if no more results.
    """

    # Clamp pagination values
    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    df = _get_filter_indicators(theme_codes=theme_codes,
                            min_start_year=min_start_year,
                            min_end_year=min_end_year,
                            geo_unit_type=geo_unit_type,
                            min_latest_update=min_latest_update
                             )

    # Filter by query if provided (plain substring match, not regex)
    if query:
        mask = df['name'].str.contains(query, case=False, na=False, regex=False)
        df = df[mask]

    # Sort for consistent pagination across calls
    df = df.sort_values('indicatorCode').reset_index(drop=True)

    # Get total before pagination
    total = len(df)

    # Apply pagination
    paginated_df = df.iloc[offset:offset + limit]

    # Convert to list of dicts
    indicators = paginated_df.to_dict(orient='records')

    has_more = offset + limit < total

    return {
        "indicators": indicators,
        "total count": str(total),
        "limit": str(limit),
        "offset": str(offset),
        "has more results": str(has_more),
        "next offset": str(offset + limit) if has_more else None,
        "hint": "If there are more results, use the next offset to get the next page of results. Adjust the limit and "
                "offset parameters to navigate through the results."
    }

def main() -> None:
    """Run the MCP server over stdio by default."""
    mcp.run()


if __name__ == "__main__":
    main()
