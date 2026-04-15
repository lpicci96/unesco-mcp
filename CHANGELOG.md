# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [0.1.0] - 2026-04-15

### Added
- MCP server with 13 tools for querying UNESCO UIS education and statistics data.
- **Discovery tools**: `list_themes`, `list_disaggregation_types`, `get_disaggregation_values`, `search_indicators`, `count_indicators`, `get_indicator_metadata`, `get_indicator_summary`.
- **Geography tool**: `search_geo_units` with regional grouping disambiguation via MCP elicitation.
- **Data retrieval tools**: `get_latest_value`, `get_time_series`, `get_country_ranking`, `compare_geographies`.
- **Utility tool**: `server_status` with DB refresh timestamp.
- Local SQLite cache with FTS5 full-text search for fast indicator and geography discovery.
- 24-hour TTL-based cache refresh for long-running server deployments.
- Database built fresh on startup, torn down on shutdown; `ensure_fresh()` guards on each tool call.
- Test suite with 115 tests covering both DB layer and server tools (88% coverage).
- CI workflow with Codecov integration.
- Prefect Horizon deployment support (writable temp directory for DB).
- Configuration module (`config.py`) for `DB_TTL_HOURS`, `MAX_RESULTS`, `MAX_RESULTS_CAP`, `MAX_SUMMARY_CODES`.

### Changed
- Upgraded from `fastmcp>=3.0.0rc1` to `fastmcp>=3.0.0` (stable release).
- Switched `unesco-reader` dependency from GitHub source to PyPI.

### Removed
- CSV export tools (`export_indicators`, `export_data`) — bulk data exports are not suited to the MCP conversational pattern. Users needing full datasets are directed to the UIS data portal or the `unesco-reader` package.

## [0.0.1] - 2026-02-15

### Added
- Initial package setup for PyPI namespace claim.
- Project metadata in `pyproject.toml`.
- Base package module under `src/unesco_mcp/`.
- Initial README and changelog.
