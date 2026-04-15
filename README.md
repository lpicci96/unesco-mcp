[![codecov](https://codecov.io/gh/lpicci96/unesco-mcp/graph/badge.svg?token=G4HJPSNA0E)](https://codecov.io/gh/lpicci96/unesco-mcp)
[![PyPI](https://img.shields.io/pypi/v/unesco-mcp)](https://pypi.org/project/unesco-mcp/)
[![Python](https://img.shields.io/pypi/pyversions/unesco-mcp)](https://pypi.org/project/unesco-mcp/)

# unesco-mcp

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for UNESCO Institute for Statistics (UIS) data.
Bring the [UIS Data Browser](https://databrowser.uis.unesco.org/browser) 
into any MCP-compatible client (Claude Desktop, Claude Code, Cursor, Windsurf, etc.).

## What it does

This server connects AI assistants to the [UIS API](https://api.uis.unesco.org/api/public/documentation/), enabling them 
to search indicators, retrieve data values, compare countries, and explore available breakdowns — all through natural conversation. 
Data is cached locally in SQLite for fast indicator discovery, while live API calls fetch the actual data values.


## Available tools

### Discovery

| Tool | Description |
|------|-------------|
| `list_themes` | List all UNESCO data themes (education, science, culture, etc.) |
| `list_disaggregation_types` | List available data breakdowns (by sex, age, education level, etc.) |
| `get_disaggregation_values` | Get specific values for a breakdown type (e.g. "Male", "Female" for SEX) |
| `search_indicators` | Search indicators by text query and structured filters |
| `count_indicators` | Count indicators matching filters, with year range support |
| `get_indicator_metadata` | Get full definition, methodology, and data sources for an indicator |
| `get_indicator_summary` | Quick overview of multiple indicators from local cache |

### Geography

| Tool | Description |
|------|-------------|
| `search_geo_units` | Search countries and regions by name or ISO3 code, with grouping disambiguation |

### Data retrieval

| Tool | Description |
|------|-------------|
| `get_latest_value` | Get a single data point for an indicator and geography |
| `get_time_series` | Get the full time series for an indicator and geography |
| `get_country_ranking` | Rank countries by indicator value (top N / bottom N) |
| `compare_geographies` | Compare an indicator across up to 20 specific geographies |

### Utility

| Tool | Description |
|------|-------------|
| `server_status` | Health check with server name and UTC timestamp |

## Installation

### Hosted (recommended)

The server is hosted on [Prefect Horizon](https://horizon.prefect.io) — no local setup needed.

Use this URL in any MCP-compatible client:

```
https://unesco.fastmcp.app/mcp
```

For example, in Claude Desktop config:

```json
{
  "mcpServers": {
    "unesco-mcp": {
      "url": "https://unesco.fastmcp.app/mcp"
    }
  }
}
```

Or in Claude Code:

```bash
claude mcp add unesco-mcp --url https://unesco.fastmcp.app/mcp
```

### Local (from source)

To run the server locally instead:

**Claude Desktop** — add to your config file (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS, `%APPDATA%\Claude\claude_desktop_config.json` on Windows):

```json
{
  "mcpServers": {
    "unesco-mcp": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/unesco-mcp", "unesco-mcp"]
    }
  }
}
```

**Claude Code:**

```bash
claude mcp add unesco-mcp -- uv run --directory /path/to/unesco-mcp unesco-mcp
```

## Example usage

Once installed, you can ask your AI assistant things like:

- "What is the primary completion rate in Kenya?"
- "Compare literacy rates across East African countries"
- "Which countries have the highest out-of-school rates?"
- "What education indicators are available broken down by sex and wealth quintile?"
- "Show me the trend in secondary enrollment for Brazil over the last 10 years"


