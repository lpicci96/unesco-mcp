# unesco-mcp

Python package for an upcoming Model Context Protocol (MCP) 
server targeting the
[UNESCO Institute for Statistics (UIS) API.](https://api.uis.unesco.org/api/public/documentation/)

## Status

This project is currently in pre-alpha and published primarily to reserve the PyPI namespace.

## Scope (Current)

- Package metadata and release scaffolding
- Basic importable module layout
- Changelog tracking

## Scope (Planned)

- UIS client integration
- MCP server tools/resources
- Documentation for setup and usage

## Installation

```bash
pip install unesco-mcp
```

## Use with Claude Desktop

1. Open Claude Desktop settings:
- Claude menu -> `Settings...` -> `Developer` -> `Edit Config`.

2. Update `claude_desktop_config.json`:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

3. Add this MCP server entry (update the path to your local clone):

```json
{
  "mcpServers": {
    "unesco-mcp": {
      "command": "uv",
      "args": [
        "run",
        "--directory", "/path/to/unesco-mcp",
        "unesco-mcp"
      ]
    }
  }
}
```

4. Fully restart Claude Desktop.

5. Open a new chat and test:
- "Call `server_status`."
- "Call `search_indicators` with `query='enrollment'`."

## License

MIT (see `LICENSE`).
