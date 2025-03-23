# troccomcp
MCP server for using the TROCCO API

## build
```bash
npm install
npm run build
```

## configuration
```json
{
  "mcpServers": {
    "trocco": {
      "command": "node",
      "args": [
        "<your_path>/build/index.js"
      ],
      "env": {
        "TROCCO_API_KEY": "<your_api_key>"
      }
    }
  }
}
```
