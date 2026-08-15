curl -fsSL https://claude.ai/install.sh | bash

Add the Atlas MCP directly via Claude Settings -> Connectors

Alternatively, you can connect using an Atlas PAT
1. Set the Env Var ATLAS_TOKEN
2. In the terminal, run `claude mcp add --transport http atlas https://dchealth.datacoves.ai/mcp --header 'Authorization: Bearer ${ATLAS_TOKEN}'`
