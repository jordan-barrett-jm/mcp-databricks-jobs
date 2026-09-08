# Databricks Jobs MCP Server

A standalone MCP (Model Context Protocol) server that deploys and manages Databricks jobs
via Declarative Automation Bundles (DABs). Designed to run **outside** the Databricks
environment — in VS Code, Codex, Claude Desktop, or any MCP-compatible client.

## Architecture

```
┌─────────────────────┐         ┌──────────────────────────────┐
│  VS Code / Codex    │  stdio  │     mcp_server.py            │
│  (MCP Client)       │◄───────►│  ┌─────────────────────┐    │
│                     │         │  │ deploy()            │    │
│                     │         │  │ run_job()           │    │
│                     │         │  │ get_job_status()    │    │
│                     │         │  │ cancel_run()        │    │
│                     │         │  └────────┬────────────┘    │
└─────────────────────┘         │           │                  │
                                │           │ Databricks SDK    │
                                │           ▼                  │
                                │  ┌─────────────────────┐    │
                                │  │ DATABRICKS_HOST     │    │
                                │  │ DATABRICKS_TOKEN    │    │
                                │  └────────┬────────────┘    │
                                └───────────┼──────────────────┘
                                            │
                                            ▼
                                ┌──────────────────────────────┐
                                │   Databricks Workspace       │
                                │   ┌────────────────────┐     │
                                │   │ sample_transform_  │     │
                                │   │ job (serverless)   │     │
                                │   └────────────────────┘     │
                                └──────────────────────────────┘
```

## Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and on PATH
- A Databricks workspace with serverless compute enabled
- A [Personal Access Token](https://docs.databricks.com/dev-tools/auth/pat.html)

## Hosted Version (Databricks App)

The MCP server is also deployed as a Databricks App: **mcp-jobs-server**, reachable at:

```
https://mcp-jobs-server-3317548451194413.aws.databricksapps.com/mcp
```

Source for the hosted version lives in `app/` (`app.py`, `app.yaml`, `requirements.txt`) and uses
the **Streamable HTTP** transport (`/mcp` endpoint) — the transport Databricks Apps requires for
hosted MCP servers. It authenticates outbound calls to the Jobs API using the app's own service
principal automatically; no token needs to be configured for that direction.

### Important: inbound auth is OAuth-only, not PAT

Databricks-hosted MCP servers (apps) **do not support personal access tokens for inbound client
authentication** — only OAuth. This differs from the local stdio server below, which uses a PAT
for its own *outbound* calls to Databricks and needs no inbound auth (it runs as a local process).

To connect an external client (VS Code, Codex, etc.) to the hosted app:

1. Have a workspace/account admin create a Databricks OAuth application (Account Console >
   Settings > App Connections > Add connection, or `databricks account custom-app-integration create`).
2. Configure your MCP client to authenticate to the app URL above via OAuth (client ID/secret or
   the client's OAuth flow), not a static bearer token.
3. For Codex specifically, consider the `ucode` CLI tool, which authenticates through your
   Databricks CLI login and refreshes OAuth tokens automatically:
   ```bash
   uv tool install git+https://github.com/databricks/ucode
   ucode mcp add --agents codex --services <your-mcp-server>
   ```

**If a plain token-authenticated setup is what you need**, use the local stdio server
(`mcp_server.py`) described below instead — it satisfies the original "authenticate with a
Databricks token" requirement directly, since the token is only used outbound (server-to-Databricks),
not for inbound client access.

### The hosted app needs the Databricks CLI to deploy

The app's `deploy` tool shells out to `databricks bundle deploy`, so the CLI must be reachable
from the app container — on `PATH`, or pointed at by `DATABRICKS_CLI_PATH`. Without it the app
can only refresh the workspace Git folder, which updates notebook source but leaves
`databricks.yml` changes (schedules, tasks, tags, compute) unapplied; the tool reports that
explicitly instead of claiming success.

The bundle itself does not need to be in the app's source path: `deploy` exports it from
`WORKSPACE_REPO_PATH` into a temp directory. Set `DAB_PROJECT_DIR` if the bundle is already
present in the container and you want to skip the export. Outbound CLI auth uses the app's
service principal, which must have permission to deploy the bundle's resources.

### Redeploying the hosted app after code changes

```bash
databricks apps deploy mcp-jobs-server --source-code-path /Workspace/Users/<you>/mcp-databricks-jobs/app
```

## Quick Start (local stdio server)

### 1. Clone and install

```bash
cd mcp-databricks-jobs
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_xxxxxxxxxxxxxxxx"
export DAB_PROJECT_DIR="$(pwd)"   # points to this repo root

# Optional
export DATABRICKS_CLI_PATH="/opt/homebrew/bin/databricks"  # if the CLI is not on PATH
export DAB_DEPLOY_TIMEOUT=900                              # seconds; caps `bundle deploy`
```

### 3. Deploy the job first

```bash
databricks bundle deploy --target dev
```

### 4. Run the MCP server

```bash
python mcp_server.py
```

## MCP Client Configuration

### VS Code (`.vscode/mcp.json`)

```json
{
  "servers": {
    "databricks-jobs": {
      "command": "python",
      "args": ["/absolute/path/to/mcp-databricks-jobs/mcp_server.py"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_xxxxxxxxxxxxxxxx",
        "DAB_PROJECT_DIR": "/absolute/path/to/mcp-databricks-jobs"
      }
    }
  }
}
```

### Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "databricks-jobs": {
      "command": "python",
      "args": ["/absolute/path/to/mcp-databricks-jobs/mcp_server.py"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_xxxxxxxxxxxxxxxx",
        "DAB_PROJECT_DIR": "/absolute/path/to/mcp-databricks-jobs"
      }
    }
  }
}
```

### Codex / OpenAI CLI

```json
{
  "mcp_servers": {
    "databricks-jobs": {
      "type": "stdio",
      "command": "python",
      "args": ["/absolute/path/to/mcp-databricks-jobs/mcp_server.py"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_xxxxxxxxxxxxxxxx",
        "DAB_PROJECT_DIR": "/absolute/path/to/mcp-databricks-jobs"
      }
    }
  }
}
```

## Available Tools

| Tool | Description |
| --- | --- |
| `deploy` | Checks out git ref, pulls latest, runs `databricks bundle deploy` |
| `run_job` | Triggers a job run with duplicate prevention + ETA from history |
| `get_job_status` | Full status: lifecycle, result, duration, error codes & stack traces |
| `cancel_run` | Cancels an active run (latest active if no run_id specified) |

### A note on job names

DAB does not deploy the job under the name in `databricks.yml`. The bundle names it
`sample_transform_job_${bundle.target}`, and `mode: development` prefixes it again, so a dev
deploy lands in the workspace as `[dev jordan_barrett] sample_transform_job_dev`.

The tools account for this: `job_name` is matched exactly first, then against the deployed
name with the dev prefix stripped and the target suffix allowed. If a name matches several
deployed jobs — typically because both `dev` and `prod` are deployed — the tools list the
candidates and ask you to pass a full name rather than picking one silently.

## Tool Parameters

### `deploy`
| Param | Default | Description |
| --- | --- | --- |
| `git_ref` | `"main"` | Branch/tag to checkout |
| `target` | `"dev"` | DAB target (dev, prod) |

Both the local server and the hosted app run `databricks bundle deploy --target <target>
--auto-approve` — `--auto-approve` because there is no one to answer an interactive prompt.
The local server checks out and pulls first; the hosted app pulls the workspace Git folder,
exports the bundle from it, then deploys. If the CLI is unavailable the app says
**"Bundle NOT deployed"** rather than reporting success for a Git pull alone.

### `run_job`
| Param | Default | Description |
| --- | --- | --- |
| `job_name` | `"sample_transform_job"` | Exact job name in workspace |
| `prevent_duplicate` | `true` | Block if already running |

### `get_job_status`
| Param | Default | Description |
| --- | --- | --- |
| `run_id` | `null` | Specific run ID (latest if omitted) |
| `job_name` | `"sample_transform_job"` | For resolving latest run |

### `cancel_run`
| Param | Default | Description |
| --- | --- | --- |
| `run_id` | `null` | Specific run to cancel (latest active if omitted) |
| `job_name` | `"sample_transform_job"` | For finding active run |

## Project Structure

```
mcp-databricks-jobs/
├── mcp_server.py          # MCP server (run locally)
├── databricks.yml         # DAB bundle config (serverless job)
├── requirements.txt       # Python dependencies
├── README.md              # This file
└── src/
    ├── ingest_orders.py    # Task 1 — lands synthetic orders in orders_raw
    └── aggregate_sales.py  # Task 2 — publishes sales_by_region
```

## The job

`sample_transform_job` is a two-task serverless workflow:

| Task | Depends on | Writes |
| --- | --- | --- |
| `ingest_orders` | — | `${catalog}.${schema}.orders_raw` |
| `aggregate_sales` | `ingest_orders` | `${catalog}.${schema}.sales_by_region` |

`catalog` and `schema` are job parameters (defaults `bronze_sandbox` /
`mcp_jobs_demo`) and arrive in the notebooks as widgets. `sales_by_region` is
created from an explicit `CREATE TABLE` contract rather than inferred from the
DataFrame, so Delta enforces the declared column types on every write.

## Security Notes

- **Never commit tokens** — use environment variables or a secrets manager.
- The MCP server runs with the permissions of the token owner. Use a service
  principal token for production deployments.
- Consider restricting the token scope to Jobs-only permissions if your
  workspace supports fine-grained tokens.

## Extending

To add more tools, define a new function with `@mcp.tool()` in `mcp_server.py`.
The MCP SDK auto-generates the tool schema from type hints and docstrings.
