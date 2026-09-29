# mcp-databricks-jobs

MCP server that deploys, runs and monitors a Databricks job via a Declarative Automation Bundle (DAB). It ships in two forms that share the same tool surface:

- `mcp_server.py`: local stdio server. Outbound auth is `DATABRICKS_HOST` + `DATABRICKS_TOKEN` (PAT).
- `app/app.py`: the same tools as a hosted Databricks App (Streamable HTTP at `/mcp`). Outbound auth is the app's own service principal (SP).

Tools: `deploy`, `run_job`, `get_job_status`, `cancel_run`, and (app only) `register_git_credential`.

## Placeholders were redacted; fill them in before deploying

Employer and workspace identifiers were removed from the tracked files (2026-09). The repo will not deploy until they are set again:

| Placeholder | File | Put back |
|---|---|---|
| `you@example.com` | `databricks.yml` (`permissions.user_name`) | your workspace login email |
| `you@example.com` | `app/app.yaml` (`WORKSPACE_REPO_PATH`), `app/app.py` (fallback default) | `/Workspace/Users/<your-email>/mcp-databricks-jobs` |
| `<app-service-principal-id>` | `databricks.yml` (`permissions.service_principal_name`) | the app's SP **application (client) ID**: `databricks apps get mcp-jobs-server --profile <profile>` -> `service_principal_client_id` |
| `<app-url>` | `README.md` | the app's URL from the same command |
| `<your-username>` | `README.md`, docstrings | the `[dev <name>]` prefix DAB shows on your deployed job |

Do **not** commit the real values. Keep them in an untracked local edit (or `git update-index --skip-worktree` on those files). Git history still contains the old values, so treat the GitHub remote as if they are exposed unless history is rewritten.

## How it works

**Bundle.** `databricks.yml` defines `sample_transform_job`, a two-task serverless job: `ingest_orders` -> `aggregate_sales` (notebooks in `src/`). `catalog` / `schema` are job parameters (default `bronze_sandbox` / `mcp_jobs_demo`) read by the notebooks as widgets. `aggregate_sales` writes `sales_by_region` from an explicit `CREATE TABLE`, so Delta enforces column types (the `net_revenue` DECIMAL cast matters; dropping it makes the demo fail with `DELTA_FAILED_TO_MERGE_FIELDS`).

**Job names.** DAB renames the job to `sample_transform_job_<target>` and `mode: development` adds a `[dev <user>]` prefix. The tools match `job_name` exactly first, then with the prefix stripped and the target suffix allowed. Ambiguous matches (dev and prod both deployed) return a candidate list instead of guessing. See `_find_job` in `app/app.py`.

**Hosted `deploy` flow** (`app/app.py`):
1. `_pull_repo`: resolve the workspace Git folder at `WORKSPACE_REPO_PATH` (`repos.get`, numeric repo id, not a path) and `repos.update` it to `git_ref`.
2. `_materialize_bundle`: export the bundle from that folder into a temp dir (skipped if `DAB_PROJECT_DIR` is set).
3. `_ensure_cli` / `_ensure_terraform`: download the Databricks CLI and Terraform into `/tmp` (see below).
4. Run `databricks bundle deploy --target <target> --auto-approve` as the app's SP. If the CLI is unavailable it says "Bundle NOT deployed" rather than reporting a pull as success.

**Auth.**
- Inbound to the hosted app is OAuth only; PATs are not accepted. From a client, use the stdio bridge in `../dbx-mcp-bridge/bridge.py` (uses your `databricks auth login` profile via the SDK credential chain, no client ID/secret/redirect URI). Add to Claude Code with `claude mcp add databricks-jobs -- python bridge.py https://<app-url>/mcp --profile <profile>`.
- Outbound from the app is its own SP. That SP needs permission on the bundle's resources and on the catalog/schema.

**The app deploys as itself.** It creates its own job (`[dev app_<id>_mcp_jobs_server] sample_transform_job_dev`), separate from any you deployed by hand. The `permissions:` block in `databricks.yml` grants both you and the SP `CAN_MANAGE` so you can see and run the app-deployed job. Both principals must be listed or the CLI warns the grants have no effect.

## Getting it working again

1. Fill in the placeholders above.
2. Redeploy the app after code changes:
   `databricks apps deploy mcp-jobs-server --source-code-path /Workspace/Users/<you>/mcp-databricks-jobs/app --profile <profile>`
   The workspace Git folder must be a checkout of this repo (the app pulls from it).
3. **Git credential for the SP (one-time).** `deploy` fails at the pull step if the app's SP has no Git credential, and creating one for another principal needs SP-manager rights. No admin is needed: call the app's own `register_git_credential` tool with `personal_access_token` (a GitHub fine-grained token with Contents:Read on this repo), `git_username`, and `git_provider` (default `gitHub`). A principal creating a credential for itself passes no `principal_id`, which is open to it. If the token is revoked, the credential breaks; re-register.
4. **CLI missing in the container**: the Databricks CLI is a Go binary with no pip package, so `_ensure_cli()` downloads the release zip (`DATABRICKS_CLI_VERSION`, default 1.14.1). Needs egress to github.com. Override with `DATABRICKS_CLI_PATH`.
5. **Terraform checksum error** (`openpgp: key expired`): HashiCorp's signing key expired, so the CLI's own Terraform download fails. `_ensure_terraform()` fetches the archive directly (`DATABRICKS_TF_VERSION`, default 1.5.5, matching `databricks bundle debug terraform`) and sets `DATABRICKS_TF_EXEC_PATH`. Keep the versions aligned if you bump the CLI.
6. Long deploys: `DAB_DEPLOY_TIMEOUT` (seconds, default 900).
7. Verify: `deploy` -> `run_job` -> `get_job_status` through the MCP client; a green run writes `bronze_sandbox.mcp_jobs_demo.sales_by_region`.

## Local stdio server

```bash
pip install -r requirements.txt
export DATABRICKS_HOST=https://<workspace-host> DATABRICKS_TOKEN=<pat> DAB_PROJECT_DIR="$(pwd)"
databricks bundle deploy --target dev
python mcp_server.py
```

Workspace admin is not required for the hosted path, but creating a PAT can be blocked in some workspaces; the hosted app plus OAuth CLI profile avoids that.

## Conventions

- Never commit tokens, workspace hosts, emails, SP IDs or app URLs; use the placeholders.
- Add tools with `@mcp.tool()`; the schema comes from type hints and docstrings. Keep `mcp_server.py` and `app/app.py` behaviour in step (job lookup, status output).
- The app must bind `DATABRICKS_APP_PORT`; never hardcode a port.
