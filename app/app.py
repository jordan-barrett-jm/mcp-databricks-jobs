"""
Databricks App: MCP Server for Job Management
===============================================
Runs as a hosted Databricks App using the Streamable HTTP transport
(the transport Databricks Apps requires for hosted MCP servers).
Served at: https://<app-url>/mcp

Authentication:
- Inbound: Databricks-hosted MCP servers require OAuth -- personal access
  tokens are NOT supported for this hosting pattern. Clients (VS Code,
  Codex, etc.) must authenticate with an OAuth token scoped to this app.
- Outbound: Uses the app's own service principal credentials (auto-injected)
  to call the Jobs API, unless DATABRICKS_TOKEN is explicitly overridden.
"""

import os
import statistics
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Databricks Apps injects DATABRICKS_APP_PORT (default 8000) — never hardcode a port.
PORT = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("APP_PORT", "8000")))

# The app can authenticate using:
# 1. Service principal (automatic in Databricks Apps via DATABRICKS_HOST)
# 2. Explicit token passed as env var (for flexibility)
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Workspace Git folder path — the app pulls latest code here via the Repos API
WORKSPACE_REPO_PATH = os.environ.get(
    "WORKSPACE_REPO_PATH",
    "/Workspace/Users/jordan.barrett@factored.ai/mcp-databricks-jobs"
)

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "databricks-jobs",
    instructions="Deploy, run, and monitor Databricks jobs via Declarative Automation Bundles. Hosted as a Databricks App.",
    host="0.0.0.0",
    port=PORT,
)


def _get_client() -> WorkspaceClient:
    """Get authenticated Databricks workspace client.
    
    In a Databricks App context, the SDK auto-detects credentials from
    the app's service principal. Falls back to explicit token if set.
    """
    kwargs = {}
    if DATABRICKS_HOST:
        kwargs["host"] = DATABRICKS_HOST
    if DATABRICKS_TOKEN:
        kwargs["token"] = DATABRICKS_TOKEN
    return WorkspaceClient(**kwargs)


def _find_job(client: WorkspaceClient, job_name: str):
    """Resolve a job by exact name."""
    for job in client.jobs.list(name=job_name):
        if job.settings and job.settings.name == job_name:
            return job
    return None


# ---------------------------------------------------------------------------
# Tool 1: Deploy
# ---------------------------------------------------------------------------

@mcp.tool()
def deploy(git_ref: str = "main") -> str:
    """
    Deploy the latest job code by pulling the newest changes from Git
    into the workspace Git folder.

    The job notebook lives inside the Git folder, so pulling the latest
    ref automatically updates the code that runs on the next job trigger.

    Args:
        git_ref: Branch or tag to sync to (default: main).

    Returns:
        Deployment result including the branch and latest commit hash.
    """
    client = _get_client()

    # Find the repo by path
    repo = None
    for r in client.repos.list(path_prefix=WORKSPACE_REPO_PATH):
        if r.path and r.path.rstrip("/") == WORKSPACE_REPO_PATH.rstrip("/"):
            repo = r
            break

    if not repo:
        return (
            f"\u274c Git folder not found at {WORKSPACE_REPO_PATH}.\n"
            f"   Ensure the repo is cloned in the workspace."
        )

    # Update the repo to the specified branch (pulls latest from remote)
    try:
        updated = client.repos.update(repo_id=repo.id, branch=git_ref)
    except Exception as e:
        return f"\u274c Failed to pull latest from '{git_ref}':\n   {e}"

    head_commit = getattr(updated, 'head_commit_id', 'unknown')

    return (
        f"\u2705 Deployed successfully\n"
        f"   Git folder: {WORKSPACE_REPO_PATH}\n"
        f"   Branch: {git_ref}\n"
        f"   Commit: {head_commit}\n"
        f"   The job will use the updated code on the next run."
    )


# ---------------------------------------------------------------------------
# Tool 2: Run Job
# ---------------------------------------------------------------------------

@mcp.tool()
def run_job(job_name: str = "sample_transform_job", prevent_duplicate: bool = True) -> str:
    """
    Trigger a Databricks job run with duplicate prevention and ETA estimation.

    Args:
        job_name: Exact name of the deployed job (default: sample_transform_job).
        prevent_duplicate: Block if the job already has an active run (default: True).

    Returns:
        Run ID and estimated duration, or duplicate-run warning.
    """
    client = _get_client()
    job = _find_job(client, job_name)
    if not job:
        return f"\u274c Job \'{job_name}\' not found. Deploy first with the `deploy` tool."

    job_id = job.job_id

    # Duplicate prevention
    if prevent_duplicate:
        active_runs = list(client.jobs.list_runs(job_id=job_id, active_only=True))
        if active_runs:
            r = active_runs[0]
            started = datetime.fromtimestamp(r.start_time / 1000).isoformat() if r.start_time else "N/A"
            return (
                f"\u26a0\ufe0f  Job already running \u2014 duplicate blocked.\n"
                f"   Active Run ID: {r.run_id}\n"
                f"   Started: {started}\n"
                f"   Use `get_job_status` to monitor or `cancel_run` to stop it."
            )

    # Historical ETA
    history = list(client.jobs.list_runs(job_id=job_id, completed_only=True, limit=10))
    durations = [r.execution_duration / 1000 for r in history if r.execution_duration]
    if durations:
        avg = statistics.mean(durations)
        m, s = divmod(int(avg), 60)
        estimate_msg = f"   \u23f1\ufe0f  Estimated duration: {m}m {s}s (avg of {len(durations)} runs)\n"
    else:
        estimate_msg = "   \u23f1\ufe0f  No prior runs \u2014 cannot estimate duration.\n"

    # Trigger
    run = client.jobs.run_now(job_id=job_id)

    return (
        f"\u2705 Job triggered\n"
        f"   Job: {job_name} (ID: {job_id})\n"
        f"   Run ID: {run.run_id}\n"
        f"{estimate_msg}"
        f"   Track with: get_job_status(run_id={run.run_id})"
    )


# ---------------------------------------------------------------------------
# Tool 3: Get Job Status
# ---------------------------------------------------------------------------

@mcp.tool()
def get_job_status(run_id: Optional[int] = None, job_name: str = "sample_transform_job") -> str:
    """
    Check the status of a job run with full error details on failure.

    Returns lifecycle state, result, duration, and complete error code +
    exception stack trace if the run failed.

    Args:
        run_id: Specific run ID. If omitted, fetches the most recent run.
        job_name: Used to resolve the latest run when run_id is not provided.

    Returns:
        Structured status report with failure diagnostics.
    """
    client = _get_client()

    if run_id is None:
        job = _find_job(client, job_name)
        if not job:
            return f"\u274c Job \'{job_name}\' not found."
        runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
        if not runs:
            return f"\u2139\ufe0f  No runs recorded for \'{job_name}\'."
        run_id = runs[0].run_id

    run = client.jobs.get_run(run_id=run_id)
    state = run.state
    lifecycle = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"

    lines = [
        "\U0001f4cb Job Run Status",
        f"   Run ID      : {run.run_id}",
        f"   Job         : {run.run_name or job_name}",
        f"   Lifecycle   : {lifecycle}",
    ]

    if state.result_state:
        lines.append(f"   Result      : {state.result_state.value}")
    if run.start_time:
        lines.append(f"   Started     : {datetime.fromtimestamp(run.start_time / 1000).isoformat()}")
    if run.execution_duration:
        m, s = divmod(int(run.execution_duration / 1000), 60)
        lines.append(f"   Duration    : {m}m {s}s")

    # Failure details
    if state.result_state == RunResultState.FAILED:
        lines.append("")
        lines.append("   \u274c FAILURE DETAILS")
        if state.state_message:
            lines.append(f"   Message: {state.state_message}")

        if run.tasks:
            for task in run.tasks:
                ts = task.state
                if ts and ts.result_state == RunResultState.FAILED:
                    lines.append(f"   Failed Task : {task.task_key}")
                    if ts.state_message:
                        lines.append(f"   Task Error  : {ts.state_message}")

        try:
            output = client.jobs.get_run_output(run_id=run_id)
            if output.error:
                lines.append(f"   Exception   : {output.error}")
            if output.error_trace:
                trace = output.error_trace[-2000:]
                lines.append(f"   Stack Trace :\n{trace}")
        except Exception:
            lines.append("   (Could not retrieve run output for trace)")

    elif state.life_cycle_state == RunLifeCycleState.RUNNING:
        lines.append("\n   \U0001f504 Currently running...")
    elif state.result_state == RunResultState.SUCCESS:
        lines.append("\n   \u2705 Completed successfully.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 4: Cancel Run
# ---------------------------------------------------------------------------

@mcp.tool()
def cancel_run(run_id: Optional[int] = None, job_name: str = "sample_transform_job") -> str:
    """
    Cancel an active job run.

    Args:
        run_id: Specific run ID to cancel. If omitted, cancels the latest active run.
        job_name: Used to find the active run when run_id is not provided.

    Returns:
        Cancellation confirmation or info message if no active run exists.
    """
    client = _get_client()

    if run_id is None:
        job = _find_job(client, job_name)
        if not job:
            return f"\u274c Job \'{job_name}\' not found."
        active = list(client.jobs.list_runs(job_id=job.job_id, active_only=True))
        if not active:
            return f"\u2139\ufe0f  No active runs for \'{job_name}\' \u2014 nothing to cancel."
        run_id = active[0].run_id

    client.jobs.cancel_run(run_id=run_id)

    return (
        f"\u2705 Cancellation requested\n"
        f"   Run ID: {run_id}\n"
        f"   The run will terminate shortly."
    )


# ---------------------------------------------------------------------------
# Entry Point — Streamable HTTP transport for Databricks App hosting
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # host/port are configured on the FastMCP instance above (v1 API);
    # run() only accepts the transport (and optional mount_path).
    # Databricks Apps requires an HTTP-compatible transport for hosted MCP
    # servers -- streamable-http is the officially supported choice, served
    # at /mcp (not /sse).
    mcp.run(transport="streamable-http")
