"""
Databricks Jobs MCP Server
===========================
A standalone MCP server for deploying and managing Databricks jobs.
Runs outside Databricks (VS Code, Codex, CLI) and authenticates via token.

Usage:
    export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."
    export DAB_PROJECT_DIR="/path/to/this/repo"
    python mcp_server.py
"""

import os
import subprocess
import statistics
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# ---------------------------------------------------------------------------
# MCP Server Definition
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "databricks-jobs",
    instructions="Deploy, run, and monitor Databricks jobs managed via Declarative Automation Bundles.",
)


def _get_client() -> WorkspaceClient:
    """Authenticated Databricks client from environment variables."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        raise EnvironmentError(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set. "
            "Generate a token at: <workspace>/settings/user/developer/access-tokens"
        )
    return WorkspaceClient(host=host, token=token)


def _find_job(client: WorkspaceClient, job_name: str):
    """Resolve a job by exact name match."""
    for job in client.jobs.list(name=job_name):
        if job.settings and job.settings.name == job_name:
            return job
    return None


# ---------------------------------------------------------------------------
# Tool 1: Deploy
# ---------------------------------------------------------------------------

@mcp.tool()
def deploy(git_ref: str = "main", target: str = "dev") -> str:
    """
    Deploy the Databricks job via Declarative Automation Bundles.

    Checks out the specified git ref, pulls latest changes, then runs
    `databricks bundle deploy` against the chosen target environment.

    Args:
        git_ref: Branch or tag to checkout (default: main).
        target: DAB target to deploy to — matches a key in databricks.yml targets (default: dev).

    Returns:
        Deployment result including any errors from git or the bundle CLI.
    """
    project_dir = os.environ.get("DAB_PROJECT_DIR", os.getcwd())

    # --- Step 1: Git checkout & pull ---
    checkout = subprocess.run(
        ["git", "checkout", git_ref],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if checkout.returncode != 0:
        return f"❌ git checkout failed:\n{checkout.stderr.strip()}"

    pull = subprocess.run(
        ["git", "pull", "origin", git_ref],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if pull.returncode != 0:
        return f"❌ git pull failed:\n{pull.stderr.strip()}"

    # --- Step 2: DAB deploy ---
    deploy_proc = subprocess.run(
        ["databricks", "bundle", "deploy", "--target", target],
        cwd=project_dir,
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    if deploy_proc.returncode != 0:
        return f"❌ bundle deploy failed:\n{deploy_proc.stderr.strip()}"

    return (
        f"✅ Deployed successfully\n"
        f"   Branch: {git_ref}\n"
        f"   Target: {target}\n"
        f"   Output: {deploy_proc.stdout.strip()[-800:]}"
    )


# ---------------------------------------------------------------------------
# Tool 2: Run Job
# ---------------------------------------------------------------------------

@mcp.tool()
def run_job(job_name: str = "sample_transform_job", prevent_duplicate: bool = True) -> str:
    """
    Trigger a one-time run of a Databricks job.

    Prevents concurrent duplicate runs by default. Also estimates completion
    time based on the mean duration of the last 10 successful runs.

    Args:
        job_name: Exact name of the deployed job (default: sample_transform_job).
        prevent_duplicate: Block execution if the job already has an active run.

    Returns:
        Run ID, estimated duration, or a duplicate-run warning.
    """
    client = _get_client()
    job = _find_job(client, job_name)
    if not job:
        return f"❌ Job \'{job_name}\' not found. Deploy first with the `deploy` tool."

    job_id = job.job_id

    # --- Duplicate prevention ---
    if prevent_duplicate:
        active_runs = list(client.jobs.list_runs(job_id=job_id, active_only=True))
        if active_runs:
            r = active_runs[0]
            return (
                f"⚠️  Job already running — duplicate blocked.\n"
                f"   Active Run ID: {r.run_id}\n"
                f"   Started: {datetime.fromtimestamp(r.start_time / 1000).isoformat() if r.start_time else 'N/A'}\n"
                f"   Tip: use `get_job_status` to monitor or `cancel_run` to stop it."
            )

    # --- Estimate duration from history ---
    history = list(client.jobs.list_runs(job_id=job_id, completed_only=True, limit=10))
    durations = [r.execution_duration / 1000 for r in history if r.execution_duration]
    estimate_msg = ""
    if durations:
        avg = statistics.mean(durations)
        m, s = divmod(int(avg), 60)
        estimate_msg = f"   ⏱️  Estimated duration: {m}m {s}s (avg of {len(durations)} runs)\n"
    else:
        estimate_msg = "   ⏱️  No prior runs — cannot estimate duration.\n"

    # --- Trigger ---
    run = client.jobs.run_now(job_id=job_id)

    return (
        f"✅ Job triggered\n"
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
    Check the status of a job run. Returns lifecycle state, result, duration,
    and full error details (error code + exception trace) on failure.

    Args:
        run_id: Specific run ID. If omitted, fetches the most recent run.
        job_name: Used to resolve the latest run when run_id is not provided.

    Returns:
        Structured status report; includes stack trace if the run failed.
    """
    client = _get_client()

    # Resolve run_id if not provided
    if run_id is None:
        job = _find_job(client, job_name)
        if not job:
            return f"❌ Job \'{job_name}\' not found."
        runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
        if not runs:
            return f"ℹ️  No runs recorded for \'{job_name}\'."
        run_id = runs[0].run_id

    run = client.jobs.get_run(run_id=run_id)
    state = run.state
    lifecycle = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"

    lines = [
        "📋 Job Run Status",
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

    # --- Failure details ---
    if state.result_state == RunResultState.FAILED:
        lines.append("")
        lines.append("   ❌ FAILURE DETAILS")
        if state.state_message:
            lines.append(f"   Message: {state.state_message}")

        # Task-level errors
        if run.tasks:
            for task in run.tasks:
                ts = task.state
                if ts and ts.result_state == RunResultState.FAILED:
                    lines.append(f"   Failed Task : {task.task_key}")
                    if ts.state_message:
                        lines.append(f"   Task Error  : {ts.state_message}")

        # Exception trace via run output
        try:
            output = client.jobs.get_run_output(run_id=run_id)
            if output.error:
                lines.append(f"   Exception   : {output.error}")
            if output.error_trace:
                trace = output.error_trace[-2000:]  # last 2000 chars
                lines.append(f"   Stack Trace :\n{trace}")
        except Exception:
            lines.append("   (Could not retrieve run output for trace)")

    elif state.life_cycle_state == RunLifeCycleState.RUNNING:
        lines.append("\n   🔄 Currently running...")

    elif state.result_state == RunResultState.SUCCESS:
        lines.append("\n   ✅ Completed successfully.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 4: Cancel Run
# ---------------------------------------------------------------------------

@mcp.tool()
def cancel_run(run_id: Optional[int] = None, job_name: str = "sample_transform_job") -> str:
    """
    Cancel an active job run. If no run_id is provided, cancels the latest
    active run of the specified job.

    Args:
        run_id: Specific run ID to cancel.
        job_name: Used to find the active run when run_id is not provided.

    Returns:
        Cancellation confirmation or message if no active run exists.
    """
    client = _get_client()

    if run_id is None:
        job = _find_job(client, job_name)
        if not job:
            return f"❌ Job \'{job_name}\' not found."
        active = list(client.jobs.list_runs(job_id=job.job_id, active_only=True))
        if not active:
            return f"ℹ️  No active runs for \'{job_name}\' — nothing to cancel."
        run_id = active[0].run_id

    client.jobs.cancel_run(run_id=run_id)

    return (
        f"✅ Cancellation requested\n"
        f"   Run ID: {run_id}\n"
        f"   The run will terminate shortly."
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Runs as stdio transport — compatible with VS Code, Codex, Claude Desktop
    mcp.run(transport="stdio")
