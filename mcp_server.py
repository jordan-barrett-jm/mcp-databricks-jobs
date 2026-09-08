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

import json
import os
import re
import shutil
import statistics
import subprocess
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


# DAB's `mode: development` prefixes every deployed resource, e.g.
# "[dev jordan_barrett] sample_transform_job_dev".
_DEV_PREFIX = re.compile(r"^\[[^\]]*\]\s*")


class JobNotResolved(Exception):
    """A job name matched no deployed job, or matched several."""


def _matches_deployed_name(actual: str, wanted: str) -> bool:
    """True if `actual` is `wanted` carrying DAB's dev prefix and/or target suffix."""
    base = _DEV_PREFIX.sub("", actual)
    return base == wanted or base.startswith(wanted + "_")


def _find_job(client: WorkspaceClient, job_name: str):
    """Resolve a job by the name DAB actually deploys it under.

    databricks.yml names the job `sample_transform_job_${bundle.target}`, and
    `mode: development` prefixes it further, so a dev deploy lands in the
    workspace as `[dev jordan_barrett] sample_transform_job_dev`. Matching on
    the exact string alone therefore never finds a bundle-deployed job.

    An exact match wins outright. Otherwise the dev prefix is stripped and the
    target suffix allowed, and an ambiguous result is reported rather than
    guessed at -- pass the full workspace name as `job_name` to pin one down.

    Raises:
        JobNotResolved: if the name matches no job, or several.
    """
    candidates = []
    for job in client.jobs.list(expand_tasks=False):
        name = job.settings.name if job.settings else None
        if not name:
            continue
        if name == job_name:
            return job
        if _matches_deployed_name(name, job_name):
            candidates.append(job)

    if not candidates:
        raise JobNotResolved(
            f"Job '{job_name}' not found. Deploy first with the `deploy` tool."
        )
    if len(candidates) > 1:
        listed = "\n".join(
            f"     - {j.settings.name} (ID: {j.job_id})" for j in candidates
        )
        raise JobNotResolved(
            f"Job '{job_name}' is ambiguous -- {len(candidates)} deployed jobs match:\n"
            f"{listed}\n"
            f"   Re-run with job_name set to one of the names above."
        )
    return candidates[0]


# ---------------------------------------------------------------------------
# Tool 1: Deploy
# ---------------------------------------------------------------------------

# `bundle deploy` can run for minutes; cap it so a wedged CLI cannot hang the
# server forever.
DEPLOY_TIMEOUT_SECONDS = int(os.environ.get("DAB_DEPLOY_TIMEOUT", "900"))


def _cli_path():
    """Path to the Databricks CLI, or None if it is not installed."""
    return os.environ.get("DATABRICKS_CLI_PATH") or shutil.which("databricks")


def _run(args, cwd, timeout: int = 120):
    """Run a subprocess, returning (returncode, combined stdout+stderr).

    stdin is closed so an unexpected CLI prompt fails fast instead of blocking
    the transport, and both streams are kept because the Databricks CLI writes
    its progress to stderr even on success.
    """
    try:
        proc = subprocess.run(
            args,
            cwd=cwd,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
        )
    except FileNotFoundError:
        return 127, f"`{args[0]}` not found on PATH."
    except subprocess.TimeoutExpired:
        return 124, f"`{' '.join(args)}` timed out after {timeout}s."
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def _deployed_job_names(cli: str, project_dir: str, target: str) -> str:
    """Workspace names of the bundle's jobs, read back from `bundle summary`.

    Best-effort: the deploy already succeeded by the time this runs, so any
    failure here just omits the detail rather than failing the tool.
    """
    rc, out = _run(
        [cli, "bundle", "summary", "--target", target, "--output", "json"],
        project_dir,
    )
    if rc != 0:
        return ""
    try:
        summary = json.loads(out).get("resources", {}).get("jobs", {})
    except (ValueError, AttributeError):
        return ""
    lines = []
    for key, info in summary.items():
        if not isinstance(info, dict):
            info = {}
        name = info.get("name") or key
        job_id = info.get("id")
        lines.append(f"     - {name}" + (f" (ID: {job_id})" if job_id else ""))
    return "\n".join(lines)


@mcp.tool()
def deploy(git_ref: str = "main", target: str = "dev") -> str:
    """
    Deploy the Databricks job via Declarative Automation Bundles.

    Checks out the specified git ref, pulls latest changes, then runs
    `databricks bundle deploy` against the chosen target environment.

    Args:
        git_ref: Branch or tag to checkout (default: main).
        target: DAB target to deploy to, matching a key in databricks.yml targets (default: dev).

    Returns:
        Deployment result including the workspace names DAB gave the deployed
        jobs, or any errors from git or the bundle CLI.
    """
    project_dir = os.environ.get("DAB_PROJECT_DIR", os.getcwd())

    if not os.path.isfile(os.path.join(project_dir, "databricks.yml")):
        return (
            f"❌ No databricks.yml in {project_dir}.\n"
            f"   Point DAB_PROJECT_DIR at the bundle root."
        )

    cli = _cli_path()
    if not cli:
        return (
            "❌ Databricks CLI not found on PATH.\n"
            "   Install it (https://docs.databricks.com/dev-tools/cli/install.html) "
            "or set DATABRICKS_CLI_PATH."
        )

    notes = []

    # --- Step 1: Git checkout & pull ---
    if os.path.isdir(os.path.join(project_dir, ".git")):
        rc, out = _run(["git", "fetch", "origin", git_ref], project_dir)
        if rc != 0:
            return f"❌ git fetch failed:\n{out}"

        rc, out = _run(["git", "checkout", git_ref], project_dir)
        if rc != 0:
            return f"❌ git checkout failed:\n{out}"

        # Only a branch can be pulled; a tag or SHA leaves a detached HEAD.
        rc, _ = _run(
            ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{git_ref}"],
            project_dir,
        )
        if rc == 0:
            rc, out = _run(["git", "pull", "--ff-only", "origin", git_ref], project_dir)
            if rc != 0:
                return f"❌ git pull failed:\n{out}"
        else:
            notes.append(f"   Note: '{git_ref}' is not a branch, checked out detached.")
    else:
        notes.append(f"   Note: {project_dir} is not a git repo, skipped checkout/pull.")

    rc, head = _run(["git", "rev-parse", "--short", "HEAD"], project_dir)
    commit = head if rc == 0 else "unknown"

    # --- Step 2: DAB deploy ---
    # --auto-approve: the CLI otherwise prompts on destructive or production
    # changes, and nothing can answer that over stdio.
    rc, out = _run(
        [cli, "bundle", "deploy", "--target", target, "--auto-approve"],
        project_dir,
        timeout=DEPLOY_TIMEOUT_SECONDS,
    )
    if rc != 0:
        return f"❌ bundle deploy failed:\n{out[-2000:]}"

    lines = [
        "✅ Deployed successfully",
        f"   Branch: {git_ref} @ {commit}",
        f"   Target: {target}",
    ]
    lines.extend(notes)
    deployed = _deployed_job_names(cli, project_dir, target)
    if deployed:
        lines.append(f"   Deployed jobs:\n{deployed}")
    if out:
        lines.append(f"   Output: {out[-800:]}")
    return "\n".join(lines)


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
    try:
        job = _find_job(client, job_name)
    except JobNotResolved as exc:
        return f"❌ {exc}"

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

# Result states that mean the run did not finish cleanly.
_FAILED_RESULT_STATES = {
    RunResultState.FAILED,
    RunResultState.TIMEDOUT,
    RunResultState.SUCCESS_WITH_FAILURES,
    RunResultState.UPSTREAM_FAILED,
}


def _termination_summary(holder) -> Optional[str]:
    """Termination code + message from the newer `status` field, when the SDK exposes it."""
    status = getattr(holder, "status", None)
    details = getattr(status, "termination_details", None) if status else None
    if not details:
        return None
    code = getattr(details, "code", None)
    code = code.value if hasattr(code, "value") else code
    message = getattr(details, "message", None)
    return " - ".join(str(p) for p in (code, message) if p) or None


def _task_output_lines(client: WorkspaceClient, task_run_id: int, indent: str = "   ") -> list:
    """Exception and stack trace for a single *task* run.

    `get_run_output` only accepts a task run_id. Passing the job-level run_id of a
    multi-task run - and every DAB job uses the multi-task format, even with one
    task - returns HTTP 400, which is why in-job errors never surfaced here.
    """
    try:
        output = client.jobs.get_run_output(run_id=task_run_id)
    except Exception as exc:
        return [f"{indent}(Could not retrieve output for task run {task_run_id}: {exc})"]

    lines = []
    if output.error:
        lines.append(f"{indent}Exception   : {output.error}")
    if output.error_trace:
        trace = output.error_trace[-4000:]  # last 4000 chars
        lines.append(f"{indent}Stack Trace :\n{trace}")
    notebook_output = getattr(output, "notebook_output", None)
    if notebook_output is not None and getattr(notebook_output, "result", None):
        lines.append(f"{indent}Notebook Exit: {notebook_output.result}")
    if not lines:
        lines.append(f"{indent}(No exception trace recorded for this task.)")
    return lines


@mcp.tool()
def get_job_status(run_id: Optional[int] = None, job_name: str = "sample_transform_job") -> str:
    """
    Check the status of a job run. Returns lifecycle state, result, duration,
    and full error details (error code + exception trace) on failure.

    Args:
        run_id: Specific run ID. If omitted, fetches the most recent run.
        job_name: Used to resolve the latest run when run_id is not provided.

    Returns:
        Structured status report; includes per-task stack traces if the run failed.
    """
    client = _get_client()

    # Resolve run_id if not provided
    if run_id is None:
        try:
            job = _find_job(client, job_name)
        except JobNotResolved as exc:
            return f"❌ {exc}"
        runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
        if not runs:
            return f"ℹ️  No runs recorded for '{job_name}'."
        run_id = runs[0].run_id

    run = client.jobs.get_run(run_id=run_id)
    state = run.state
    lifecycle = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
    result_state = state.result_state if state else None

    lines = [
        "📋 Job Run Status",
        f"   Run ID      : {run.run_id}",
        f"   Job         : {run.run_name or job_name}",
        f"   Lifecycle   : {lifecycle}",
    ]

    if result_state:
        lines.append(f"   Result      : {result_state.value}")

    if run.start_time:
        lines.append(f"   Started     : {datetime.fromtimestamp(run.start_time / 1000).isoformat()}")

    duration_ms = run.execution_duration or run.run_duration
    if duration_ms:
        m, s = divmod(int(duration_ms / 1000), 60)
        lines.append(f"   Duration    : {m}m {s}s")

    if run.run_page_url:
        lines.append(f"   Run Page    : {run.run_page_url}")

    failed = (
        result_state in _FAILED_RESULT_STATES
        or (state and state.life_cycle_state == RunLifeCycleState.INTERNAL_ERROR)
    )

    # --- Failure details ---
    if failed:
        lines.append("")
        lines.append("   ❌ FAILURE DETAILS")
        if state and state.state_message:
            lines.append(f"   Message     : {state.state_message}")
        summary = _termination_summary(run)
        if summary:
            lines.append(f"   Termination : {summary}")

        tasks = run.tasks or []
        failed_tasks = [
            t for t in tasks if t.state and t.state.result_state in _FAILED_RESULT_STATES
        ]
        if not failed_tasks:
            # Run failed before any task reported a result (compute/setup errors).
            failed_tasks = [
                t for t in tasks
                if not (t.state and t.state.result_state == RunResultState.SUCCESS)
            ]

        for task in failed_tasks:
            ts = task.state
            lines.append("")
            lines.append(f"   -- Task '{task.task_key}' (task run_id: {task.run_id}) --")
            if ts and ts.result_state:
                lines.append(f"   State       : {ts.result_state.value}")
            if ts and ts.state_message:
                lines.append(f"   Task Error  : {ts.state_message}")
            task_summary = _termination_summary(task)
            if task_summary:
                lines.append(f"   Termination : {task_summary}")
            if task.run_id:
                lines.extend(_task_output_lines(client, task.run_id))

        if not tasks:
            # Legacy single-task run: the job run_id *is* the task run_id.
            lines.extend(_task_output_lines(client, run_id))

    elif state and state.life_cycle_state == RunLifeCycleState.RUNNING:
        lines.append("\n   🔄 Currently running...")

    elif result_state == RunResultState.SUCCESS:
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
        try:
            job = _find_job(client, job_name)
        except JobNotResolved as exc:
            return f"❌ {exc}"
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
