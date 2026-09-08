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

import json
import os
import re
import shutil
import statistics
import subprocess
import tempfile
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.workspace import ExportFormat, Language, ObjectType

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


# Bundle root on the app container. Databricks Apps only sync the source path
# passed to `apps deploy` (this app/ directory), so databricks.yml is normally
# absent here and gets exported from the workspace Git folder instead.
DAB_PROJECT_DIR = os.environ.get("DAB_PROJECT_DIR", "")

_SOURCE_SUFFIX = {
    Language.PYTHON: ".py",
    Language.SQL: ".sql",
    Language.SCALA: ".scala",
    Language.R: ".r",
}


def _pull_repo(client: WorkspaceClient, git_ref: str):
    """Pull the workspace Git folder to `git_ref`. Returns (head_commit, error).

    `repos.get`/`repos.update` take a numeric repo_id, not a path -- unlike the
    CLI's `repos get REPO_ID_OR_PATH`, which does this same path resolution
    client-side before calling the API. So the folder's object_id has to be
    looked up first, via `workspace.get_status`.

    This also sidesteps `repos.list(path_prefix=...)`, used previously: `list`
    is deprecated and explicitly excludes repos with Git CLI enabled, so it
    silently missed this folder -- reporting "not found" even though the
    folder was present and current.
    """
    try:
        status = client.workspace.get_status(WORKSPACE_REPO_PATH)
    except Exception as e:
        msg = str(e)
        if "RESOURCE_DOES_NOT_EXIST" in msg or "does not exist" in msg.lower():
            return None, (
                f"Git folder not found at {WORKSPACE_REPO_PATH}.\n"
                f"   Ensure the repo is cloned in the workspace."
            )
        return None, f"Could not look up {WORKSPACE_REPO_PATH}:\n   {e}"

    if status.object_id is None:
        return None, f"{WORKSPACE_REPO_PATH} has no object ID -- cannot resolve it to a repo."

    # `directory_info.is_git_folder` would be the direct check, but it's a
    # newer ObjectInfo field and isn't reliably present across SDK versions,
    # so it isn't trustworthy as a pre-check here. repos.update() against this
    # object_id is the operation that actually has to succeed, so let its own
    # success or failure be the answer instead of guessing beforehand.
    try:
        updated = client.repos.update(repo_id=status.object_id, branch=git_ref)
    except Exception as e:
        msg = str(e)
        if "RESOURCE_DOES_NOT_EXIST" in msg or "does not exist" in msg.lower() or "No API found" in msg:
            return None, (
                f"{WORKSPACE_REPO_PATH} exists but is not a Git folder ({e}).\n"
                f"   Ensure it was cloned as a Repo, not a plain workspace directory."
            )
        return None, f"Failed to pull latest from '{git_ref}':\n   {e}"
    return getattr(updated, "head_commit_id", "unknown"), None


def _export_workspace_dir(client: WorkspaceClient, remote: str, local: str) -> int:
    """Recursively download a workspace directory to local disk. Returns file count."""
    os.makedirs(local, exist_ok=True)
    count = 0
    for obj in client.workspace.list(remote):
        name = (obj.path or "").rsplit("/", 1)[-1]
        if not name:
            continue
        if obj.object_type == ObjectType.DIRECTORY:
            count += _export_workspace_dir(client, obj.path, os.path.join(local, name))
            continue
        if obj.object_type == ObjectType.NOTEBOOK:
            # Git folders store .py notebooks with the extension stripped, so
            # export as SOURCE and put the extension back for the bundle.
            data = client.workspace.download(obj.path, format=ExportFormat.SOURCE).read()
            suffix = _SOURCE_SUFFIX.get(obj.language, ".py")
            if not name.endswith(suffix):
                name += suffix
        elif obj.object_type == ObjectType.FILE:
            data = client.workspace.download(obj.path).read()
        else:
            continue
        with open(os.path.join(local, name), "wb") as fh:
            fh.write(data)
        count += 1
    return count


def _materialize_bundle(client: WorkspaceClient):
    """Get a bundle root on local disk, returning (path, error).

    Prefers DAB_PROJECT_DIR when it exists in the container; otherwise exports
    the freshly pulled workspace Git folder to a temp dir so the CLI has a
    databricks.yml to deploy.
    """
    if DAB_PROJECT_DIR and os.path.isfile(os.path.join(DAB_PROJECT_DIR, "databricks.yml")):
        return DAB_PROJECT_DIR, None

    dest = os.path.join(tempfile.gettempdir(), "dab-bundle")
    shutil.rmtree(dest, ignore_errors=True)
    try:
        count = _export_workspace_dir(client, WORKSPACE_REPO_PATH, dest)
    except Exception as e:
        return None, f"could not export the bundle from {WORKSPACE_REPO_PATH}: {e}"
    if not os.path.isfile(os.path.join(dest, "databricks.yml")):
        return None, (
            f"no databricks.yml found under {WORKSPACE_REPO_PATH} "
            f"({count} files exported)."
        )
    return dest, None


@mcp.tool()
def deploy(git_ref: str = "main", target: str = "dev") -> str:
    """
    Deploy the job: pull the latest code into the workspace Git folder, then run
    `databricks bundle deploy` so job definition changes actually take effect.

    Pulling alone only refreshes notebook source; changes to databricks.yml
    (schedules, tasks, tags, compute) need the bundle deploy.

    Args:
        git_ref: Branch or tag to sync to (default: main).
        target: DAB target to deploy to (default: dev).

    Returns:
        The result of both steps, and an explicit warning if the bundle deploy
        could not be run.
    """
    client = _get_client()

    head_commit, error = _pull_repo(client, git_ref)
    if error:
        return f"\u274c {error}"

    pulled = (
        f"\u2705 Pulled latest code\n"
        f"   Git folder: {WORKSPACE_REPO_PATH}\n"
        f"   Branch: {git_ref}\n"
        f"   Commit: {head_commit}\n"
    )

    cli = _cli_path()
    if not cli:
        return pulled + (
            "\n\u26a0\ufe0f  Bundle NOT deployed: the Databricks CLI is not available in this app\n"
            "   container, so only the Git folder was refreshed and databricks.yml changes\n"
            "   have not been applied. Install the CLI into the app image or set\n"
            "   DATABRICKS_CLI_PATH, or run the deploy from the local stdio server."
        )

    project_dir, error = _materialize_bundle(client)
    if error:
        return pulled + f"\n\u26a0\ufe0f  Bundle NOT deployed: {error}"

    # --auto-approve: nothing can answer an interactive prompt in an app container.
    rc, out = _run(
        [cli, "bundle", "deploy", "--target", target, "--auto-approve"],
        project_dir,
        timeout=DEPLOY_TIMEOUT_SECONDS,
    )
    if rc != 0:
        return pulled + f"\n\u274c bundle deploy failed:\n{out[-2000:]}"

    lines = [pulled.rstrip(), "", "\u2705 Bundle deployed", f"   Target: {target}"]
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
    Trigger a Databricks job run with duplicate prevention and ETA estimation.

    Args:
        job_name: Exact name of the deployed job (default: sample_transform_job).
        prevent_duplicate: Block if the job already has an active run (default: True).

    Returns:
        Run ID and estimated duration, or duplicate-run warning.
    """
    client = _get_client()
    try:
        job = _find_job(client, job_name)
    except JobNotResolved as exc:
        return f"\u274c {exc}"

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
            return f"\u274c {exc}"
        runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
        if not runs:
            return f"\u2139\ufe0f  No runs recorded for '{job_name}'."
        run_id = runs[0].run_id

    run = client.jobs.get_run(run_id=run_id)
    state = run.state
    lifecycle = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
    result_state = state.result_state if state else None

    lines = [
        "\U0001f4cb Job Run Status",
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
        lines.append("   \u274c FAILURE DETAILS")
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
        lines.append("\n   \U0001f504 Currently running...")

    elif result_state == RunResultState.SUCCESS:
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
        try:
            job = _find_job(client, job_name)
        except JobNotResolved as exc:
            return f"\u274c {exc}"
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
