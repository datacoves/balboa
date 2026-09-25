#!/usr/bin/env python3
"""
Manage the shared Airflow development branch (airflow_development).

The dev branch is never edited by hand. It is always:

    <base commit of main>  +  <registered feature branches, each pinned to a SHA>

The list of registered branches lives in a file (.airflow_dev.json) on the dev
branch itself, so no external state or CI is required. All git operations run
in a temporary worktree, so your current checkout is never touched.

Commands:
  add [BRANCH]      Deploy BRANCH (default: current branch) to the dev branch.
                    Re-run it to deploy new commits. Refuses if the branch is
                    not up to date with main, or if it conflicts with branches
                    already deployed.
  remove [BRANCH]   Remove BRANCH (default: current branch) and rebuild the dev
                    branch from the same main commit with the remaining branches.
  status            Show what is deployed and what is out of date.
  refresh [--backup]
                    Rebuild on the latest main. Drops branches that were merged
                    or deleted, and branches that no longer merge cleanly.
  reset --yes [--backup]
                    Point the dev branch at main with nothing deployed.
                    Use this once to adopt the workflow on an existing repo.

  --backup first copies the dev branch to <dev branch>__bkp_YYYYMMDD on the
  remote, so nothing is lost when the branch is rewritten.

Configuration (environment variables):
  AIRFLOW_DEV_BRANCH   Dev branch name     (default: airflow_development)
  AIRFLOW_DEV_BASE     Base branch name    (default: main)
  AIRFLOW_DEV_REMOTE   Git remote name     (default: origin)
"""

import argparse
import getpass
import json
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone

DEV_BRANCH = os.environ.get("AIRFLOW_DEV_BRANCH", "airflow_development")
BASE_BRANCH = os.environ.get("AIRFLOW_DEV_BASE", "main")
REMOTE = os.environ.get("AIRFLOW_DEV_REMOTE", "origin")

REGISTRY_FILE = ".airflow_dev.json"
COMMIT_PREFIX = "airflow-dev:"
PUSH_RETRIES = 3


class AirflowDevError(Exception):
    pass


class PushRejected(Exception):
    pass


class MergeConflict(Exception):
    def __init__(self, files):
        super().__init__(", ".join(files))
        self.files = files


# --------------------------------------------------------------------------- #
# git helpers
# --------------------------------------------------------------------------- #

def git(*args, cwd=None, check=True):
    """Run git (with hooks disabled) and return stripped stdout."""
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise AirflowDevError(
            f"❌ git {' '.join(args)} failed:\n{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def git_ok(*args, cwd=None):
    return subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    ).returncode == 0


def remote_ref(branch):
    return f"refs/remotes/{REMOTE}/{branch}"


def remote_sha(branch):
    """SHA of REMOTE/branch, or None if it does not exist."""
    return git("rev-parse", "--verify", "-q", remote_ref(branch), check=False) or None


def is_ancestor(ancestor, descendant):
    return git_ok("merge-base", "--is-ancestor", ancestor, descendant)


def short(sha):
    return sha[:8] if sha else "-"


def current_user():
    return git("config", "user.name", check=False) or getpass.getuser()


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch():
    print(f"Fetching {REMOTE}...")
    git("fetch", "--prune", REMOTE)


@contextmanager
def worktree(start_sha):
    """Temporary detached worktree at start_sha, removed afterwards."""
    path = tempfile.mkdtemp(prefix="airflow_dev_")
    git("worktree", "add", "--detach", "--quiet", path, start_sha)
    try:
        yield path
    finally:
        git("worktree", "remove", "--force", path, check=False)
        shutil.rmtree(path, ignore_errors=True)
        git("worktree", "prune", check=False)


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #

def read_registry():
    """Return (dev_sha, registry) for the remote dev branch."""
    dev_sha = remote_sha(DEV_BRANCH)
    if not dev_sha:
        raise AirflowDevError(
            f"❌ {REMOTE}/{DEV_BRANCH} does not exist. "
            f"Run: {sys.argv[0]} reset --yes"
        )
    raw = git("show", f"{dev_sha}:{REGISTRY_FILE}", check=False)
    if not raw:
        raise AirflowDevError(
            f"❌ {DEV_BRANCH} is not managed by this script yet (no {REGISTRY_FILE}).\n"
            f"   An admin must run once: {sys.argv[0]} reset --yes"
        )
    return dev_sha, json.loads(raw)


def write_registry(path, registry):
    with open(os.path.join(path, REGISTRY_FILE), "w") as f:
        json.dump(registry, f, indent=2)
        f.write("\n")


def find_entry(registry, branch):
    return next((e for e in registry["branches"] if e["name"] == branch), None)


def changed_files(sha):
    """Files a branch changes relative to where it forked from main."""
    base = remote_ref(BASE_BRANCH)
    out = git("diff", "--name-only", f"{base}...{sha}", check=False)
    return set(out.splitlines())


# --------------------------------------------------------------------------- #
# building the dev branch
# --------------------------------------------------------------------------- #

def merge(path, sha, message):
    """Merge sha into the worktree. Raises MergeConflict (after aborting)."""
    if git_ok("merge", "--no-ff", "--no-edit", "-m", message, sha, cwd=path):
        return
    files = git("diff", "--name-only", "--diff-filter=U", cwd=path, check=False)
    git("merge", "--abort", cwd=path, check=False)
    raise MergeConflict(files.splitlines())


def commit_registry(path, registry, message):
    write_registry(path, registry)
    git("add", REGISTRY_FILE, cwd=path)
    git("commit", "--quiet", "--no-verify", "-m", message, cwd=path)


def push(path, expected_sha, force):
    """Push the worktree HEAD to the dev branch, guarded by expected_sha."""
    args = ["push", "--quiet"]
    if force:
        args.append(f"--force-with-lease=refs/heads/{DEV_BRANCH}:{expected_sha or ''}")
    args += [REMOTE, f"HEAD:refs/heads/{DEV_BRANCH}"]
    if not git_ok(*args, cwd=path):
        raise PushRejected()


def rebuild(base_sha, entries, reason):
    """
    Build the dev branch from base_sha plus entries (in order) and force push.
    Entries that fail to merge are skipped. Returns the list of skipped
    (entry, conflicted_files) tuples.
    """
    dev_sha = remote_sha(DEV_BRANCH)
    kept, skipped = [], []
    with worktree(base_sha) as path:
        for entry in entries:
            try:
                merge(path, entry["sha"], merge_message(entry))
                kept.append(entry)
            except MergeConflict as ex:
                skipped.append((entry, ex.files))
        registry = {
            "base": {"branch": BASE_BRANCH, "sha": base_sha},
            "branches": kept,
        }
        commit_registry(path, registry, f"{COMMIT_PREFIX} {reason}")
        push(path, dev_sha, force=True)
    return skipped


def merge_message(entry):
    return f"{COMMIT_PREFIX} deploy {entry['name']} ({short(entry['sha'])}) by {entry['user']}"


def with_retries(fn):
    """Re-fetch and retry fn when someone else pushed the dev branch first."""
    for attempt in range(1, PUSH_RETRIES + 1):
        try:
            return fn()
        except PushRejected:
            if attempt == PUSH_RETRIES:
                raise AirflowDevError(
                    f"❌ {DEV_BRANCH} kept changing while we tried to push. Try again."
                )
            print(f"{DEV_BRANCH} was updated by someone else, retrying...")
            fetch()


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #

def resolve_branch(branch):
    if branch:
        return branch
    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    if branch == "HEAD":
        raise AirflowDevError("❌ You are in detached HEAD. Pass a branch name.")
    return branch


def ensure_pushed(branch):
    """Make sure REMOTE/branch has the local commits. Pushes if fast-forward."""
    local = git("rev-parse", "--verify", "-q", f"refs/heads/{branch}", check=False)
    remote = remote_sha(branch)
    if not local:
        if not remote:
            raise AirflowDevError(f"❌ Branch '{branch}' not found locally or on {REMOTE}.")
        return remote
    if local == remote:
        return remote
    if remote and is_ancestor(local, remote):
        print(f"⚠️  Local '{branch}' is behind {REMOTE}; deploying the {REMOTE} version.")
        return remote
    if remote is None or is_ancestor(remote, local):
        print(f"Pushing '{branch}' to {REMOTE}...")
        git("push", "--quiet", REMOTE, f"refs/heads/{branch}:refs/heads/{branch}")
        git("fetch", "--quiet", REMOTE, branch)
        return local
    raise AirflowDevError(
        f"❌ Local '{branch}' and {REMOTE}/{branch} have diverged. "
        f"Pull and push your branch first."
    )


def validate_branch(branch, sha):
    if branch in (DEV_BRANCH, BASE_BRANCH):
        raise AirflowDevError(f"❌ You cannot deploy '{branch}' itself.")

    base_sha = remote_sha(BASE_BRANCH)
    if not is_ancestor(base_sha, sha):
        behind = git("rev-list", "--count", f"{sha}..{base_sha}")
        raise AirflowDevError(
            f"❌ '{branch}' is {behind} commit(s) behind {BASE_BRANCH}. Update it first:\n"
            f"     git checkout {branch} && git pull {REMOTE} {BASE_BRANCH} && git push\n"
            f"   then run this command again."
        )

    polluted = git("log", "--format=%h %s", "-F", f"--grep={COMMIT_PREFIX}",
                   f"{base_sha}..{sha}", check=False)
    if polluted or git_ok("cat-file", "-e", f"{sha}:{REGISTRY_FILE}"):
        raise AirflowDevError(
            f"❌ '{branch}' contains commits from {DEV_BRANCH} (someone merged {DEV_BRANCH} "
            f"into it).\n   That would deploy other people's work and must never reach "
            f"{BASE_BRANCH}.\n   Recreate your branch from {BASE_BRANCH} with only your changes."
        )


def cmd_add(args):
    branch = resolve_branch(args.branch)
    fetch()
    if git("status", "--porcelain", check=False) and not args.branch:
        print("⚠️  You have uncommitted changes. They will NOT be deployed.")
    sha = ensure_pushed(branch)
    validate_branch(branch, sha)

    def attempt():
        dev_sha, registry = read_registry()
        existing = find_entry(registry, branch)
        if existing and existing["sha"] == sha:
            print(f"✅ '{branch}' ({short(sha)}) is already deployed. Nothing to do.")
            return
        entry = {"name": branch, "sha": sha, "user": current_user(), "added": now()}
        with worktree(dev_sha) as path:
            try:
                merge(path, sha, merge_message(entry))
            except MergeConflict as ex:
                report_conflict(branch, ex.files, registry)
            registry["branches"] = [e for e in registry["branches"] if e["name"] != branch]
            registry["branches"].append(entry)
            if git("rev-parse", "HEAD", cwd=path) == dev_sha:
                # Already contained in the dev branch, so no merge commit was made
                commit_registry(path, registry, merge_message(entry))
            else:
                write_registry(path, registry)
                git("add", REGISTRY_FILE, cwd=path)
                git("commit", "--quiet", "--amend", "--no-edit", "--no-verify", cwd=path)
            push(path, dev_sha, force=False)
        verb = "Updated" if existing else "Deployed"
        print(f"✅ {verb} '{branch}' ({short(sha)}) on {DEV_BRANCH}.")

    with_retries(attempt)


def report_conflict(branch, files, registry):
    owners = []
    files = set(files)
    for entry in registry["branches"]:
        if entry["name"] == branch:
            continue
        overlap = files & changed_files(entry["sha"])
        if overlap:
            owners.append(f"     - {entry['name']} ({entry['user']}): {', '.join(sorted(overlap))}")
    msg = [f"❌ '{branch}' conflicts with what is deployed on {DEV_BRANCH}.",
           "   Conflicting files:"]
    msg += [f"     - {f}" for f in sorted(files)]
    if owners:
        msg += ["   Deployed branches touching these files:"] + owners
        msg.append("   Coordinate with the owners, or ask them to run 'remove' when done.")
    else:
        msg.append(f"   The conflict may be with newer {BASE_BRANCH} changes; "
                   f"ask an admin to run 'refresh'.")
    raise AirflowDevError("\n".join(msg))


def cmd_remove(args):
    branch = resolve_branch(args.branch)
    fetch()

    def attempt():
        _, registry = read_registry()
        if not find_entry(registry, branch):
            print(f"'{branch}' is not deployed on {DEV_BRANCH}. Nothing to do.")
            return
        remaining = [e for e in registry["branches"] if e["name"] != branch]
        print(f"Rebuilding {DEV_BRANCH} without '{branch}' ({len(remaining)} branch(es) remain)...")
        skipped = rebuild(registry["base"]["sha"], remaining, f"remove {branch}")
        print(f"✅ Removed '{branch}' from {DEV_BRANCH}.")
        report_skipped(skipped)

    with_retries(attempt)


def cmd_refresh(args):
    fetch()

    def attempt():
        _, registry = read_registry()
        base_sha = remote_sha(BASE_BRANCH)
        keep, dropped = [], []
        for entry in registry["branches"]:
            if not remote_sha(entry["name"]):
                dropped.append(f"{entry['name']} (branch deleted)")
            elif is_ancestor(entry["sha"], base_sha):
                dropped.append(f"{entry['name']} (merged into {BASE_BRANCH})")
            else:
                keep.append(entry)
        if args.backup:
            backup_dev_branch()
        print(f"Rebuilding {DEV_BRANCH} on {BASE_BRANCH} ({short(base_sha)}) "
              f"with {len(keep)} branch(es)...")
        skipped = rebuild(base_sha, keep, f"refresh on {BASE_BRANCH} {short(base_sha)}")
        print(f"✅ {DEV_BRANCH} rebuilt on latest {BASE_BRANCH}.")
        for d in dropped:
            print(f"   dropped: {d}")
        report_skipped(skipped)

    with_retries(attempt)


def report_skipped(skipped):
    if not skipped:
        return
    print(f"⚠️  These branches no longer merge cleanly and were removed from {DEV_BRANCH}:")
    for entry, files in skipped:
        print(f"   - {entry['name']} ({entry['user']}): {', '.join(files)}")
    print(f"   Owners should update their branch from {BASE_BRANCH} and run 'add' again.")


def cmd_reset(args):
    if not args.yes:
        raise AirflowDevError(
            f"❌ This replaces {DEV_BRANCH} with {BASE_BRANCH} and removes every "
            f"deployed branch. Re-run with --yes to confirm."
        )
    fetch()
    base_sha = remote_sha(BASE_BRANCH)

    def attempt():
        if args.backup:
            backup_dev_branch()
        rebuild(base_sha, [], f"reset to {BASE_BRANCH} {short(base_sha)}")

    with_retries(attempt)
    print(f"✅ {DEV_BRANCH} reset to {BASE_BRANCH} ({short(base_sha)}).")


def backup_dev_branch():
    """Copy the remote dev branch to <dev>__bkp_<YYYYMMDD>. Never overwrites."""
    dev_sha = remote_sha(DEV_BRANCH)
    if not dev_sha:
        print(f"{REMOTE}/{DEV_BRANCH} does not exist, nothing to back up.")
        return
    raw = git("show", f"{dev_sha}:{REGISTRY_FILE}", check=False)
    if raw and not json.loads(raw)["branches"]:
        print(f"No branches deployed on {DEV_BRANCH}, nothing to back up.")
        return
    name = f"{DEV_BRANCH}__bkp_{datetime.now().strftime('%Y%m%d')}"
    existing = remote_sha(name)
    if existing == dev_sha:
        print(f"Backup {name} already has the current {DEV_BRANCH}.")
        return
    if existing:
        name += datetime.now().strftime("_%H%M%S")
    git("push", "--quiet", REMOTE, f"{dev_sha}:refs/heads/{name}")
    print(f"✅ Backed up {DEV_BRANCH} ({short(dev_sha)}) to {REMOTE}/{name}.")


def cmd_status(args):
    fetch()
    _, registry = read_registry()
    base_sha = registry["base"]["sha"]
    latest = remote_sha(BASE_BRANCH)
    behind = git("rev-list", "--count", f"{base_sha}..{latest}")
    print(f"{DEV_BRANCH}: based on {BASE_BRANCH} {short(base_sha)} "
          f"({behind} commit(s) behind latest {BASE_BRANCH})")
    if not registry["branches"]:
        print("No feature branches deployed.")
        return
    print(f"{len(registry['branches'])} deployed branch(es):")
    for e in registry["branches"]:
        tip = remote_sha(e["name"])
        if not tip:
            note = "branch deleted"
        elif is_ancestor(e["sha"], latest):
            note = f"merged into {BASE_BRANCH}"
        elif tip != e["sha"]:
            note = "newer commits not deployed, run 'add' again"
        else:
            note = "up to date"
        print(f"  {e['name']:<45} {short(e['sha'])}  {e['user']:<20} {e['added'][:10]}  {note}")


def main():
    parser = argparse.ArgumentParser(
        description=f"Manage the shared {DEV_BRANCH} branch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Commands:")[1],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("add", help="deploy a branch (default: current)")
    p.add_argument("branch", nargs="?")
    p.set_defaults(func=cmd_add)

    p = sub.add_parser("remove", help="remove a branch (default: current)")
    p.add_argument("branch", nargs="?")
    p.set_defaults(func=cmd_remove)

    sub.add_parser("status", help="show deployed branches").set_defaults(func=cmd_status)
    p = sub.add_parser("refresh", help="rebuild on latest main")
    p.add_argument("--backup", action="store_true",
                   help=f"first copy {DEV_BRANCH} to {DEV_BRANCH}__bkp_YYYYMMDD")
    p.set_defaults(func=cmd_refresh)

    p = sub.add_parser("reset", help="reset to main with nothing deployed")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--backup", action="store_true",
                   help=f"first copy {DEV_BRANCH} to {DEV_BRANCH}__bkp_YYYYMMDD")
    p.set_defaults(func=cmd_reset)

    args = parser.parse_args()
    os.chdir(git("rev-parse", "--show-toplevel"))
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except AirflowDevError as ex:
        print(ex)
        sys.exit(1)
