#!/usr/bin/env python3
"""Report why dbt cannot partial parse.

Run from a datacoves_dbt task so the dbt-api artifacts are already downloaded.
The task runs against the read-write copy of the repo at $DATACOVES__REPO_PATH,
not the read-only clone $DATACOVES__DBT_HOME points into, so this script resolves
the project the same way.
dbt reads the saved manifest from <target-path>/partial_parse.msgpack and
rejects it if any of five checks fail. Only one of them names a reason
specific enough to act on, and dbt never says WHICH project or value differed,
so this prints both sides of every comparison.

Note the dbt version check runs FIRST and returns immediately, so a version
skew between the container that built the artifact and this one hides every
other reason.

Output goes to stdout (the task log) and to <project>/logs/partial_parse_debug/.
"""

import argparse
import glob
import hashlib
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

import msgpack

# dbt hashes the profile name, the target name and the cli vars into vars_hash,
# and the selected target's connection info into profile_hash, so the profile
# and target this script parses with decide two of the five checks. Pass the
# ones an Airflow task uses to reproduce what the task sees.
parser = argparse.ArgumentParser(
    description="Report why dbt cannot partial parse.",
    epilog="These flags are passed through to the live `dbt parse`.",
)
parser.add_argument("-t", "--target", help="dbt target, e.g. -t default")
parser.add_argument("--profile", help="dbt profile name (default: the one in dbt_project.yml)")
parser.add_argument("--profiles-dir", help="directory holding profiles.yml (default: ~/.dbt)")
parser.add_argument("--vars", help="dbt vars as a YAML/JSON string")
ARGS = parser.parse_args()

DBT_FLAGS = []
for _flag, _value in (
    ("--target", ARGS.target),
    ("--profile", ARGS.profile),
    ("--profiles-dir", ARGS.profiles_dir),
    ("--vars", ARGS.vars),
):
    if _value:
        DBT_FLAGS += [_flag, _value]
DBT_FLAGS_STR = " ".join(shlex.quote(a) for a in DBT_FLAGS)

# Datacoves gives an Airflow task two copies of the repo. DATACOVES__REPO_PATH_RO
# is the read-only clone the scheduler keeps in sync, and DATACOVES__REPO_PATH is
# the read-write copy the task actually runs from -- that copy is where the
# dbt-api artifacts are downloaded and the only one this script can write to.
# DATACOVES__DBT_HOME points into the read-only clone, so use it for the dbt
# project's path relative to the repo root, never as the project directory.
CWD = os.getcwd()
REPO_PATH = os.environ.get("DATACOVES__REPO_PATH", "")
REPO_PATH_RO = os.environ.get("DATACOVES__REPO_PATH_RO", "")
DBT_HOME_ENV = os.environ.get("DATACOVES__DBT_HOME", "")


def dbt_subdir():
    """The dbt project's path relative to the repo root, e.g. 'transform'."""
    for repo in (REPO_PATH_RO, REPO_PATH):
        if repo and DBT_HOME_ENV:
            rel = os.path.relpath(DBT_HOME_ENV, repo)
            if rel != os.curdir and not rel.startswith(os.pardir):
                return rel
    return os.path.basename(os.path.normpath(DBT_HOME_ENV)) or "transform"


def pick_project_dir():
    """Prefer the read-write copy: the task runs there, the artifacts land there,
    and this script writes its output next to the project."""
    candidates = []
    if REPO_PATH:
        candidates.append(os.path.join(REPO_PATH, dbt_subdir()))
    candidates += [CWD, DBT_HOME_ENV]
    for cand in candidates:
        if cand and os.path.exists(os.path.join(cand, "dbt_project.yml")):
            return cand
    return CWD


PROJECT = pick_project_dir()
LOGS = os.path.join(PROJECT, "logs")
SCRATCH = os.path.join(LOGS, "partial_parse_debug")
PP = "partial_parse.msgpack"

# The worktree is owned by another uid, which makes git refuse to read it.
# Override per-invocation rather than mutating global config.
GIT = "git -c safe.directory='*'"

try:
    from importlib.metadata import version as pkg_version

    RUNNING_DBT = pkg_version("dbt-core")
except Exception:
    RUNNING_DBT = "?"


def rule(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=PROJECT)
    return (r.stdout + r.stderr).strip()


def git_blob(rev, path):
    """Raw file contents at a revision. Not stripped -- the hash covers the
    trailing newline, so trimming it changes the result."""
    r = subprocess.run(
        ["git", "-c", "safe.directory=*", "show", f"{rev}:./{path}"],
        capture_output=True,
        text=True,
        cwd=PROJECT,
    )
    return r.stdout if r.returncode == 0 else ""


def file_hash(path):
    """Same hash dbt stores: sha256 of the file's utf-8 contents."""
    try:
        with open(path) as fp:
            return hashlib.sha256(fp.read().encode("utf-8")).hexdigest()
    except OSError as exc:
        return f"<{exc.__class__.__name__}>"


def local_project_files():
    """Every dbt_project.yml dbt hashes: the root project and each dependency."""
    paths = {"<root>": os.path.join(PROJECT, "dbt_project.yml")}
    for p in sorted(glob.glob(os.path.join(PROJECT, "dbt_packages", "*", "dbt_project.yml"))):
        paths[os.path.basename(os.path.dirname(p))] = p

    # The global project and the adapter ship their own, inside the installed
    # python packages, so a different dbt or adapter build changes these.
    try:
        import dbt

        paths["dbt"] = os.path.join(
            os.path.dirname(dbt.__file__), "include", "global_project", "dbt_project.yml"
        )
    except ImportError:
        pass
    for mod in ("dbt.adapters.snowflake", "dbt.include.snowflake"):
        try:
            m = __import__(mod, fromlist=["__file__"])
        except ImportError:
            continue
        for cand in (
            os.path.join(os.path.dirname(m.__file__), "include", "snowflake", "dbt_project.yml"),
            os.path.join(os.path.dirname(m.__file__), "dbt_project.yml"),
        ):
            if os.path.exists(cand):
                paths["dbt_snowflake"] = cand
    return paths


rule("CONTEXT")
print(f"cwd                    : {CWD}")
print(f"DATACOVES__REPO_PATH   : {REPO_PATH or '<unset>'}  (read-write copy)")
print(f"DATACOVES__REPO_PATH_RO: {REPO_PATH_RO or '<unset>'}  (read-only clone)")
print(f"DATACOVES__DBT_HOME    : {DBT_HOME_ENV or '<unset>'}")
print(f"project dir in use     : {PROJECT}")
if CWD and os.path.normpath(CWD) != os.path.normpath(PROJECT):
    print("  ^ cwd differs from the project dir; dbt resolves the project from the")
    print("    working directory, so a dbt task run from here would parse a different tree")
if DBT_HOME_ENV and os.path.normpath(DBT_HOME_ENV) != os.path.normpath(PROJECT):
    print("    DATACOVES__DBT_HOME is the read-only clone and is not used as the project dir")
print(f"project writable       : {os.access(PROJECT, os.W_OK)}")
print(f"dbt parse flags        : {DBT_FLAGS_STR or '<none: profile from dbt_project.yml, default target>'}")
print(f"git branch             : {sh(f'{GIT} rev-parse --abbrev-ref HEAD')}")
print(f"git HEAD               : {sh(f'{GIT} rev-parse --short HEAD')}")
print(f"git describe           : {sh(f'{GIT} describe --tags --always')}")
dirty = sh(f"{GIT} status --porcelain dbt_project.yml")
print(f"dbt_project.yml        : {'MODIFIED in working tree: ' + dirty if dirty else 'clean'}")
print(f"project version        : {sh('grep -m1 ^version: dbt_project.yml')}")
print(f"dbt-core installed     : {RUNNING_DBT}")

rule("PARTIAL PARSE SETTINGS")
for var in (
    "DBT_PARTIAL_PARSE",
    "DBT_PARTIAL_PARSE_FILE_PATH",
    "DBT_TARGET_PATH",
    "DBT_TARGET",
    "DBT_PROFILES_DIR",
    "DBT_STATE",
):
    print(f"{var:32} {os.environ.get(var, '<unset>')}")

rule("WHERE THE SAVED MANIFEST IS")
# dbt uses PARTIAL_PARSE_FILE_PATH if set, else <target-path>/partial_parse.msgpack.
override = os.environ.get("DBT_PARTIAL_PARSE_FILE_PATH")
candidates = [
    ("target/", os.path.join(PROJECT, "target", PP)),
    ("logs/", os.path.join(LOGS, PP)),
]
if override:
    candidates.insert(0, ("DBT_PARTIAL_PARSE_FILE_PATH", override))

found = []
for label, path in candidates:
    if os.path.exists(path):
        print(f"  FOUND   {label:32} {path}  ({os.path.getsize(path):,} bytes)")
        found.append((label, path))
    else:
        print(f"  missing {label:32} {path}")

if not found:
    print("\nNo saved manifest anywhere -- dbt would say 'saved manifest not found'.")
    print("If the task log shows artifacts being downloaded, check the paths above")
    print("against where they landed.")
    sys.exit(0)

used_label, used_path = found[0]
print(f"\ndbt will read: {used_path}")

rule(f"SAVED STATE CHECK ({used_label})")
with open(used_path, "rb") as fp:
    saved = msgpack.unpackb(fp.read(), raw=False, strict_map_key=False)
check = saved.get("state_check", {})
meta = saved.get("metadata", {})
saved_dbt = meta.get("dbt_version")
print(f"generated_at        : {meta.get('generated_at')}")
print(f"invocation_id       : {meta.get('invocation_id')}")
print(f"dbt_version         : {saved_dbt}")
print(f"vars_hash           : {check.get('vars_hash', {}).get('checksum')}")
print(f"profile_hash        : {check.get('profile_hash', {}).get('checksum')}")
print(f"project_env_vars    : {check.get('project_env_vars_hash', {}).get('checksum')}")

if saved_dbt != RUNNING_DBT:
    print(f"\n*** VERSION SKEW: manifest written by dbt {saved_dbt}, running dbt {RUNNING_DBT}")
    print("*** dbt checks this FIRST and returns immediately, reporting")
    print('*** "Unable to do partial parsing because of a version mismatch".')
    print("*** Nothing below can be reached until the versions match.")
else:
    print(f"\ndbt version matches ({RUNNING_DBT}).")

rule("PROJECT HASH COMPARISON")
saved_hashes = check.get("project_hashes", {})
local = local_project_files()
root_name = sh("grep -m1 ^name: dbt_project.yml").split(":", 1)[-1].strip().strip("'\"")
if root_name:
    local[root_name] = local.pop("<root>")

print(f"{'project':24} {'saved':14} {'local':14} status")
mismatched = []
for name, val in sorted(saved_hashes.items()):
    s = val.get("checksum", "")
    l = file_hash(local[name]) if name in local else "<NOT INSTALLED>"
    status = "match" if s == l else "DIFFERS"
    if status == "DIFFERS":
        mismatched.append((name, local.get(name)))
    print(f"{name:24} {s[:12]:14} {l[:12]:14} {status}")

extra = [k for k in local if k not in saved_hashes and k != "<root>"]
if extra:
    print(f"\nInstalled but NOT in the saved manifest {extra}")
    print("dbt reports that as 'a project dependency has been added'.")

if mismatched:
    print(f"\nMISMATCHED: {[m[0] for m in mismatched]}")
    if root_name in dict(mismatched):
        # The hash is of the file's exact bytes, so walk this file's history and
        # find the revision the saved manifest was built from. That says whether
        # the checkout is behind, ahead, or on a different branch entirely.
        print("\nSearching git history for the revision matching the saved hash...")
        want = saved_hashes[root_name]["checksum"]
        for rev in sh(f"{GIT} log --all --format=%h -40 -- dbt_project.yml").split():
            blob = git_blob(rev, "dbt_project.yml")
            if hashlib.sha256(blob.encode("utf-8")).hexdigest() == want:
                ver = next(
                    (l.split(":", 1)[1].strip() for l in blob.splitlines() if l.startswith("version:")),
                    "?",
                )
                print(f"  saved manifest was built from {rev} (version {ver})")
                print(f"  this checkout is at            {sh(f'{GIT} rev-parse --short HEAD')}")
                break
        else:
            print("  No revision in the last 40 matches -- built from a tree not in")
            print("  this clone's history, or the file was edited locally.")
else:
    print("\nAll project hashes match.")

rule(f"LIVE dbt parse {DBT_FLAGS_STR or '(default profile/target)'} -- artifact left untouched")
# Copy the saved manifest into a scratch target dir and parse against it there,
# so dbt reports its own reason without overwriting the downloaded artifact
# that the real dbt task still needs.
try:
    os.makedirs(SCRATCH, exist_ok=True)
except OSError as exc:
    # Only reachable if we fell back to the read-only clone above.
    SCRATCH = os.path.join(tempfile.gettempdir(), "partial_parse_debug")
    print(f"{LOGS} is not writable ({exc.__class__.__name__}); using {SCRATCH}")
    os.makedirs(SCRATCH, exist_ok=True)
shutil.copy2(used_path, os.path.join(SCRATCH, PP))
out = sh(
    f"dbt parse --target-path {shlex.quote(SCRATCH)} --log-level debug {DBT_FLAGS_STR}".strip()
)
keep = (
    "partial parsing",
    "partial parse",
    "checksum:",
    "Unable to do",
)
for line in out.splitlines():
    if any(k.lower() in line.lower() for k in keep):
        print(line)

# dbt's own labels on the vars_hash line are inverted. In
# is_partial_parsable(self, manifest), `manifest` is the manifest read off disk
# and `self.manifest` is the one just built from this environment, so the line
# reads "previous checksum: <THIS RUN>, current checksum: <SAVED ARTIFACT>".
# dbt fires this on every parse, so it always reports what this run resolved to.
state = re.search(
    r"checksum: (\w+), vars: (.*?), profile: (\S+), target: (\S+), version:", out
)
pdir = re.search(r"'profiles_dir': '([^']*)'", out)
if state:
    print(
        f"\nthis run parsed as  : profile={state.group(3)} target={state.group(4)} "
        f"vars={state.group(2)}"
    )
    print(f"profiles.yml        : {pdir.group(1) if pdir else '?'}/profiles.yml")

if "previous checksum" in out:
    print("\nNOTE: dbt labels that line backwards --")
    print('  "previous checksum" is what THIS environment computes')
    print('  "current checksum"  is what the SAVED artifact carries')

rule("VERDICT")
reasons = [
    line.split("Unable to do partial parsing because", 1)[-1].strip()
    for line in out.splitlines()
    if "Unable to do partial parsing" in line
]
if reasons:
    print("dbt would NOT reuse the saved manifest. Reasons dbt gave:")
    for r in reasons:
        print(f"  - {r}")
    if not DBT_FLAGS and any("profile" in r or "config vars" in r for r in reasons):
        print("\nBoth of those depend on the profile and target. The artifact was likely")
        print("built with a different target -- re-run with, e.g., -t default to compare.")
elif "Partial parsing enabled" in out:
    print("dbt REUSED the saved manifest -- partial parsing succeeded.")
else:
    print("No partial parsing verdict in the log; read the full log below.")

with open(os.path.join(SCRATCH, "dbt_parse_debug.log"), "w") as fp:
    fp.write(out)
print(f"\nFull parse log: {os.path.join(SCRATCH, 'dbt_parse_debug.log')}")
