#!/usr/bin/env python3
"""
Detect dbt-managed schemas that a change leaves orphaned.

Compares the production manifest (downloaded by get_artifacts.sh into
logs/manifest.json) against the manifest built from the current project
(target/manifest.json). A schema that production dbt built, and that no node in
the current project builds any more, is orphaned: its relations are still in
Snowflake but nothing in dbt maintains them.

Why this exists: drop_orphaned_relations only scans schemas that still contain
at least one dbt node, so deleting *every* model in a schema hides the
leftovers from it. That is how L1_ERP, L2_VENDORS, L2_INVOICES,
L2_PURCHASE_ORDERS and L3_ACCOUNTS_PAYABLE survived PR #294.

Ownership test: a schema is only ever reported when it appears in the
production dbt manifest, which is what proves dbt built it. Schemas owned by
Airbyte, dlt, Cartage, Fivetran or Snowcap never appear in a dbt manifest and
so can never be flagged here. drop_orphaned_schemas applies a second guard
before dropping anything.

Schemas are keyed by name only, not database.schema, on purpose: the PR job
builds into BALBOA_PR_<n> and the blue-green deploy builds into the staging
database, so database names differ between the two manifests being compared.
The database actually operated on is chosen by the caller.

Usage (run from DATACOVES__DBT_HOME, or set it in the environment):
  orphaned_schemas.py --format json      # {"L2_VENDORS": ["DIM_VENDORS"], ...}
  orphaned_schemas.py --format markdown  # PR comment body
  orphaned_schemas.py --format github    # found/schemas/comment -> $GITHUB_OUTPUT
"""

import argparse
import json
import os
import sys

# Resource types that dbt materializes into a schema. Tests write to
# DBT_TEST__AUDIT, which keeps living as long as any test does, so they are not
# part of the ownership picture.
MANAGED_RESOURCE_TYPES = {"model", "seed", "snapshot"}

# Refuse to report if more than this share of production schemas look orphaned.
# A truncated or wrong-project manifest looks exactly like "everything was
# deleted", and that must never reach a DROP.
MAX_ORPHANED_SHARE = 0.5


def load_manifest(path):
    """Return a parsed manifest, or None when it is missing or empty."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return None
    with open(path) as manifest_file:
        return json.load(manifest_file)


def schema_map(manifest):
    """Map SCHEMA -> sorted relation names that dbt builds in it."""
    schemas = {}

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") not in MANAGED_RESOURCE_TYPES:
            continue

        # Ephemeral models are CTEs, never relations, so they neither keep a
        # schema alive nor leave anything behind.
        config = node.get("config") or {}
        if config.get("materialized") == "ephemeral":
            continue

        schema = (node.get("schema") or "").upper()
        relation = (node.get("alias") or node.get("name") or "").upper()
        if not schema or not relation:
            continue

        schemas.setdefault(schema, set()).add(relation)

    return {schema: sorted(relations) for schema, relations in schemas.items()}


def find_orphaned(prod_manifest, current_manifest):
    """Return {SCHEMA: [relations prod dbt built there]} for orphaned schemas."""
    prod_schemas = schema_map(prod_manifest)
    current_schemas = schema_map(current_manifest)

    if not current_schemas:
        raise ValueError(
            "The current manifest contains no models, seeds or snapshots. "
            "Refusing to treat every production schema as orphaned."
        )

    orphaned = {
        schema: relations
        for schema, relations in prod_schemas.items()
        if schema not in current_schemas
    }

    if orphaned and len(orphaned) > len(prod_schemas) * MAX_ORPHANED_SHARE:
        raise ValueError(
            f"{len(orphaned)} of {len(prod_schemas)} production schemas look "
            "orphaned, which is more than this check will ever report. "
            "Verify the manifests are from the same project before continuing."
        )

    return orphaned


def as_markdown(orphaned):
    """Build the sticky PR comment body."""
    lines = [
        "## 🗑️ Schemas this PR stops managing",
        "",
        "These schemas are built by production dbt today, and no model, seed or "
        "snapshot in this branch builds into them any more. **The next "
        "production deploy will drop them**, along with the relations listed "
        "below.",
        "",
        "| Schema | Relations dbt builds there today |",
        "| --- | --- |",
    ]

    for schema, relations in sorted(orphaned.items()):
        lines.append(f"| `{schema}` | {', '.join(f'`{r}`' for r in relations)} |")

    lines += [
        "",
        "This is expected when you intend to retire a data domain, and nothing "
        "is required of you. The deploy skips any schema that holds an object "
        "dbt did not build, so anything another tool has put there is safe.",
        "",
        "**Before merging, check that nothing outside dbt still reads these** — "
        "dashboards, Streamlit apps and `visualize/dbt_charts` queries are not "
        "part of the dbt graph, so no CI check can see those references.",
        "",
        "To keep a schema, keep at least one dbt model building into it.",
    ]

    return "\n".join(lines)


def write_github_output(orphaned):
    """Expose found/schemas/comment to later steps and jobs."""
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return

    with open(github_output, "a") as output_file:
        output_file.write(f"found={'true' if orphaned else 'false'}\n")
        output_file.write(f"schemas={json.dumps(orphaned, separators=(',', ':'))}\n")

        if orphaned:
            delimiter = "ORPHANED_SCHEMAS_EOF"
            output_file.write(f"comment<<{delimiter}\n{as_markdown(orphaned)}\n{delimiter}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format",
        choices=["json", "markdown", "github"],
        default="json",
        help="json: --args payload. markdown: PR comment. github: $GITHUB_OUTPUT.",
    )
    parser.add_argument("--prod-manifest", default="logs/manifest.json")
    parser.add_argument("--current-manifest", default="target/manifest.json")
    args = parser.parse_args()

    dbt_home = os.environ.get("DATACOVES__DBT_HOME")
    if dbt_home:
        os.chdir(dbt_home)

    prod_manifest = load_manifest(args.prod_manifest)
    current_manifest = load_manifest(args.current_manifest)

    # No production manifest means there is nothing to compare against, which
    # is a normal first-deploy state rather than a failure.
    if prod_manifest is None:
        print(f"No production manifest at {args.prod_manifest}; nothing to compare.", file=sys.stderr)
        orphaned = {}
    elif current_manifest is None:
        print(f"No current manifest at {args.current_manifest}; run dbt first.", file=sys.stderr)
        return 1
    else:
        try:
            orphaned = find_orphaned(prod_manifest, current_manifest)
        except ValueError as error:
            print(f"Orphaned schema check failed: {error}", file=sys.stderr)
            return 1

    if orphaned:
        print(
            f"{len(orphaned)} schema(s) no longer built by dbt: "
            f"{', '.join(sorted(orphaned))}",
            file=sys.stderr,
        )
    else:
        print("No orphaned dbt-managed schemas found.", file=sys.stderr)

    if args.format == "github":
        write_github_output(orphaned)
    elif args.format == "markdown":
        print(as_markdown(orphaned) if orphaned else "")
    else:
        print(json.dumps(orphaned, separators=(",", ":")))

    return 0


if __name__ == "__main__":
    sys.exit(main())
