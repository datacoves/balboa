# jq command line utility
Inspecting the manifest.json

## Get the entire file

`cat target/manifest.json | jq '.'`

## Get the top level keys
You can use the pipe within the jq command to call a function

`cat target/manifest.json | jq '. | keys'`

This will output something line
```
[
  "child_map",
  "disabled",
  "docs",
  "exposures",
  "group_map",
  "groups",
  "macros",
  "metadata",
  "metrics",
  "nodes",
  "parent_map",
  "saved_queries",
  "selectors",
  "semantic_models",
  "sources"
]
```

# To get the objects within nodes
You can add the specific key you want to select

`cat target/manifest.json | jq '.nodes | keys'`

# To see the contents of the first node
You can get the first element using to_entries and selecting the first item on the list

`cat target/manifest.json | jq '.nodes | to_entries | .[0].value'`

and you can see the top level keys of that item by calling the keys function

`cat target/manifest.json | jq '.nodes | to_entries | .[0].value | keys'`

This should output something like:
```
[
  "access",
  "alias",
  "build_path",
  "checksum",
  "columns",
  "compiled_path",
  "config",
  "constraints",
  "contract",
  "created_at",
  "database",
  "deferred",
  "depends_on",
  "deprecation_date",
  "description",
  "docs",
  "fqn",
  "group",
  "language",
  "latest_version",
  "meta",
  "metrics",
  "name",
  "original_file_path",
  "package_name",
  "patch_path",
  "path",
  "raw_code",
  "refs",
  "relation_name",
  "resource_type",
  "schema",
  "sources",
  "tags",
  "unique_id",
  "unrendered_config",
  "version"
]
```

## To find the nodes with public access
We can use the select function

`cat target/manifest.json | jq '.nodes | to_entries | .[].value | select(.access == "public")'`

## To get the names of the public models
We can just select the name key

`cat target/manifest.json | jq '.nodes | to_entries | .[].value | select(.access == "public") | .name'`

