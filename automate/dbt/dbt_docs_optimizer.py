#!/usr/bin/env python
import os
import json

DBT_HOME = os.environ["DATACOVES__DBT_HOME"]

if DBT_HOME == '':
    print("DBT_HOME is not defined. Please set it as an Environment Variable")
else:

    search_str = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'

    original_index_file = f"{DBT_HOME}/target/index.html"
    backup_original_index_file = f"{DBT_HOME}/target/index_original.html"
    with open(original_index_file, "r") as f:
        content_index = f.read()
        os.rename(original_index_file, backup_original_index_file)


    with open(f"{DBT_HOME}/target/manifest.json", "r") as f:
        json_manifest = json.loads(f.read())

    with open(f"{DBT_HOME}/target/catalog.json", "r") as f:
        json_catalog = json.loads(f.read())

    with open(f"{DBT_HOME}/target/index.html", "w") as f:
        new_str = (
            "o=[{label: 'manifest', data: "
            + json.dumps(json_manifest)
            + "},{label: 'catalog', data: "
            + json.dumps(json_catalog)
            + "}]"
        )
        new_content = content_index.replace(search_str, new_str)
        f.write(new_content)
