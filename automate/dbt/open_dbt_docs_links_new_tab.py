#!/usr/bin/env python

# This script is used to enable linking to external resources by updating descriptions
# example:
#    description: Current Population by Country [Read more](https://www.google.com/)


import os
from pathlib import Path

from rich import console

console = console.Console()

DBT_HOME = os.environ.get("DBT_HOME", "/config/workspace/transform")

dbt_docs_index_path = Path(f"{DBT_HOME}/target/index.html")

if not dbt_docs_index_path.exists():
    console.print(
        "[red]:cross_mark:[/red] dbt docs not found, run [i]'dbt docs generate'[/i] first"
    )
else:
    with open(dbt_docs_index_path, "r") as f:
        html_content = f.read()

    new_tab_tag = "<base target='_blank'>"

    head_start = html_content.find("<head>") + len("<head>")
    head_end = html_content.find("</head>")

    if new_tab_tag not in html_content[head_start:head_end]:
        modified_content = (
            html_content[:head_end] + new_tab_tag + html_content[head_end:]
        )

        # Write the modified content back to the HTML file
        with open(dbt_docs_index_path, "w") as html_file:
            html_file.write(modified_content)
        console.print(
            "[green]:heavy_check_mark:[/green] dbt docs links will now open in a new tab"
        )
    else:
        console.print(
            "[red]:cross_mark:[/red] dbt docs links are already being opened in a new tab."
        )
