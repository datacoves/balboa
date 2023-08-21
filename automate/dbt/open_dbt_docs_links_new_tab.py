import os
from pathlib import Path

DBT_HOME = os.environ.get("DBT_HOME", "/config/workspace/transform")

dbt_docs_index_path = Path(f"{DBT_HOME}/target/index.html")

if not dbt_docs_index_path.exists():
    raise Exception("DBT docs not generated")

with open(dbt_docs_index_path, "r") as f:
    html_content = f.read()

new_tab_tag = "<base target='_blank'>"

head_start = html_content.find("<head>") + len("<head>")
head_end = html_content.find("</head>")

if new_tab_tag not in html_content[head_start:head_end]:
    modified_content = html_content[:head_end] + new_tab_tag + html_content[head_end:]

    # Write the modified content back to the HTML file
    with open(dbt_docs_index_path, "w") as html_file:
        html_file.write(modified_content)
else:
    print("New-tab tag already exists in the dbt-docs page.")
