"""dlt destinations that write each table as one readable document: JSON or XML."""
import gzip
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import dlt


def _rows(file_path: str) -> list[dict]:
    rows = []
    opener = gzip.open if file_path.endswith(".gz") else open  # dlt compresses load files by default
    with opener(file_path, "rt", encoding="utf-8") as f:
        lines = f.read().splitlines()
    for line in lines:
        if line.strip():
            item = json.loads(line)
            rows += item if isinstance(item, list) else [item]
    return [{k: v for k, v in row.items() if not k.startswith("_dlt")} for row in rows]


def _sink(name: str, path: str, write):
    out = Path(path)

    # batch_size=0: dlt hands over each load file, so a table is written in one go.
    # ponytail: one file per table per load job; a table split over several job files keeps only the last one.
    @dlt.destination(name=name, batch_size=0, loader_file_format="typed-jsonl", max_table_nesting=0)
    def sink(file_path, table) -> None:
        out.mkdir(parents=True, exist_ok=True)
        write(out / table["name"], table["name"], _rows(file_path))

    return sink


def json_file(path: str = "output/json"):
    def write(target: Path, table: str, rows: list[dict]) -> None:
        target.with_suffix(".json").write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return _sink("json_file", path, write)


def _element(tag: str, value) -> ET.Element:
    element = ET.Element(tag if tag[:1].isalpha() or tag[:1] == "_" else f"_{tag}")
    if isinstance(value, dict):
        element.extend(_element(k, v) for k, v in value.items())
    elif isinstance(value, list):
        element.extend(_element("item", v) for v in value)
    elif value is not None:
        element.text = str(value)
    return element


def xml_file(path: str = "output/xml"):
    def write(target: Path, table: str, rows: list[dict]) -> None:
        root = ET.Element(table)
        root.extend(_element("record", row) for row in rows)
        ET.indent(root)
        ET.ElementTree(root).write(target.with_suffix(".xml"), encoding="utf-8", xml_declaration=True)

    return _sink("xml_file", path, write)
