#!/usr/bin/env python3
"""Transform the documented timed-statechart subset into UPPAAL and PlantUML."""

import argparse
import json
import re
from pathlib import Path
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent


def validate(model):
    """Reject unsupported statechart constructs before translating any semantics."""
    if model.get("schema_version") != 1:
        raise ValueError("Unsupported statechart version")
    for template in model["templates"]:
        names = [s["name"] for s in template["states"]]
        if len(names) != len(set(names)) or template["initial"] not in names:
            raise ValueError("Duplicate states or unknown initial state")
        if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n) for n in names):
            raise ValueError("Invalid state identifier")
        for transition in template["transitions"]:
            if set(transition) - {
                "source",
                "target",
                "guard",
                "sync",
                "effect",
                "requirement",
            }:
                raise ValueError("Unsupported transition construct")
            if transition["source"] not in names or transition["target"] not in names:
                raise ValueError("Transition endpoint is missing")
        for state in template["states"]:
            if set(state) - {"name", "invariant", "committed"}:
                raise ValueError("Unsupported state construct")


def transform(model, reserve_first=True):
    """Map states, invariants and labelled edges to UPPAAL with source traceability."""
    validate(model)
    root = ET.Element("nta")
    ET.SubElement(root, "declaration").text = model["declaration"].replace(
        "$SAFE", "true" if reserve_first else "false"
    )
    mapping, uml = [], ["@startuml", "hide empty description"]
    for item in model["templates"]:
        template = ET.SubElement(root, "template")
        ET.SubElement(template, "name").text = item["name"]
        if item.get("parameter"):
            ET.SubElement(template, "parameter").text = item["parameter"]
        ET.SubElement(template, "declaration").text = item.get("declaration", "")
        ids = {}
        uml.append(f'state "{item["name"]}" as {item["name"]} {{')
        for i, state in enumerate(item["states"]):
            ident = f'{item["name"]}_{state["name"]}'
            ids[state["name"]] = ident
            location = ET.SubElement(
                template, "location", id=ident, x=str(i * 180), y="0"
            )
            ET.SubElement(location, "name").text = state["name"]
            if state.get("invariant"):
                ET.SubElement(location, "label", kind="invariant").text = state[
                    "invariant"
                ]
            if state.get("committed"):
                ET.SubElement(location, "committed")
            mapping.append(
                {"source": f'{item["name"]}.states.{state["name"]}', "uppaal_id": ident}
            )
            uml.append(f"  state {ident}")
        ET.SubElement(template, "init", ref=ids[item["initial"]])
        uml.append(f'  [*] --> {ids[item["initial"]]}')
        for i, edge in enumerate(item["transitions"]):
            transition = ET.SubElement(template, "transition")
            ET.SubElement(transition, "source", ref=ids[edge["source"]])
            ET.SubElement(transition, "target", ref=ids[edge["target"]])
            for key, kind in [
                ("guard", "guard"),
                ("sync", "synchronisation"),
                ("effect", "assignment"),
            ]:
                if edge.get(key):
                    ET.SubElement(transition, "label", kind=kind).text = edge[key]
            label = " / ".join(
                edge.get(k, "") for k in ["guard", "sync", "effect"] if edge.get(k)
            )
            uml.append(f'  {ids[edge["source"]]} --> {ids[edge["target"]]} : {label}')
            mapping.append(
                {
                    "source": f'{item["name"]}.transitions.{i}',
                    "requirement": edge.get("requirement"),
                    "source_id": ids[edge["source"]],
                    "target_id": ids[edge["target"]],
                }
            )
        uml.append("}")
    ET.SubElement(root, "system").text = model["system"]
    queries = ET.SubElement(root, "queries")
    for query in model["queries"]:
        element = ET.SubElement(queries, "query")
        ET.SubElement(element, "formula").text = query["formula"]
        ET.SubElement(element, "comment").text = (
            query["id"] + ": " + query["description"]
        )
    uml.append("@enduml")
    ET.indent(root)
    return ET.tostring(root, encoding="unicode"), "\n".join(uml) + "\n", mapping


def generate(output=HERE / "generated"):
    """Write both reservation variants, shared queries and the traceability map."""
    model = json.loads((HERE / "behaviour.json").read_text())
    output.mkdir(parents=True, exist_ok=True)
    for safe, name in [(False, "naive"), (True, "reserved")]:
        xml, uml, mapping = transform(model, safe)
        (output / f"{name}.xml").write_text(
            '<?xml version="1.0" encoding="utf-8"?>\n' + xml + "\n"
        )
        if safe:
            (output / "transfer.puml").write_text(uml)
            (output / "traceability.json").write_text(
                json.dumps(mapping, indent=2) + "\n"
            )
    (output / "queries.q").write_text(
        "\n".join(q["formula"] for q in model["queries"]) + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=HERE / "generated")
    generate(parser.parse_args().output)
