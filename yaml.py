"""Minimal YAML subset parser for environments without PyYAML."""
from __future__ import annotations

from typing import Any, Union


def safe_load(stream: Union[str, bytes]) -> Any:
    if hasattr(stream, "read"):
        text = stream.read()
    else:
        text = stream

    if isinstance(text, bytes):
        text = text.decode("utf-8")

    data = {}
    section_name = None
    section_container: dict[str, Any] | None = None
    current_list = None

    for raw_line in str(text).splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if not raw_line.startswith(" "):
            # Top-level mapping key
            section_name = stripped.rstrip(":")
            section_container = {}
            data[section_name] = section_container
            current_list = None
            continue

        if section_container is None:
            continue

        if raw_line.startswith("  ") and ":" in stripped:
            key, rest = stripped.split(":", 1)
            key = key.strip().strip("'\"")
            rest = rest.strip()

            if rest:
                if rest.startswith("[") and rest.endswith("]"):
                    items = [item.strip().strip("'\"") for item in rest[1:-1].split(",") if item.strip()]
                    section_container[key] = items
                else:
                    section_container[key] = rest.strip("'\"")
                current_list = None
            else:
                current_list = []
                section_container[key] = current_list
            continue

        if raw_line.startswith("    -") and current_list is not None:
            item = stripped.lstrip("-").strip().strip("'\"")
            current_list.append(item)

    return data
