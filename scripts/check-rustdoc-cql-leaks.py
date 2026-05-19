#!/usr/bin/env python3
"""
Checks that the public API of the scylla crate does not leak any types
from the scylla_cql crate. Only scylla_cql_core types are acceptable
in the public API.

Usage: python3 check-rustdoc-cql-leaks.py <path-to-rustdoc-json>

Generate the JSON with:
  RUSTDOCFLAGS="-Zunstable-options" cargo +nightly rustdoc -p scylla -- --output-format json
"""

import json
import sys
from typing import Any


def find_path_refs(obj: Any, target_ids: set[int]) -> set[int]:
    """Recursively find all path IDs from target_ids referenced in a JSON object."""
    found: set[int] = set()
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], int) and obj["id"] in target_ids:
            found.add(obj["id"])
        for v in obj.values():
            found |= find_path_refs(v, target_ids)
    elif isinstance(obj, list):
        for v in obj:
            found |= find_path_refs(v, target_ids)
    return found


def format_location(span: dict[str, Any] | None) -> str:
    if span is None:
        return "unknown location"
    return f"{span['filename']}:{span['begin'][0]}"


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <rustdoc-json-path>", file=sys.stderr)
        sys.exit(2)

    with open(sys.argv[1]) as f:
        data: dict[str, Any] = json.load(f)

    # Find the crate_id for scylla_cql in external_crates.
    scylla_cql_id: int | None = None
    for crate_id, crate_info in data["external_crates"].items():
        if crate_info["name"] == "scylla_cql":
            scylla_cql_id = int(crate_id)
            break

    if scylla_cql_id is None:
        print("OK: scylla_cql not found in external_crates (nothing leaks).")
        sys.exit(0)

    # Collect all path IDs belonging to scylla_cql.
    scylla_cql_paths: dict[int, dict[str, Any]] = {}
    for path_id, path_info in data["paths"].items():
        if path_info["crate_id"] == scylla_cql_id:
            scylla_cql_paths[int(path_id)] = path_info

    if not scylla_cql_paths:
        print("OK: No scylla_cql paths found.")
        sys.exit(0)

    target_ids: set[int] = set(scylla_cql_paths.keys())

    # Walk the index to find which scylla_cql types are actually referenced
    # from public API items (function signatures, struct fields, re-exports, etc.).
    # The `paths` map is just a lookup table; many entries are never referenced.
    refs_by_target: dict[int, list[dict[str, str]]] = {}
    for item_id, item_info in data["index"].items():
        found_ids = find_path_refs(item_info.get("inner", {}), target_ids)
        if not found_ids:
            continue

        loc = format_location(item_info.get("span"))
        name: str = item_info.get("name") or "(unnamed)"
        inner: dict[str, Any] = item_info.get("inner", {})
        kind: str = next(iter(inner), "?")

        for fid in found_ids:
            refs_by_target.setdefault(fid, []).append(
                {"name": name, "location": loc, "kind": kind}
            )

    if not refs_by_target:
        print("OK: No public API items reference scylla_cql types.")
        sys.exit(0)

    # Print results grouped by leaked type, with the locations that cause the leak.
    print(
        f"ERROR: Found {len(refs_by_target)} scylla_cql type(s) leaking "
        f"into the scylla public API.\n"
        f"These types need to be migrated to scylla_cql_core.\n"
    )
    for pid in sorted(
        refs_by_target, key=lambda x: "::".join(scylla_cql_paths[x]["path"])
    ):
        p = scylla_cql_paths[pid]
        path_str = "::".join(p["path"])
        print(f"  {p['kind']}: {path_str}")
        for ref in refs_by_target[pid]:
            print(f"    <- {ref['kind']} \"{ref['name']}\" at {ref['location']}")
        print()

    sys.exit(1)


if __name__ == "__main__":
    main()
