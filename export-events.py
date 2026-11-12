#!/usr/bin/env python3
"""Export the CDC events captured by Debezium Server from Redis to a JSON file.

Debezium Server 2.5 has no `file` sink, so this exercise uses the Redis sink and
reproduces the file output here: every change event is dumped, one JSON document
per line (JSON Lines), to cdc-output/cdc_events.json.

Usage:
    python export-events.py [--stream tutorial.public.products]
"""

import argparse
import json
import pathlib
import subprocess
import sys

DEFAULT_STREAM = "tutorial.public.products"
OUTPUT = pathlib.Path(__file__).parent / "cdc-output" / "cdc_events.json"
OP_NAMES = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "READ (snapshot)"}


def read_stream(container, stream):
    """Return the raw XRANGE reply of the Redis stream as parsed JSON."""
    result = subprocess.run(
        ["docker", "exec", container, "redis-cli", "-3", "--json",
         "XRANGE", stream, "-", "+"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"redis-cli failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def entry_fields(fields):
    """Normalise an XRANGE entry into (field_name, field_value) pairs.

    The Debezium Redis sink stores each event as a single field whose name is
    the serialized event key and whose value is the serialized event payload.
    Depending on the redis-cli version the reply is either a flat list or a map.
    """
    if isinstance(fields, dict):
        return list(fields.items())
    return list(zip(fields[0::2], fields[1::2]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default="redis_sink")
    parser.add_argument("--stream", default=DEFAULT_STREAM)
    args = parser.parse_args()

    entries = read_stream(args.container, args.stream)
    if not entries:
        sys.exit(f"stream '{args.stream}' is empty - no CDC events captured yet")

    events = []
    for entry_id, fields in entries:
        for key, value in entry_fields(fields):
            if not value:
                # Tombstone record emitted after a DELETE.
                continue
            events.append({
                "redis_entry_id": entry_id,
                "key": json.loads(key),
                "value": json.loads(value),
            })

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event["value"], ensure_ascii=False) + "\n")

    print(f"{len(events)} events written to {OUTPUT}\n")
    print(f"{'op':<4} {'operation':<16} {'before':<40} after")
    for event in events:
        payload = event["value"]["payload"]
        op = payload.get("op", "?")
        print(f"{op:<4} {OP_NAMES.get(op, 'UNKNOWN'):<16} "
              f"{str(payload.get('before')):<40.38} {payload.get('after')}")


if __name__ == "__main__":
    main()
