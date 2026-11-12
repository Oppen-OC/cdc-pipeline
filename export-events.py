#!/usr/bin/env python3
"""Exporta a un fichero JSON los eventos CDC que Debezium Server deja en Redis.

Debezium Server 2.5 no incluye un sink `file`, así que el pipeline usa el sink
de Redis y este script reproduce la salida a fichero: cada evento de cambio se
vuelca, un documento JSON por línea (JSON Lines), en cdc-output/cdc_events.json.

Uso:
    python export-events.py [--stream inventory.public.products]
"""

import argparse
import json
import pathlib
import subprocess
import sys

DEFAULT_STREAM = "inventory.public.products"
OUTPUT = pathlib.Path(__file__).parent / "cdc-output" / "cdc_events.json"
OP_NAMES = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "READ (snapshot)"}


def read_stream(container, stream):
    """Devuelve la respuesta XRANGE del stream de Redis ya parseada como JSON."""
    result = subprocess.run(
        ["docker", "exec", container, "redis-cli", "-3", "--json",
         "XRANGE", stream, "-", "+"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"redis-cli failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def entry_fields(fields):
    """Normaliza una entrada de XRANGE en pares (nombre_campo, valor_campo).

    El sink de Redis de Debezium guarda cada evento como un único campo cuyo
    nombre es la clave del evento serializada y cuyo valor es el payload del
    evento serializado. Según la versión de redis-cli, la respuesta es una lista
    plana o un mapa.
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
                # Registro tombstone emitido tras un DELETE.
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
