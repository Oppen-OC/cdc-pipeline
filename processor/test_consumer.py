"""Tests unitarios de la transformación del procesador.

No necesitan Redis ni PostgreSQL: importar consumer.py no abre conexiones.
Asumen VAT_RATE por defecto (0.21).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import consumer

SAMPLE_EVENTS = Path(__file__).resolve().parent.parent / "docs" / "sample-events.jsonl"


# --- categorize ---------------------------------------------------------

@pytest.mark.parametrize("name, expected", [
    ("Wireless Mouse", "Peripherals"),
    ("Gaming Monitor", "Displays"),
    ("NVMe SSD 1TB", "Storage"),
    ("Charger 65W", "Accessories"),
    ("Desk Lamp", "Uncategorized"),
])
def test_categorize_one_word_per_category(name, expected):
    assert consumer.categorize(name) == expected


def test_categorize_is_case_insensitive():
    assert consumer.categorize("USB-C HUB") == "Peripherals"


def test_categorize_first_rule_wins():
    # "monitor" (Displays) y "cable" (Accessories) coinciden; gana la primera regla
    assert consumer.categorize("Monitor cable") == "Displays"


@pytest.mark.parametrize("name", [None, ""])
def test_categorize_missing_name_is_uncategorized(name):
    assert consumer.categorize(name) == "Uncategorized"


# --- to_datetime --------------------------------------------------------

def test_to_datetime_none():
    assert consumer.to_datetime(None) is None


def test_to_datetime_string_is_returned_as_is():
    assert consumer.to_datetime("2026-01-01T00:00:00Z") == "2026-01-01T00:00:00Z"


def test_to_datetime_microseconds_to_utc():
    assert consumer.to_datetime(1786981502839123) == datetime(
        2026, 8, 17, 15, 45, 2, 839123, tzinfo=timezone.utc
    )


# --- transform ----------------------------------------------------------

def make_payload(op, **blocks):
    return {"op": op, "source": {"lsn": 123}, **blocks}


ROW = {"id": 1, "name": "Wireless Mouse", "description": "d",
       "price": 25.99, "created_at": 1786981502839123}


def test_transform_create():
    record = consumer.transform(make_payload("c", after=ROW))
    assert record["price_with_vat"] == 31.45
    assert record["is_deleted"] is False
    assert record["source_lsn"] == 123
    assert record["last_op"] == "c"
    assert record["created_at"] == consumer.to_datetime(ROW["created_at"])


def test_transform_delete_uses_before_and_marks_deleted():
    record = consumer.transform(make_payload("d", before=ROW))
    assert record["is_deleted"] is True
    assert record["last_op"] == "d"


def test_transform_unsupported_op_returns_none():
    assert consumer.transform(make_payload("t", after=ROW)) is None


def test_transform_without_row_returns_none():
    assert consumer.transform(make_payload("c")) is None


def test_transform_price_string_becomes_float():
    record = consumer.transform(make_payload("c", after={**ROW, "price": "25.99"}))
    assert record["price"] == 25.99
    assert isinstance(record["price"], float)


# --- handle_entry -------------------------------------------------------

class FakeCursor:
    """Cursor mínimo: guarda los parámetros de cada execute y fija rowcount."""

    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, _sql, params):
        self.calls.append(params)


def raw_event(op, row_id):
    return json.dumps({"payload": make_payload(op, after={**ROW, "id": row_id})})


def test_handle_entry_skips_tombstone():
    cur = FakeCursor()
    assert consumer.handle_entry(cur, {"k": ""}) == 0
    assert cur.calls == []


def test_handle_entry_skips_json_without_payload():
    cur = FakeCursor()
    assert consumer.handle_entry(cur, {"k": json.dumps({"schema": {}})}) == 0
    assert cur.calls == []


def test_handle_entry_two_valid_fields():
    cur = FakeCursor(rowcount=1)
    fields = {"k1": raw_event("c", 1), "k2": raw_event("c", 2)}
    assert consumer.handle_entry(cur, fields) == 2
    assert [c["id"] for c in cur.calls] == [1, 2]


def test_handle_entry_malformed_json_raises_value_error():
    # El bucle principal depende de ello para hacer ack de mensajes rotos
    with pytest.raises(ValueError):
        consumer.handle_entry(FakeCursor(), {"k": "{not json"})


# --- eventos reales -----------------------------------------------------

def test_sample_events_transform_to_expected_ops():
    lines = SAMPLE_EVENTS.read_text(encoding="utf-8").splitlines()
    records = [consumer.transform(json.loads(line)["payload"]) for line in lines if line]
    assert [r["last_op"] for r in records] == ["c", "c", "u", "d"]
