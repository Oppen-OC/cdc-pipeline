# Eventos CDC capturados

Lo que Debezium emite por cada cambio en `products`, tomado de una ejecución
real. Los eventos en bruto están en [`sample-events.jsonl`](./sample-events.jsonl),
un documento JSON por línea; `python export-events.py` los regenera desde el
stream de Redis en `cdc-output/cdc_events.json`.

Dos detalles de configuración que conviene conocer:

* `ALTER TABLE products REPLICA IDENTITY FULL`: con la identidad de réplica por
  defecto, PostgreSQL solo escribe la clave primaria en el WAL para UPDATE y
  DELETE, así que el bloque `before` contendría únicamente `id`. `FULL` registra
  la fila anterior completa.
* `debezium.source.decimal.handling.mode=double`: de lo contrario `price` se
  emite como un `Decimal` de Kafka Connect, es decir, bytes en base64 (`"Cic="`
  en lugar de `25.99`).

## Eventos capturados

`XLEN inventory.public.products` devuelve **4** eventos, uno por mutación:

| `op` | Operación | `before` | `after` |
| --- | --- | --- | --- |
| `c` | CREATE | `null` | `id=1, Wireless Mouse, price=25.99` |
| `c` | CREATE | `null` | `id=2, Mechanical Keyboard, price=89.99` |
| `u` | UPDATE | `id=1, price=25.99` | `id=1, price=22.99` |
| `d` | DELETE | `id=2, price=89.99` | `null` |

El evento UPDATE, tal como está guardado en [`sample-events.jsonl`](./sample-events.jsonl):

```json
{
  "before": { "id": 1, "name": "Wireless Mouse", "price": 25.99, "created_at": 1786981502839123 },
  "after":  { "id": 1, "name": "Wireless Mouse", "price": 22.99, "created_at": 1786981502839123 },
  "source": {
    "version": "2.5.4.Final", "connector": "postgresql", "name": "inventory",
    "snapshot": "false", "db": "inventory_db", "schema": "public", "table": "products",
    "txId": 732, "lsn": 26596432
  },
  "op": "u",
  "ts_ms": 1786981503208
}
```

Observaciones:

* Cada evento lleva un bloque `schema` y un bloque `payload`; `payload`
  contiene `before` / `after` / `source` / `op` / `ts_ms`.
* `before` es `null` en los INSERT y `after` es `null` en los DELETE: el estado
  que no existía en ese instante.
* `source.lsn` y `source.txId` vinculan el evento con una posición del WAL y una
  transacción de PostgreSQL. Ese mismo LSN es el que acaba en `offsets.dat`, el
  almacén de offsets de Debezium (último LSN confirmado), y con él Debezium
  reanuda tras un reinicio sin reproducir eventos. El fichero vive en el
  volumen con nombre `debezium-data` (no en `cdc-output/`) y lo borra
  `docker compose down -v`, de modo que tras un reinicio completo Debezium
  vuelve a empezar desde cero.
* `ts_ms` de primer nivel es el momento en que Debezium procesó el evento,
  mientras que `source.ts_ms` es el momento en que el cambio se confirmó en la
  base de datos. La diferencia entre ambos es la latencia del CDC (~360 ms
  aquí, dominada por el intervalo de sondeo del conector).
* No aparecen eventos `r` (lectura de snapshot): la tabla se creó después de
  arrancar el conector, así que todo llegó por la fase de streaming.
* Está activado `tombstones.on.delete=false`; de lo contrario cada DELETE iría
  seguido de un segundo registro con payload vacío, que el sink de Redis
  guardaría como una entrada de ruido.

---
[⬅ Volver al README del proyecto](../README.md)
