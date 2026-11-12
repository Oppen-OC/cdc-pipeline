# cdc-pipeline

Los cambios de PostgreSQL se capturan del WAL con Debezium, se transportan por
Redis Streams, los enriquece un procesador Python, se materializan en un
esquema analítico y se sirven con FastAPI, en pocos segundos y sin tocar la
aplicación de origen.

![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![Debezium 2.5](https://img.shields.io/badge/Debezium-2.5-FF6B6B)
![Redis 7](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?logo=docker&logoColor=white)

## Arquitectura

```mermaid
flowchart LR
    subgraph source["Capa de origen"]
        PG[("PostgreSQL 15<br/>public.products")]
    end

    subgraph ingest["Ingesta — CDC basado en log"]
        DBZ["Debezium Server 2.5.4<br/>PostgresConnector"]
        RDS[("Redis 7<br/>stream inventory.public.products")]
    end

    subgraph process["Procesamiento — streaming"]
        PROC["processor<br/>XREADGROUP + UPSERT idempotente"]
    end

    subgraph storage["Almacenamiento — tabla de servicio"]
        ANA[("PostgreSQL 15<br/>analytics.products_enriched")]
    end

    subgraph serve["Exposición"]
        API["FastAPI + uvicorn<br/>:8000"]
    end

    CLIENT(["Cliente / curl"])

    PG -->|"replicación lógica<br/>slot, pgoutput"| DBZ
    DBZ -->|"XADD, eventos de cambio en JSON"| RDS
    RDS -->|"XREADGROUP, consumer group"| PROC
    PROC -->|"INSERT ... ON CONFLICT"| ANA
    ANA -->|"SELECT, pool de conexiones"| API
    API -->|"HTTP, JSON"| CLIENT
```

Desglose completo de componentes, flujo de datos y decisiones:
**[docs/architecture.md](./docs/architecture.md)**.

## Stack

| Capa | Tecnología |
| --- | --- |
| Ingesta | Debezium Server 2.5.4: CDC basado en log, sin clúster de Kafka |
| Transporte | Redis 7 streams con consumer groups |
| Procesamiento | Python 3.11, consumidor de streaming en tiempo real |
| Almacenamiento | PostgreSQL 15, esquema `analytics` |
| Exposición | FastAPI 0.109 + uvicorn, REST/JSON |
| Despliegue | Docker Compose, cinco servicios |

## Arranque rápido

Requiere Docker y Docker Compose. Nada más: Python se ejecuta dentro de los
contenedores.

```bash
docker compose up -d --build
docker compose ps          # cinco servicios, postgres y api healthy
```

Crear los esquemas y generar los primeros eventos de cambio:

```bash
docker exec -i postgres_source psql -U postgres -d inventory_db -v ON_ERROR_STOP=1 < sql/02-analytics-schema.sql
docker exec -i postgres_source psql -U postgres -d inventory_db -v ON_ERROR_STOP=1 < sql/01-products-mutations.sql
```

Ver cómo fluyen:

```bash
docker logs -f cdc_processor
```

```
connected to postgres at postgres:5432
processing stream 'inventory.public.products' as consumer 'processor-1'
op=c id=1 lsn=26595880 applied=True
op=c id=2 lsn=26596200 applied=True
op=u id=1 lsn=26596432 applied=True
op=d id=2 lsn=26596664 applied=True
```

Para parar y limpiar: `docker compose down -v`.

## API

Documentación interactiva, con los esquemas de respuesta, en
**http://localhost:8000/docs**.

| Método | Ruta | Descripción |
| --- | --- | --- |
| GET | `/health` | Estado del servicio con una comprobación real contra la base de datos; `503` si PostgreSQL no responde. |
| GET | `/products` | Estado actual de los productos. Parámetros: `limit` (1-500), `offset`, `include_deleted`, `category`. |
| GET | `/products/{id}` | Un producto concreto; `404` si no existe. |
| GET | `/stats` | Agregados: totales, activos/eliminados, estadísticas de precio y desglose por categoría. |

```bash
curl -s localhost:8000/health
curl -s localhost:8000/products
curl -s "localhost:8000/products?include_deleted=true&limit=10"
curl -s "localhost:8000/products?category=Displays"
curl -s localhost:8000/products/1
curl -s localhost:8000/stats
```

En PowerShell de Windows usa `curl.exe`: el `curl` a secas es un alias de
`Invoke-WebRequest` y no acepta esta sintaxis.

## Qué demuestra

### El pipeline es en vivo, no por lotes

Se escribe en la tabla de origen y se consulta la API; no se toca nada más:

```bash
docker exec postgres_source psql -U postgres -d inventory_db -c \
  "INSERT INTO products (name, description, price) VALUES ('4K Monitor', '27 inch IPS panel', 329.00);"
sleep 3
curl -s localhost:8000/products
```

```
1 Wireless Mouse  22.99   27.82  Peripherals
3 USB-C Hub       45.50   55.05  Peripherals
4 4K Monitor     329.00  398.09  Displays
```

La fila nueva llega con el IVA calculado y una categoría derivada de su
nombre, tras atravesar el WAL, Debezium, Redis y el procesador.

### Los borrados son lógicos (soft deletes)

```bash
docker exec postgres_source psql -U postgres -d inventory_db -c "DELETE FROM products WHERE id = 5;"
sleep 3
curl -s localhost:8000/products                        # desaparece
curl -s "localhost:8000/products?include_deleted=true" # sigue ahí, marcado
```

```
1 Wireless Mouse       active
2 Mechanical Keyboard  deleted
3 USB-C Hub            active
4 4K Monitor           active
5 4K Monitor           deleted
```

### La reentrega no puede corromper los datos

Se rebobina el consumer group y se deja que el procesador reprocese todo:

```bash
docker exec redis_sink redis-cli XGROUP SETID inventory.public.products analytics-processor 0
docker logs -f cdc_processor
```

```
op=c id=1 lsn=26595880 applied=False
op=c id=2 lsn=26596200 applied=False
op=u id=1 lsn=26596432 applied=False
op=d id=2 lsn=26596664 applied=False
op=c id=3 lsn=27005016 applied=False
```

La guarda por LSN rechaza todos los eventos y las marcas `ingested_at` no se
modifican. Entrega at-least-once, efecto exactly-once.

### No se pierde nada mientras el procesador está caído

```bash
docker compose stop processor
docker exec postgres_source psql -U postgres -d inventory_db -c \
  "UPDATE products SET price = 39.90 WHERE id = 3;"
docker exec redis_sink redis-cli XINFO GROUPS inventory.public.products   # el lag crece
docker compose start processor                                           # el lag vuelve a 0
```

## Decisiones de diseño

* **Redis en lugar de un sink `file`**: Debezium Server 2.5 no incluye sink de
  fichero; Redis evita un clúster de Kafka y añade confirmación (ack) por
  mensaje con consumer groups.
* **Idempotencia por LSN**: cada fila guarda la posición del WAL que la produjo
  y el UPSERT ignora cualquier evento que no sea estrictamente más reciente.
* **Commit antes del acknowledge**: un fallo provoca una reentrega, no una
  pérdida.
* **Soft deletes**: la capa analítica conserva el historial.
* **`table.include.list`**: evita que las escrituras del propio procesador se
  capturen y reprocesen en bucle.
* **Endpoints síncronos con pool de conexiones**, porque psycopg2 es bloqueante.

Cada una se explica en
[docs/architecture.md](./docs/architecture.md#decisiones-de-diseño).

## Limitaciones conocidas

* Origen y servicio comparten la misma instancia de PostgreSQL; un despliegue
  real separaría OLTP de analítica.
* El procesador es un único consumidor; escalarlo exigiría orden por clave.
* El slot de replicación nunca se elimina: si Debezium se detiene mucho tiempo,
  el WAL crece hasta que se vuelva a consumir.
* La API no tiene autenticación; un despliegue público la necesitaría.

Detalle en [docs/architecture.md](./docs/architecture.md#limitaciones-conocidas).

## Estructura del repo

```
.
├── docker-compose.yml                    # todo el stack
├── debezium-conf/application.properties  # configuración de origen y sink del CDC
├── sql/
│   ├── 01-products-mutations.sql         # tabla de origen y mutaciones de ejemplo
│   └── 02-analytics-schema.sql           # tabla de servicio (idempotente)
├── processor/
│   ├── consumer.py                       # stream de Redis -> tabla analítica
│   ├── Dockerfile
│   └── requirements.txt
├── api/
│   ├── main.py                           # API REST sobre la tabla de servicio
│   ├── Dockerfile
│   └── requirements.txt
├── docs/
│   ├── architecture.md                   # diagrama de arquitectura y decisiones
│   ├── cdc-events.md                     # análisis de los eventos CDC capturados
│   └── sample-events.jsonl               # eventos en bruto de una ejecución real
└── export-events.py                      # vuelca los eventos CDC en bruto a JSON
```

## Eventos CDC en bruto

El análisis de los eventos que emite Debezium (`op`, `before`, `after`) está en
[docs/cdc-events.md](./docs/cdc-events.md). Para inspeccionarlos en una
ejecución propia:

```bash
python export-events.py
```

Vuelca el stream de Redis a `cdc-output/cdc_events.json`, un evento de cambio
por línea.

---

## Licencia

MIT, ver [LICENSE](LICENSE).

Proyecto desarrollado a partir de una práctica del Máster en IA y Big Data (UPV).
