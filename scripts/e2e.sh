#!/usr/bin/env bash
# Prueba end-to-end: una fila escrita en PostgreSQL debe llegar a la API pasando
# por Debezium, Redis y el procesador. Parte de un entorno limpio y deja el stack
# levantado (si falla, para poder inspeccionar los logs); limpiar después con
# `docker compose down -v`. En CI lo hace el propio workflow.
set -euo pipefail

cd "$(dirname "$0")/.."

API=localhost:8000
PSQL=(docker exec -i postgres_source psql -U postgres -d inventory_db -v ON_ERROR_STOP=1)

trap 'rc=$?; [ "$rc" -eq 0 ] || echo "E2E FALLÓ (código $rc). Logs: docker compose logs; limpiar: docker compose down -v" >&2' EXIT

# Reintenta "$@" cada segundo hasta que tenga éxito o pasen $1 segundos.
esperar() {
  local timeout=$1; shift
  local i
  for ((i = 0; i < timeout; i++)); do
    "$@" && return 0
    sleep 1
  done
  return 1
}

slot_activo() { [ "$("${PSQL[@]}" -tAc "SELECT active FROM pg_replication_slots WHERE slot_name='debezium'" | tr -d '\r')" = "t" ]; }
precio_actualizado() { local body; body=$(curl -sf "$API/products/1") && grep -q '"price":22.99' <<<"$body"; }
# Eventos rechazados por la guarda LSN (applied=False) que constan en los logs.
rechazados() { docker logs cdc_processor 2>&1 | grep -c 'applied=False' || true; }

echo "==> 1/6 Levantando el stack"
docker compose up -d --build --wait

echo "==> 2/6 Esperando a que Debezium tenga el slot de replicación activo (máx. 60 s)"
esperar 60 slot_activo || { echo "El slot 'debezium' no se activó" >&2; exit 1; }

echo "==> 3/6 Ejecutando las mutaciones de demostración"
"${PSQL[@]}" < sql/demo-mutations.sql

echo "==> 4/6 Esperando a que la API refleje el UPDATE (price 22.99, máx. 60 s)"
esperar 60 precio_actualizado || { echo "/products/1 no llegó a price 22.99" >&2; exit 1; }

echo "==> 5/6 Comprobando soft delete, listado y estadísticas"
body=$(curl -sf "$API/products/2") || { echo "/products/2 no respondió 200" >&2; exit 1; }
grep -q '"is_deleted":true' <<<"$body" \
  || { echo "/products/2 debería estar marcado como is_deleted" >&2; exit 1; }
body=$(curl -sf "$API/products") || { echo "/products no respondió 200" >&2; exit 1; }
if grep -q '"id":2[,}]' <<<"$body"; then
  echo "/products no debería incluir el id 2 (borrado)" >&2; exit 1
fi
curl -sf -o /dev/null "$API/stats" || { echo "/stats no respondió 200" >&2; exit 1; }

echo "==> 6/6 Idempotencia: reentregando todo el stream, ingested_at no debe cambiar"
ingested() { local b; b=$(curl -sf "$API/products/1") && grep -o '"ingested_at":"[^"]*"' <<<"$b"; }
antes=$(ingested)
rechazados_antes=$(rechazados)
docker exec redis_sink redis-cli XGROUP SETID inventory.public.products analytics-processor 0
# Los 4 eventos de la demo (c, c, u, d) deben reentregarse y ser rechazados por la guarda LSN.
reentregados() { [ "$(( $(rechazados) - rechazados_antes ))" -ge 4 ]; }
esperar 60 reentregados || { echo "el procesador no reentregó los 4 eventos (applied=False)" >&2; exit 1; }
despues=$(ingested)
if [ "$antes" != "$despues" ]; then
  echo "ingested_at cambió tras la reentrega: $antes -> $despues" >&2; exit 1
fi

echo "E2E OK"
