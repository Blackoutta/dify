#!/usr/bin/env bash
set -euo pipefail

API_URL=${API_URL:-http://localhost:5001/v1/workflows/run}
API_TOKEN=${API_TOKEN:-app-5pbHbXIMPKiI6uyVZXKhLmyy}
DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-1000}
TIMEOUT=${TIMEOUT:-120}
COMPOSE_PROJECT=${COMPOSE_PROJECT:-dify-middlewares-dev}
NETWORK=${NETWORK:-${COMPOSE_PROJECT}_default}
OUT=${OUT:-/tmp/dify-load-$(date +%Y%m%d-%H%M%S)}

mkdir -p "$OUT"
echo "$OUT" > /tmp/dify-last-load-dir

queue_json() {
  local q=$1 file=$2
  docker run --rm --network "$NETWORK" curlimages/curl:8.11.1 -fsS \
    -H 'Origin: http://localhost' -u admin:admin \
    "http://activemq:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=$q/QueueSize,EnqueueCount,DequeueCount,ConsumerCount" > "$file" || true
}

sample_docker() {
  local end=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$end" ]; do
    printf '\n=== %s ===\n' "$(date -u +%FT%TZ)" >> "$OUT/docker-stats-samples.txt"
    docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}' \
      "$COMPOSE_PROJECT-db_postgres-1" "$COMPOSE_PROJECT-activemq-1" \
      "$COMPOSE_PROJECT-dify_log_consumer-1" "$COMPOSE_PROJECT-dify_log_consumer-2" "$COMPOSE_PROJECT-dify_log_consumer-3" \
      >> "$OUT/docker-stats-samples.txt" 2>&1 || true
    sleep 2
  done
}

sample_pg() {
  local end=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$end" ]; do
    printf '\n=== %s ===\n' "$(date -u +%FT%TZ)" >> "$OUT/pg-samples.txt"
    docker exec "$COMPOSE_PROJECT-db_postgres-1" psql -U postgres -d dify -Atc \
      "select count(*) total, count(*) filter (where state='active') active, count(*) filter (where state='idle') idle, count(*) filter (where state='idle in transaction') idle_in_tx from pg_stat_activity where datname='dify';" \
      >> "$OUT/pg-samples.txt" 2>&1 || true
    sleep 2
  done
}

queue_json dify.workflow.node-executions "$OUT/node-before.json"
queue_json dify.workflow.app-logs "$OUT/app-before.json"

sample_docker & stats_pid=$!
sample_pg & pg_pid=$!

hey -z "$DURATION" -c "$CONCURRENCY" -t "$TIMEOUT" -m POST \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"inputs": {}, "response_mode": "blocking", "user": "abc-123"}' \
  "$API_URL" > "$OUT/hey.txt" 2>&1

sleep 10
kill "$stats_pid" "$pg_pid" 2>/dev/null || true
wait "$stats_pid" "$pg_pid" 2>/dev/null || true

queue_json dify.workflow.node-executions "$OUT/node-after.json"
queue_json dify.workflow.app-logs "$OUT/app-after.json"
queue_json dify.workflow.node-executions.dlq "$OUT/node-dlq-after.json"
queue_json dify.workflow.app-logs.dlq "$OUT/app-dlq-after.json"

for n in 1 2 3; do
  docker logs --since 2m "$COMPOSE_PROJECT-dify_log_consumer-$n" > "$OUT/consumer-$n.log" 2>&1 || true
done

python - <<'PY' "$OUT"
import json, pathlib, re, sys
p = pathlib.Path(sys.argv[1])
print('OUT=' + str(p))
text = (p / 'hey.txt').read_text(errors='ignore')
for line in text.splitlines():
    if any(x in line for x in ['Total:', 'Slowest:', 'Fastest:', 'Average:', 'Requests/sec', 'Status code distribution', '[200]', '[500]', 'Error distribution']):
        print(line)
for name in ['node-after', 'app-after', 'node-dlq-after', 'app-dlq-after']:
    data = json.loads((p / f'{name}.json').read_text())
    print(name, data.get('value') or data.get('status'))
slow, ack, err = [], 0, 0
for f in p.glob('consumer-*.log'):
    for line in f.read_text(errors='ignore').splitlines():
        ack += 'ack failed' in line
        err += 'level=ERROR' in line
        m = re.search(r'duration=([0-9.]+)(µs|ms|s|m)', line)
        if 'gorm slow sql' in line and m:
            v, u = float(m.group(1)), m.group(2)
            slow.append(v / 1000 if u == 'µs' else v * 1000 if u == 's' else v * 60000 if u == 'm' else v)
slow.sort()
def pct(vals, q): return vals[min(len(vals)-1, int(len(vals)*q/100))] if vals else 0
print('consumer slow_sql_count', len(slow), 'ack_failed', ack, 'errors', err)
if slow:
    print('consumer slow_sql_ms p50/p95/p99/max', round(pct(slow,50),1), round(pct(slow,95),1), round(pct(slow,99),1), round(slow[-1],1))
cpus = []
for line in (p / 'docker-stats-samples.txt').read_text(errors='ignore').splitlines():
    if 'db_postgres' in line:
        try: cpus.append(float(line.split()[1].rstrip('%')))
        except Exception: pass
if cpus:
    cpus.sort(); print('db_cpu p50/p95/max', round(cpus[len(cpus)//2],2), round(cpus[min(len(cpus)-1,int(len(cpus)*.95))],2), round(cpus[-1],2))
rows = []
for line in (p / 'pg-samples.txt').read_text(errors='ignore').splitlines():
    if re.match(r'^\d+\|\d+\|\d+\|\d+$', line): rows.append(tuple(map(int, line.split('|'))))
if rows:
    print('pg max total/active/idle/idle_tx', max(r[0] for r in rows), max(r[1] for r in rows), max(r[2] for r in rows), max(r[3] for r in rows))
PY
