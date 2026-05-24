#!/usr/bin/env bash
# BalanceAI end-to-end demo: scale -> even split -> kill -> auto recovery -> hot swap.
# Assumes Nasiko is already running on localhost:9100 (Kong) and the router
# is on localhost:8081 with the balancer wired in.

set -uo pipefail

NASIKO_DIR="${NASIKO_DIR:-$HOME/Desktop/Plans/Hack-31/nasiko}"
COMPOSE="docker compose -f docker-compose.local.yml --env-file .nasiko-local.env"
BALANCER="http://localhost:8081/balancer"
ROUTER="http://localhost:9100/router"

step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

cd "$NASIKO_DIR" || { echo "set NASIKO_DIR=path/to/nasiko" >&2; exit 1; }

step "1. Scale a2a-translator to 3 replicas"
$COMPOSE up -d --scale a2a-translator=3
sleep 8
docker ps --format '{{.Names}}' | grep a2a-translator || true

step "2. Even split — fire 300 requests"
for i in $(seq 1 300); do
  curl -s "$ROUTER/route?query=translate hello to french" >/dev/null &
done; wait

step "Per-replica request count (look for ~100 each)"
for r in 1 2 3; do
  c=$(docker logs "a2a-translator-$r" 2>&1 | grep -c "POST /invoke" || true)
  echo "  a2a-translator-$r: $c"
done

step "Pool snapshot + fairness Gini"
curl -s "$BALANCER/pools" | python3 -m json.tool

step "3. Kill replica 2"
docker stop a2a-translator-2
sleep 6  # let active probe fail twice + circuit open

step "Pool after eject (replica-2 should be missing or non-serving)"
curl -s "$BALANCER/pool/translator" | python3 -m json.tool

step "Fire 60 requests; only 1 and 3 should serve them"
for i in $(seq 1 60); do
  curl -s "$ROUTER/route?query=translate hello" >/dev/null
done

step "4. Restart replica 2 — watch slow-start ramp"
docker start a2a-translator-2
for _ in $(seq 1 15); do
  curl -s "$ROUTER/route?query=translate hello" >/dev/null
  curl -s "$BALANCER/pool/translator" \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); [print(f"  {r[\"container_name\"]}: status={r[\"status\"]} inflight={r[\"inflight\"]}") for r in d["replicas"]]'
  echo "---"
  sleep 2
done

step "5. Hot-swap strategy to P2C"
curl -s -X PUT "$BALANCER/strategy/translator" \
  -H 'Content-Type: application/json' \
  -d '{"strategy": "p2c"}' | python3 -m json.tool

step "Fire 300 more, still ~even"
for i in $(seq 1 300); do
  curl -s "$ROUTER/route?query=translate hello" >/dev/null &
done; wait
curl -s "$BALANCER/pools" | python3 -m json.tool

step "DONE — fairness should be < 0.1 throughout"
