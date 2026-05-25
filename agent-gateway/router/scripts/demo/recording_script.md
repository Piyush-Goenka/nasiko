# BalanceAI 3-minute demo recording script

Six beats. Target 2:45-3:00 total. Re-record any beat that flakes.

Use a clean terminal + browser side-by-side. The "right side" referenced below
is `http://localhost:8081/balancer/pool/translator` polled via `watch` or the
Load Distribution UI tab.

## Beat 1 (0:00-0:25) — the problem

**Voiceover:** "Nasiko is the control plane for AI agents. But scale a
translator agent to 3 replicas and watch what happens."

**Terminal:**
```bash
docker compose -f docker-compose.local.yml --env-file .nasiko-local.env \
  up -d --scale a2a-translator=3 && sleep 5
docker ps --format '{{.Names}}' | grep a2a-translator
```

**Then fire 50 requests:**
```bash
for i in $(seq 1 50); do
  curl -s "http://localhost:9100/router/route?query=translate hello to french" >/dev/null
done

for r in 1 2 3; do
  echo "a2a-translator-$r: $(docker logs a2a-translator-$r 2>&1 | grep -c "POST /invoke")"
done
```

**Voiceover:** "Replica 1 does all the work. The other two are idle."

## Beat 2 (0:25-0:55) — what BalanceAI does

**Voiceover:** "BalanceAI sits between the router and the agent pool."

```bash
curl -s http://localhost:8081/balancer/pools | jq
```

Bars are equal across 3 replicas. Gini = 0.04. Strategy = round_robin.

```bash
for i in $(seq 1 300); do
  curl -s "http://localhost:9100/router/route?query=translate hello" >/dev/null &
done; wait

curl -s http://localhost:8081/balancer/pools | jq
```

Bars stay equal, Gini stays low.

## Beat 3 (0:55-1:30) — self-healing

**Voiceover:** "Kill a replica."

```bash
docker stop a2a-translator-2
```

Wait 6 seconds. Show:
```bash
curl -s http://localhost:8081/balancer/pool/translator | jq '.replicas[] | {n: .container_name, s: .status, i: .inflight}'
```

Pool donut: 2/3 healthy. Event log: `replica_ejected: a2a-translator-2 reason=active_health`.

```bash
for i in $(seq 1 60); do
  curl -s "http://localhost:9100/router/route?query=translate hello" >/dev/null
done
```

All hit replicas 1 and 3 only.

**Voiceover:** "Failover in five seconds, no manual intervention."

## Beat 4 (1:30-2:00) — recovery + slow-start

```bash
docker start a2a-translator-2
```

Show the dashboard: replica returns with `slow_start` tag.

```bash
for _ in $(seq 1 15); do
  curl -s "http://localhost:9100/router/route?query=translate hello" >/dev/null
  curl -s http://localhost:8081/balancer/pool/translator \
    | jq '.replicas[] | {n: .container_name, i: .inflight}'
  sleep 2
done
```

Replica 2 traffic ramps from ~0 to ~33% over 30s.

**Voiceover:** "Cold replica gets traffic gradually so it doesn't get hammered."

## Beat 5 (2:00-2:30) — hot strategy swap + LLM affinity

```bash
curl -X PUT http://localhost:8081/balancer/strategy/translator \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"chwbl"}'

for i in $(seq 1 50); do
  curl -s -H "X-Session-Id: conv-A" \
    "http://localhost:9100/router/route?query=translate hello" >/dev/null
done
```

All 50 hit the same replica (KV-cache affinity).

```bash
for i in $(seq 1 50); do
  curl -s -H "X-Session-Id: session-$i" \
    "http://localhost:9100/router/route?query=translate hello" >/dev/null
done
```

Now spread across all 3.

**Voiceover:** "AI-agent-specific routing. Same KV-cache pattern vLLM Router uses."

## Beat 6 (2:30-3:00) — close

Show PPT slide 7 with the repo link + Gini-before-after chart.

**Voiceover:** "BalanceAI turns Nasiko's broken horizontal scaling into a
production-grade primitive in ~700 lines of Python. Full test suite passes,
demo runs end-to-end in three minutes, ready as an upstream PR."

## Recording mechanics

- **asciinema** for the terminal-only cut: `asciinema rec demo.cast`
- **Loom or QuickTime/OBS** for the full voiceover version with the dashboard visible
- Upload .mp4 unlisted (YouTube / Loom). Paste link into PPT slide 7 ("Live Demo")
  and the module README
