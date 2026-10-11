---
name: verify-wirejournal
description: Drive the wirejournal demo, proxy, and crash-recovery bench the way a user does. Use when proving resume, replay, or at-most-once mail, or before claiming the live demo works.
---

# Verify wirejournal

Primary surface: the two-lane web demo at `/`. Secondary surfaces: the `wirejournal` CLI (`proxy`, `agent`, `bench`) and `/status` JSON.

## Launch

Build once from the repo root, then start a disposable demo. Do not reuse a demo you did not start.

```bash
go build -o /tmp/wj-verify/wirejournal ./cmd/wirejournal
/tmp/wj-verify/wirejournal demo \
  -bin /tmp/wj-verify/wirejournal \
  -listen 127.0.0.1:18080 \
  -steps 8 \
  -delay 80ms
```

Ready when `curl -fsS http://127.0.0.1:18080/healthz` prints `ok`.

Preferred one-shot (build, doctor, crash-after-email, write evidence, kill only that pid):

```bash
scripts/verify-demo.sh
```

Override with `WJ_VERIFY_DIR`, `WJ_VERIFY_LISTEN`, `WJ_VERIFY_STEPS`.

Teardown: `kill -9` the pid `verify-demo.sh` printed, or the pid you started. Never `pkill wirejournal`. Evidence stays under `$WJ_VERIFY_DIR/evidence` (default `/tmp/wj-verify-$$/evidence`).

## Doctor

Read-only. Fail the instance if any check misses.

1. `curl -fsS http://$LISTEN/healthz` equals `ok`.
2. `curl -fsS http://$LISTEN/` contains `Crash (kill -9)` and `font-synthesis: none`.
3. `ps -p $PID -o args=` contains `wirejournal demo` and the listen address you chose.
4. `GET /status` decodes as JSON with `wirejournal` and `baseline` objects.

If the page is stale or the EventSource is dead, reload. If `/healthz` is another process, stop. Do not drive it.

## Drive

Prefer HTTP handles. The crash button is `#crash`. Reset is `#reset`. The live view is `GET /status` and `GET /events` (SSE). Do not poke `run.Store` or the mock servers directly when proving the demo. Those counters are what `/status` already reads.

```bash
curl -fsS http://127.0.0.1:18080/status
curl -fsS -X POST http://127.0.0.1:18080/crash
curl -fsS -X POST http://127.0.0.1:18080/reset
```

CLI replay (no demo):

```bash
go test -count=1 ./internal/agent -run TestAgentThroughProxyReplaysWithoutRebilling
go test -count=1 ./internal/bench -run TestCrashRecoveryVsBaseline
```

## Evidence

Proof is the user path plus the mock ledgers.

- Demo: `before.json` and `after.json` from `/status`, plus a screenshot of `/` if a browser is available. After a crash past `send_email`, wirejournal `deliveries` must be `1` and `tokens_billed` must stay at one ledger. Baseline `deliveries` must be `2` and `tokens_billed` must be higher.
- Do not treat the proxy `/r/{run}/status` tokens as ground truth for a published number. The demo `/status` tokens come from `mockllm.Stats`.
- Cleanup must not delete `$WJ_VERIFY_DIR/evidence`.

## Cleanup

Kill only the pid this run started. Leave `/tmp/wj-verify-*/evidence` and any files under `/opt/cursor/artifacts`.

## Helpers

`scripts/verify-demo.sh` is executable. It builds, doctors, waits for both inboxes, POSTs `/crash`, writes `evidence/before.json` and `evidence/after.json`, asserts the counts, then kills its own demo.

## Feature map

See [features/README.md](features/README.md). Drive from that map. A proof that only hits `/healthz` is incomplete.

For map upkeep later, use `/maintain-verification-skill`.
