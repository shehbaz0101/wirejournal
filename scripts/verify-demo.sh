#!/usr/bin/env bash
# Launch the demo, crash both lanes after send_email, and prove
# wirejournal keeps one email while baseline sends a second.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="${WJ_VERIFY_DIR:-/tmp/wj-verify-$$}"
LISTEN="${WJ_VERIFY_LISTEN:-127.0.0.1:18080}"
STEPS="${WJ_VERIFY_STEPS:-8}"
mkdir -p "$WORKDIR/evidence"
BIN="$WORKDIR/wirejournal"

go build -o "$BIN" "$ROOT/cmd/wirejournal"
"$BIN" demo -bin "$BIN" -listen "$LISTEN" -steps "$STEPS" -delay 80ms \
  >"$WORKDIR/demo.log" 2>&1 &
PID=$!
cleanup() {
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

ok=0
for _ in $(seq 1 50); do
  if curl -fsS "http://$LISTEN/healthz" | grep -qx ok; then
    ok=1
    break
  fi
  sleep 0.1
done
if [ "$ok" -ne 1 ]; then
  echo "doctor: demo did not become healthy on $LISTEN" >&2
  cat "$WORKDIR/demo.log" >&2 || true
  exit 1
fi
echo "doctor: pid=$PID listen=$LISTEN healthz=ok"

python3 - "$LISTEN" "$WORKDIR/evidence" <<'PY'
import json, sys, time, urllib.request

base, evdir = sys.argv[1], sys.argv[2]

def get(path):
    with urllib.request.urlopen("http://" + base + path) as r:
        return r.read()

def post(path):
    req = urllib.request.Request("http://" + base + path, method="POST", data=b"")
    with urllib.request.urlopen(req) as r:
        r.read()

def status():
    return json.loads(get("/status"))

page = get("/").decode()
if "Crash (kill -9)" not in page:
    raise SystemExit("ui missing crash button")
if "font-synthesis: none" not in page:
    raise SystemExit("ui missing font-synthesis:none")

deadline = time.time() + 20
last = None
while time.time() < deadline:
    last = status()
    if last["wirejournal"]["deliveries"] >= 1 and last["baseline"]["deliveries"] >= 1:
        break
    time.sleep(0.05)
else:
    raise SystemExit("email never sent: " + json.dumps(last))
open(evdir + "/before.json", "w").write(json.dumps(last, indent=2))
post("/crash")
deadline = time.time() + 25
while time.time() < deadline:
    last = status()
    if (
        last["wirejournal"]["state"] == "done"
        and last["baseline"]["state"] == "done"
        and last["baseline"]["deliveries"] >= 2
    ):
        break
    time.sleep(0.05)
else:
    raise SystemExit("did not finish after crash: " + json.dumps(last))
open(evdir + "/after.json", "w").write(json.dumps(last, indent=2))
wj, base_lane = last["wirejournal"], last["baseline"]
if wj["deliveries"] != 1:
    raise SystemExit("wirejournal deliveries=%s" % wj["deliveries"])
if base_lane["deliveries"] != 2:
    raise SystemExit("baseline deliveries=%s" % base_lane["deliveries"])
if wj["tokens_billed"] >= base_lane["tokens_billed"]:
    raise SystemExit("baseline did not re-bill")
if wj["replayed"] < 1:
    raise SystemExit("wirejournal did not replay")
print(
    "ok wj_tokens=%s wj_mail=%s base_tokens=%s base_mail=%s replayed=%s"
    % (wj["tokens_billed"], wj["deliveries"], base_lane["tokens_billed"], base_lane["deliveries"], wj["replayed"])
)
print("evidence", evdir + "/before.json", evdir + "/after.json")
PY
