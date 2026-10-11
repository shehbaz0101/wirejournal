# wirejournal design

This is the design rationale for v0. It came out of an architecture arena: two independent
candidate designs (Claude Opus and Grok), a cross-judge on a third run, and a synthesis by the
lead. The decision trail is in [`decisions.tsv`](decisions.tsv).

## Problem

A long-running agent dies (OOM, deploy, `kill -9`) and starts again from the top. It re-sends
every model call, which bills again, and every tool call, which can repeat a side effect such as
an email. wirejournal is a proxy in front of the agent. The agent changes configuration only: its
OpenAI base URL and its MCP tool URL. The proxy journals every call durably, serves finished calls
from the journal on restart, runs each side-effecting tool call at most once, and goes live again
at the crash point.

Three facts make the shape non-obvious:

- Byte-identical requests are legitimate. An agent retries a bad sample with the same request, or
  sends the same email twice on purpose. The k-th identical request must map to the k-th result.
- The proxy cannot see the agent process. It sees URLs, request bodies, and sockets.
- A streaming client sees bytes before the proxy can make them durable.

## Usage

```bash
wirejournal proxy --listen 127.0.0.1:7070 --data ./wj-data \
	--llm-upstream https://api.openai.com --mcp-upstream http://127.0.0.1:9200/mcp

export OPENAI_BASE_URL=http://127.0.0.1:7070/r/job-42/v1
export MCP_URL=http://127.0.0.1:7070/r/job-42/mcp
python agent.py          # unchanged; kill -9 it and run it again
```

`job-42` is the run. The run is the durable identity of one job and stays the same across
restarts. For exact restart detection when the proxy outlives the agent, add a fresh attempt id
per agent launch: `http://127.0.0.1:7070/r/job-42/a/$(uuidgen)/v1`.

`GET /r/job-42/status` reports replayed and live calls, tokens not re-billed, the time the run
caught up to the crash point, the first divergence, and unresolved ambiguous side effects.

Optional `wirejournal.json` sets tool classes and request canonicalization:

```json
{
	"canonical": { "drop_fields": ["user", "metadata"], "masks": ["\\d{4}-\\d{2}-\\d{2}T[0-9:.]+Z"] },
	"tools": { "send_email": { "class": "side_effect", "idempotency_key": true } }
}
```

## Shape

### Language

Go with the standard library only. The proxy needs an HTTP server and an HTTPS client, SSE
flushing, JSON, CRC32C, `flock`, and `os/exec` for a demo that sends real `SIGKILL`s. Go's
standard library has all of them, so the build has no third-party dependencies. A Rust build
without crates would need a hand-written TLS client. The proxy is I/O bound, so Go's garbage
collector costs nothing that matters next to a model call. One static binary carries every
subcommand (`proxy`, `mockllm`, `mockmail`, `agent`, `bench`, `demo`) and runs on Render's native
Go runtime.

### Journal (`internal/journal`)

A generic append-only log. It knows framing, durability, recovery, and locking. It does not know
what a call is.

- One file per run. Runs do not share a writer or a lock.
- File header `WJNL0001`. Record frame: `"WJR1" | len u32 LE | crc32c(len || payload) u32 LE |
  payload`. `len` is between 1 and 64 MiB.
- `Append` writes the frame and calls `fsync` before it returns. An acknowledged record survives
  power loss. A failed write or `fsync` poisons the log, because the file state is unknown after
  a failed `fsync`.
- Creating the file writes the header, then fsyncs the file and its parent directory.
- `flock(LOCK_EX|LOCK_NB)` gives one writer per file. The kernel drops the lock when the process
  dies, including on `SIGKILL`, so there is no stale lock to clean up.
- Recovery scans from the header. At the first bad frame (short, bad magic, bad length, or bad
  CRC), it looks for any valid frame after it. If none exists, the bad bytes are a torn tail:
  truncate, fsync, continue. If one exists, the damage is in the middle of the file: refuse to
  open with `ErrCorrupt` and change nothing, because truncating would drop acknowledged records.
- Storage goes through a small `File` interface. `CrashFile` implements it in memory and models
  power loss. Writes are volatile until `Sync`, and `LosePower` keeps the synced bytes plus a torn
  prefix (or zeros) of the rest.

### Call identity (`internal/run`)

A call's identity is `(kind, sha256(canonical request), k)`. `kind` is `llm` or `tool`. `k` counts
identical requests within the current attempt, assigned in arrival order under the run's mutex.
One map per run, `entries[CallKey]`, is both the replay cache and the at-most-once ledger. Entries
from every attempt stay matchable. A diverged agent that sends an email the run already sent gets
the recorded result, not a second email.

An attempt is one life of the agent process. A new attempt resets the `k` counters. It starts
when:

1. The proxy process starts. This covers container death, where agent and proxy die together.
2. The URL carries a new attempt id (`/r/{run}/a/{attempt}/...`). This is exact.
3. With no attempt id in the URL, the run's first recorded model request arrives again after the
   current attempt has handled other requests. A sequential agent that restarts sends that request
   first. A naive retry of the first request does not trigger it, because no other request came
   in between.

Rule 3 can misfire. It fires falsely if an agent deliberately repeats its very first request
mid-run. A false fire maps later identical calls onto recorded ones, which suppresses a repeated
side effect but never duplicates one. It misses a restart whose first request differs from the
recorded one. Replay cannot work for that agent anyway, and the divergence report names the
field at step 1. Use rule 2 when that matters.

| crash mode | behavior |
|---|---|
| agent and proxy killed | Recovery truncates a torn tail and rebuilds `entries`. The first attempt of the new process starts at `k=1`. Committed calls replay. An in-flight model call goes live again, and that is the only re-billing. |
| only the agent killed | Upstream calls run on a detached context and commit even after the client disconnects. A new attempt that asks for a call still in flight waits for it and replays it. Nothing is billed twice and no side effect is ambiguous. |
| crash twice | The journal is append-only. Each recovery sees every attempt's committed calls. |
| identical request twice | `k=1` and `k=2` map to two recorded results. A retry after a bad sample gets a fresh live sample. |
| concurrent requests | Distinct requests never interact. Identical concurrent requests match in arrival order and may swap results across attempts. |

### Commit points

The run package owns every commit decision in one function, `Run.Do`. The protocol adapters
(`llmproxy`, `mcpgw`) never write the journal.

| call | before dispatch | after upstream completes | client sees |
|---|---|---|---|
| model, streaming | nothing | `Committed` fsynced | SSE events as they arrive, then `data: [DONE]` only after the commit |
| model, unary | nothing | `Committed` fsynced | the body, after the commit |
| read-only or idempotent tool | nothing | `Committed` fsynced | the result, after the commit |
| side-effecting tool | `Intent` fsynced | `Committed` fsynced | the result, after the commit |

The client never sees a complete response that is not durable. Every complete upstream response
is journaled, including 4xx and 5xx, so an agent that retried a 429 replays the same 429 and the
`k` numbering cannot drift. A transport failure (no HTTP response) is not journaled.

### The ambiguous window

An `Intent` without a `Committed` record means the proxy died during dispatch. The side effect may
or may not have happened. This cannot be solved in general, so the policy is explicit:

- Read-only and idempotent tools have no intent. They run again.
- A side-effecting tool whose server honors idempotency keys (`"idempotency_key": true`) is sent
  again with the same key. The key is derived from the call identity
  (`wj1-` + 26 base32 chars of `sha256(run, kind, hash, k)`), so it is stable across attempts and
  crashes. wirejournal forwards it on every side-effecting call as `params._meta["wirejournal/idempotency-key"]`
  and as an `Idempotency-Key` header.
- Any other side-effecting tool fails closed. The agent gets an MCP tool result with
  `isError: true` that says the outcome is unknown and the call was not repeated. The run is then
  quarantined: wirejournal refuses every new side-effecting dispatch in that run, so a model that
  retries with a changed body cannot send a duplicate. `GET /status` lists the call.

### Tool classification (`internal/mcpgw`)

Precedence: the config file, then the MCP annotations from `tools/list`, then the MCP spec
defaults. `readOnlyHint: true` is read-only. Otherwise `idempotentHint: true` is idempotent.
Otherwise the tool is side-effecting, which is also what absent annotations mean
(`readOnly=false`, `destructive=true`, `idempotent=false`). The gateway reads annotations from the
MCP server itself, so it works even when an agent SDK drops them
(openai/openai-agents-python#5315). `tools/list` is journaled like a read-only call, so replay
works with the tool server down and classification cannot change between attempts.

The gateway answers `initialize` itself and keeps its own session with the upstream MCP server, so
a restarted agent never reuses a stale upstream session id.

### Divergence

Each attempt counts `remaining`, the committed model calls it has not consumed yet. On a model
request that misses:

- `remaining == 0` means the agent caught up to the crash point. The request is new work. The
  attempt records the catch-up time, which is the resume time.
- `remaining > 0` means divergence. The agent asked for something its predecessor never asked
  while recorded calls were still waiting. wirejournal compares the request with the lowest
  unconsumed recorded model call, reports the first differing JSON pointer with short excerpts,
  logs a warning, and goes live. Divergence changes the report, not matching. Later identical
  requests still replay.

### Package map

```
cmd/wirejournal      subcommands and flags
internal/journal     framed log, fsync, recovery, flock, CrashFile
internal/canon       canonical JSON, masks, first-difference diff (pure)
internal/run         Store, Run, call identity, attempts, Run.Do, divergence, records
internal/llmproxy    OpenAI chat completions adapter, SSE split, usage
internal/mcpgw       MCP Streamable HTTP gateway, classification, idempotency key
internal/proxy       HTTP routes and status endpoints
internal/mockllm     deterministic OpenAI-compatible model with a billing ledger
internal/mockmail    MCP tool server with a delivery counter that dedupes on the key
internal/agent       SDK-free 200-step agent
internal/lane        start, kill -9, and restart one agent (and its proxy)
internal/bench       crash-recovery benchmark
internal/demo        web demo with two lanes
```

A request crosses three files: the adapter (`llmproxy` or `mcpgw`), `run.Do`, and `journal`.

## Synthesis decision

Base: candidate 1. Its single `Run.Do` owns every commit decision, and its global per-run ledger
handles replay and at-most-once in one structure. The cross-judge picked the same base.

Grafts from candidate 2:

- The `File` seam and `CrashFile` power-loss model, so durability is tested against lost writes,
  not only against `kill -9`.
- Corruption detection that looks for a valid frame after the bad one, so a zero-filled tail
  after power loss is a torn tail and not a fatal error. The CRC also covers the length field.
- A pure decision function inside `run`, separate from I/O, so the match rules are table tests.
- Recovery checks: the stored hash must match the stored canonical bytes, and a second commit for
  one call is corruption.
- The demo and benchmark keep the mock services in the parent process and `SIGKILL` the children.
  Published numbers come from the mocks' counters, never from the proxy's report.

Changed from both:

- Restart detection. Candidate 1 required an attempt id in the URL. Its claim that a reused
  attempt id still blocks duplicates was wrong, because the repeated email gets a new `k`.
  Candidate 2 inferred restarts from TCP connection freshness, which has holes in both
  directions. v0 uses the proxy's lifetime, an optional attempt id, and rule 3 above. Rule 3 needs
  no socket tracking and fails toward suppression.

Rejected:

- Candidate 2's `Cut` records. Abandoning recorded side effects on divergence lets a diverged
  agent send them again.
- Candidate 2's intent record for every call. Model and read-only intents add an fsync without a
  durable benefit.
- Candidate 1's group commit, durable `Replayed`, `Divergence`, and `CaughtUp` records, the
  8-candidate divergence scorer, and the `resolve` command. They do not earn their complexity in
  v0. Measured fsync is 0.42 ms at p50.

## Tradeoffs accepted

- We accept one fsync per committed call in exchange for "acknowledged means durable".
- We accept holding back `data: [DONE]` by one fsync in exchange for "a stream the client saw
  complete is in the journal".
- We accept that rule 3 can suppress a deliberately repeated side effect in exchange for a static
  base URL that handles agent-only restarts.
- We accept run-wide quarantine after an unresolved ambiguous side effect in exchange for never
  duplicating through a model-driven retry. v0 has no command to resolve it. Start a new run.
- We accept plaintext prompts in the journal. Authorization headers are never written.

## Alternatives considered

- Content-addressed cache with no occurrence index. Smallest structure. It replays a bad sample
  forever and merges two deliberate emails into one.
- Positional cursor (the i-th request maps to the i-th record). It breaks on concurrency and on a
  single extra or missing call, and it fails silently.
- Durable execution with an SDK (Temporal, Restate, DBOS). It names steps exactly and handles the
  ambiguous window better. It requires code changes and a programming model, which is what
  wirejournal exists to avoid.
- One shared journal for all runs. It couples runs through one lock and one corruption domain.

## Not in v0

Anthropic Messages, OpenAI Responses, and embeddings (pass-through only). MCP stdio transport.
Journal compaction, encryption, and replication. Proxy authentication. Timing fidelity on replay.
Reconciliation queries against tool servers. Durable divergence reports.
