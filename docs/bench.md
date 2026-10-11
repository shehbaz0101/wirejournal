# Crash-recovery benchmark

Raw trials: [`bench.json`](bench.json).

## Claim

After `kill -9` of the agent and the proxy at a random step of a 200-step run, wirejournal resumes to the crash point in a few hundred milliseconds, re-bills **0** model calls, and delivers **1** email. A baseline agent pointed at the same mocks re-pays every call up to the crash point and, when the crash is after `send_email`, delivers a second email.

## Method

- Parent process holds the mock LLM (billing ledger) and mock mail (delivery counter).
- Each trial starts a `wirejournal proxy` and `wirejournal agent` (or the agent alone, for baseline).
- The parent waits until `GET /stats` on the mock LLM reports `calls == crashAt`, then `SIGKILL`s the children.
- Restart: new attempt id, same run id and journal directory. `resume_ms` is wall time from restart until the mock LLM sees the next *live* call (wirejournal) or until it has been called `crashAt` more times (baseline, i.e. the prefix was re-executed).
- Extra calls, extra tokens, and deliveries are read from the mocks after the restarted agent exits. The proxy's `/status` is not used.
- 30 seeds, crash points uniform in `[1, 199]`, arms interleaved (WJ then baseline per seed).
- Binary: `go build` (no `-race`). Host: linux amd64, 4 cores, load average 0.43 / 0.22 / 0.08. `WJ_STEPS=200`.

## Results (median of 30, all trials OK)

| arm | resume to crash point | extra model calls | extra tokens | email deliveries |
|---|---|---|---|---|
| wirejournal | **233 ms** (20–454) | **0** (max 0) | **0** | **1** (max 1) |
| baseline | 132 ms (7–290) | **145** (max 199) | 45 820 (max 84 376) | 1 (max **2**; 2/30 trials duplicated) |

Baseline "resume" is re-execution, not replay, so it can be faster in CPU time: it does not go through the proxy. The product claim is zero re-billing and at-most-once mail, not a faster loop.

## Checklist

1. **Why not double?** Replay is a sequential agent loop over localhost HTTP. A 199-step replay at 454 ms is ~2.3 ms/call. Replay does not fsync. The bound is the agent's request/response round trip, not the disk. Load average was 0.43 on 4 cores; the machine was not saturated.
2. **Tuned?** Both arms use the same agent, the same mocks, and the same `go build` binary. wirejournal is measured as it ships (one fsync per live commit).
3. **Limits?** 500 localhost calls/s is far below loopback capacity. A result of 0 extra calls cannot be a clock bug: the mock counter only increases on live handler entry.
4. **Errors?** 60/60 trials `ok: true`. Agent processes exited after the final assistant message.
5. **Reproduce?** 30 interleaved pairs. The extra-call gap (0 vs 145) is larger than the run-to-run spread (WJ extra is identically 0).
6. **Matter?** This is the end-to-end path: real processes, real `SIGKILL`, the same 200-step agent the demo uses.
7. **Did it happen?** Live calls increment `mockllm.Stats.Calls` inside the handler before the response is written. Deliveries append to the mock inbox. Those counters are the report.

## Other scenarios (integration tests, not in the 30-seed table)

- **Agent-only kill:** proxy stays up. An in-flight model call is committed on a detached context; the restarted agent waits and does not re-bill (`TestAgentOnlyDoesNotRebillInFlight`).
- **Dispatch window:** `send_email` sleeps 80 ms after accept. Killing the proxy then restarting redispatches the same `wj1-` key; the mock dedupes; inbox length stays 1 (`TestDispatchWindowHonorKeyNoDuplicate`).
