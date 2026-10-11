# Crash-recovery benchmark

Raw trials: [`bench.json`](bench.json).

## Claim

After `kill -9` of the agent and the proxy at a random step of a 200-step run, wirejournal resumes to the crash point in a few hundred milliseconds, re-bills a median of **0** model calls, and delivers **1** email. One of 30 trials re-billed a single in-flight call that the mock had accepted before the commit landed. A baseline agent pointed at the same mocks re-pays the prefix and, when the crash is after `send_email`, delivers a second email.

## Method

- Parent process holds the mock LLM (billing ledger) and mock mail (delivery counter).
- Each trial starts a `wirejournal proxy` and `wirejournal agent` (or the agent alone, for baseline).
- The parent waits until `GET /stats` on the mock LLM reports `calls == crashAt`, then `SIGKILL`s the children.
- Restart: new attempt id, same run id and journal directory. `resume_ms` is wall time from restart until the mock LLM sees the next live call (wirejournal) or until it has been called `crashAt` more times (baseline, meaning the prefix was re-executed).
- After the restarted agent exits, `extra_calls` is `mockllm.Stats.Calls - Steps`. `extra_tokens` is mock prompt plus completion minus one full run (`script.Usage` for steps `0` to `Steps-1`). Deliveries are `len(mockmail.inbox)`. The proxy `/status` is not used.
- A trial is `ok` only if the next live call arrived and the mock call count is at least `Steps`.
- 30 seeds, crash points uniform in `[1, 199]`, arms interleaved (WJ then baseline per seed).
- Binary: `go build` (no `-race`). Host: linux amd64, 4 cores, load average 0.70 / 0.26 / 0.22. `WJ_STEPS=200`.

## Results (median of 30, all 60 trials `ok`)

| arm | resume to the crash point | extra model calls | extra tokens | email deliveries |
|---|---|---|---|---|
| wirejournal | **166 ms** (19-388) | **0** (max **1**) | **0** (max 688) | **1** (max 1) |
| baseline | 96 ms (6-208) | **147** (max 200) | 47040 (max 85200) | 1 (max **2**; 2/30 trials sent a second email) |

Baseline "resume" is re-execution, not replay, so it can use less CPU time than journal replay. The product claim is low re-billing and at-most-once mail, not a faster loop.

The one wirejournal extra call (seed 22, crash at 165) is the kill-both in-flight window: the mock increments `Calls` on accept, then the proxy and agent die before the journal commit. Restart runs that step live. Agent-only kills do not re-bill that window (`TestAgentOnlyDoesNotRebillInFlight`).

## Checklist

1. **Why not double?** Replay is a sequential agent loop over localhost HTTP. A 199-step replay at 388 ms is about 2 ms/call. Replay does not fsync. The bound is the agent's request/response round trip, not the disk. Load average was 0.70 on 4 cores.
2. **Tuned?** Both arms use the same agent, the same mocks, and the same `go build` binary. wirejournal is measured as it ships (one fsync per live commit).
3. **Limits?** A few hundred localhost calls/s is far below loopback capacity. Extra calls cannot be a clock bug. The mock counter only increases on live handler entry.
4. **Errors?** 60/60 trials `ok: true`. Agent processes exited after the final assistant message.
5. **Reproduce?** 30 interleaved pairs. The extra-call gap (median 0 vs 147) is larger than the run-to-run spread. WJ extra is 0 on 29/30 trials.
6. **Matter?** This is the end-to-end path: real processes, real `SIGKILL`, the same 200-step agent the demo uses.
7. **Did it happen?** Live calls increment `mockllm.Stats.Calls` inside the handler before the response is written. Deliveries append to the mock inbox. Extra is those counters minus one full run.

## Other scenarios (integration tests, not in the 30-seed table)

- **Agent-only kill:** proxy stays up. An in-flight model call is committed on a detached context; the restarted agent waits and does not re-bill (`TestAgentOnlyDoesNotRebillInFlight`).
- **Dispatch window:** `send_email` sleeps 80 ms after accept. Killing the proxy then restarting redispatches the same `wj1-` key; the mock dedupes; inbox length stays 1 (`TestDispatchWindowHonorKeyNoDuplicate`).
