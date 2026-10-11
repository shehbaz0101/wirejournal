# Crash-recovery bench

The bench parent holds the mock LLM and mock mail. It `SIGKILL`s child processes at a chosen call count, restarts, and reads extra calls, extra tokens, and deliveries from those mocks.

## Sub-features

- `bench-kill-both` kills agent and proxy.
- `bench-agent-only` kills only the agent and must not re-bill an in-flight model call.
- `bench-dispatch` kills during `send_email` and relies on the `wj1-` key.

## How to get to it (user POV)

- `go test ./internal/bench`
- `./wirejournal bench -n 30 -steps 200 -out docs/bench.json` for a published table.

## Driving it with Go tests / CLI

Preconditions:

- Not `go test -short`. Those tests skip.
- The host can `SIGKILL` child processes.

- **Integration.** `go test -count=1 -timeout 5m ./internal/bench -run TestCrashRecoveryVsBaseline`. Wirejournal median extra calls must be 0. Baseline median extra calls must be at least 1.
- **In-flight.** `go test -count=1 ./internal/bench -run TestAgentOnlyDoesNotRebillInFlight`.
- **Ambiguous window.** `go test -count=1 ./internal/bench -run TestDispatchWindowHonorKeyNoDuplicate`. Inbox length stays 1.

## Gotchas

- Published numbers in `docs/bench.md` came from a 30-seed, 200-step run. The CI smoke uses 3 trials at 6 steps. Do not replace the README table with a smoke run.
- Extra calls of 0 cannot be a clock bug. `mockllm.Stats.Calls` only increases on handler entry.
- Baseline resume time is re-execution, not replay. Do not claim it is slower.
