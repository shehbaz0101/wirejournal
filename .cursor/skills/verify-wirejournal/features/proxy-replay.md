# Replay through the proxy

An SDK-free agent pointed at `OPENAI_BASE_URL` and `MCP_URL` under `/r/{run}/a/{attempt}/` replays a second attempt from the journal. The mock LLM call count does not rise. The mock inbox stays at one email.

## Sub-features

- `proxy-health` serves `/healthz`.
- `proxy-first` records one full run.
- `proxy-second` replays on a new attempt id.

## How to get to it (user POV)

- Run `wirejournal proxy` and `wirejournal agent` as in the README, or run the package test that does the same in-process.

## Driving it with Go tests / CLI

Preconditions:

- `go test` can build the module.
- `WJ_STEPS` is small (the packaged test uses 5).

- **Packaged proof.** `go test -count=1 ./internal/agent -run TestAgentThroughProxyReplaysWithoutRebilling`. It must pass.
- **CLI path.** Start `mockllm`, `mockmail`, and `proxy` on free ports. Run `wirejournal agent` twice with attempt `1` then `2`. `GET <mockllm>/stats` `calls` must be unchanged after the second run. `GET <mockmail>/stats` `deliveries` must stay `1`.

## Gotchas

- The agent talks to `.../v1` on the proxy. The proxy's `-llm-upstream` is the origin without a forced `/v1` suffix when the origin already serves `/v1/chat/completions`.
- A retry on the same attempt id is not a restart. Use a new attempt id.
- Auth headers must never appear in a journal file. If you dump `*.wj`, grep for `Bearer` and fail on a hit.
