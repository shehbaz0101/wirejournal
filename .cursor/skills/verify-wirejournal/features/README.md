# wirejournal verification map

Read this index, then the feature file for the path you are proving.

## Baseline preconditions

- Build `./cmd/wirejournal` into a disposable directory.
- Start the demo with `-listen 127.0.0.1:<free-port> -steps 8 -delay 80ms`, or run `scripts/verify-demo.sh`.
- Run the Doctor checks in the parent skill.
- Never drive a demo this run did not start.

## Driving conventions

- HTTP paths and element ids are literal: `/`, `/healthz`, `/status`, `/crash`, `/reset`, `#crash`, `#reset`.
- Prefer `GET /status` over reading the SSE stream when asserting numbers.
- Tokens and inbox counts must come from `/status` (mock ledgers). Do not quote the proxy's `/r/{run}/status` as the published number.

## Proof and skip reporting

- Capture the action and the resulting `/status` body.
- A UI proof also needs the page to contain `Crash (kill -9)`.
- Report an unreachable path with the command and the missing precondition.

## Features

- [Crash and resume the demo](./demo-crash-resume.md) is the primary user path.
- [Replay through the proxy](./proxy-replay.md) is the drop-in `base_url` path.
- [Crash-recovery bench](./bench-smoke.md) is the published-number path.
