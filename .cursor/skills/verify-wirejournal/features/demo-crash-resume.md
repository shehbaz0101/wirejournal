# Crash and resume the demo

The demo runs the same ticket-triage agent in two lanes. Crash sends `SIGKILL` to both agent children. The left lane replays from a journal. The right lane starts over and can send a second email.

## Sub-features

- `demo-health` serves `/` and `/healthz`.
- `demo-progress` increments `step` on both lanes from mock LLM calls.
- `demo-crash` POSTs `/crash` or clicks `#crash` after `send_email`.
- `demo-resume` shows wirejournal replay with one email and baseline re-pay with two.

## How to get to it (user POV)

- Open `http://<listen>/` in a browser.
- Or run `scripts/verify-demo.sh`.
- Press **Crash (kill -9)** or **Reset lanes**.

## Driving it with verify-demo / HTTP

Preconditions:

- A demo this run started is healthy on the listen address.
- Doctor has passed.

- **See the page.** `GET /` contains `Crash (kill -9)`.
- **Wait for mail.** Poll `GET /status` until `wirejournal.deliveries >= 1` and `baseline.deliveries >= 1`.
- **Crash.** `POST /crash` or click `#crash`.
- **Wait for both lanes to finish.** `wirejournal.state` and `baseline.state` are `done`.
- **Proof.** `wirejournal.deliveries == 1`, `baseline.deliveries == 2`, `wirejournal.tokens_billed < baseline.tokens_billed`, and `wirejournal.replayed >= 1`. Save `/status` as `evidence/after.json`.

## Gotchas

- Crashing before `send_email` still proves no re-bill, but baseline may also end at one email. Crash after both inboxes are non-empty when you need the duplicate.
- `/status` is the server truth. The SSE view can lag one tick. Wait 300 ms after the JSON condition before taking a screenshot.
- Reset starts a new journal directory. Do not compare tokens across a reset.
- The demo kills agent children only. The in-process proxy stays up.
