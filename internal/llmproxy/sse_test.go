package llmproxy

import (
	"testing"
)

func TestSplitSSE(t *testing.T) {
	in := []byte("data: {\"d\":\"he\"}\n\ndata: {\"d\":\"llo\",\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	frames, leftover, done := splitSSE(in)
	if len(frames) != 2 || len(leftover) != 0 || !isDone(done) {
		t.Fatalf("frames=%d leftover=%q done=%q", len(frames), leftover, done)
	}
	if u := usageFromEvent(frames[1]); u.Prompt != 3 || u.Completion != 2 {
		t.Fatalf("usage = %+v", u)
	}

	partial := []byte("data: {\"d\":\"he\"}\n\ndata: {\"d\":\"")
	frames, leftover, done = splitSSE(partial)
	if len(frames) != 1 || string(leftover) != "data: {\"d\":\"" || done != nil {
		t.Fatalf("partial: frames=%q leftover=%q done=%q", frames, leftover, done)
	}

	crlf := []byte("data: a\r\n\r\ndata: [DONE]\r\n\r\n")
	frames, leftover, done = splitSSE(crlf)
	if len(frames) != 1 || !isDone(done) || len(leftover) != 0 {
		t.Fatalf("crlf: frames=%q done=%q leftover=%q", frames, done, leftover)
	}
}

func TestUsageFromJSON(t *testing.T) {
	u := usageFromJSON([]byte(`{"id":"x","usage":{"prompt_tokens":11,"completion_tokens":7}}`))
	if u.Prompt != 11 || u.Completion != 7 {
		t.Fatalf("%+v", u)
	}
	if usageFromJSON([]byte(`{"id":"x"}`)).Total() != 0 {
		t.Fatal("missing usage must be zero")
	}
}
