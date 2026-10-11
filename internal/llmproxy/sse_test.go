package llmproxy

import (
	"io"
	"net/http"
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

type frameCap struct{ frames [][]byte }

func (f *frameCap) Head(int, string) {}
func (f *frameCap) Frame(p []byte)   { f.frames = append(f.frames, append([]byte(nil), p...)) }

func TestReadStreamKeepsDONEAfterTrailingRead(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("data: {\"v\":1}\n\ndata: [DONE]\n\n"))
		_, _ = pw.Write([]byte("\n"))
		_ = pw.Close()
	}()
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       pr,
	}
	cap := &frameCap{}
	got, err := readStream(resp, cap)
	if err != nil {
		t.Fatal(err)
	}
	if !isDone(got.Tail) {
		t.Fatalf("lost [DONE]: tail=%q frames=%d", got.Tail, len(cap.frames))
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
