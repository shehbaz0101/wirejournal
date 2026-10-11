package llmproxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/journal"
	"github.com/shehbaz0101/wirejournal/internal/proxy"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

func TestCanonicalizeDropsVolatileKeepsStream(t *testing.T) {
	a, ma, sa, err := Canonicalize([]byte(`{"model":"m","user":"u1","metadata":{"t":1},"stream":true,"messages":[{"role":"user","content":"hi"}]}`), DefaultRules())
	if err != nil || ma != "m" || !sa {
		t.Fatalf("a: %s model=%s stream=%v err=%v", a, ma, sa, err)
	}
	b, _, _, err := Canonicalize([]byte(`{"messages":[{"content":"hi","role":"user"}],"model":"m","stream":true,"user":"u2"}`), DefaultRules())
	if err != nil || string(a) != string(b) {
		t.Fatalf("canonical forms differ:\n%s\n%s", a, b)
	}
	want := `{"messages":[{"content":"hi","role":"user"}],"model":"m","stream":true}`
	if string(a) != want {
		t.Fatalf("canonical form = %s, want %s", a, want)
	}
	_, _, stream, err := Canonicalize([]byte(`{"model":"m"}`), DefaultRules())
	if err != nil || stream {
		t.Fatalf("absent stream must be false: %v %v", stream, err)
	}
}

type llmEnv struct {
	proxy    *httptest.Server
	hits     atomic.Int64
	auths    []string
	mu       sync.Mutex
	sseBlock chan struct{}
	sseReady chan struct{}
	store    *run.Store
	dir      string
}

func startEnv(t *testing.T, sse bool, status int, body string) *llmEnv {
	t.Helper()
	e := &llmEnv{dir: t.TempDir(), sseReady: make(chan struct{}, 1), sseBlock: make(chan struct{})}
	close(e.sseBlock)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.hits.Add(1)
		e.mu.Lock()
		e.auths = append(e.auths, r.Header.Get("Authorization"))
		e.mu.Unlock()
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if status != 200 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			io.WriteString(w, body)
			return
		}
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			fl := w.(http.Flusher)
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n")
			fl.Flush()
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4}}\n\n")
			fl.Flush()
			e.sseReady <- struct{}{}
			<-e.sseBlock
			io.WriteString(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":9,"completion_tokens":4}}`)
	}))
	t.Cleanup(up.Close)
	st, err := run.NewStore(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	e.store = st
	t.Cleanup(func() { st.Close() })
	u, _ := url.Parse(up.URL)
	h := &Handler{Store: st, Upstream: u}
	e.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg, ok := proxy.Parse(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if tg.Path == "status" {
			rn, err := st.Run(tg.Run, false)
			if err != nil {
				http.Error(w, err.Error(), 404)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(rn.Status())
			return
		}
		rn, err := st.Run(tg.Run, true)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		h.ServeChat(w, r, rn, tg.Attempt)
	}))
	t.Cleanup(e.proxy.Close)
	return e
}

func post(t *testing.T, e *llmEnv, path, body, auth string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.proxy.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func read(t *testing.T, resp *http.Response) (int, string, http.Header) {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b), resp.Header
}

func TestUnaryRecordAndReplay(t *testing.T) {
	e := startEnv(t, false, 200, "")
	body := `{"model":"m","user":"alice","messages":[{"role":"user","content":"hi"}]}`
	st, b, h := read(t, post(t, e, "/r/job/a/1/v1/chat/completions", body, "Bearer SECRET-TOKEN"))
	if st != 200 || !strings.Contains(b, `"hello"`) || h.Get("X-Wirejournal-Replay") != "" {
		t.Fatalf("live: %d %s %v", st, b, h)
	}
	st, b2, h := read(t, post(t, e, "/r/job/a/2/v1/chat/completions", `{"model":"m","user":"bob","messages":[{"role":"user","content":"hi"}]}`, "Bearer SECRET-TOKEN"))
	if st != 200 || b2 != b || h.Get("X-Wirejournal-Replay") != "1" {
		t.Fatalf("replay: %d %s header=%q", st, b2, h.Get("X-Wirejournal-Replay"))
	}
	if e.hits.Load() != 1 {
		t.Fatalf("upstream billed %d, want 1", e.hits.Load())
	}
	e.mu.Lock()
	if len(e.auths) != 1 || e.auths[0] != "Bearer SECRET-TOKEN" {
		t.Fatalf("auth forwarded = %v", e.auths)
	}
	e.mu.Unlock()
	raw, _ := os.ReadFile(filepath.Join(e.dir, "job.wj"))
	if strings.Contains(string(raw), "SECRET-TOKEN") || strings.Contains(string(raw), "alice") {
		t.Fatal("journal stored an authorization header or a dropped field")
	}
}

func TestSSEForwardsFramesThenDONEAfterCommit(t *testing.T) {
	e := startEnv(t, true, 200, "")
	e.sseBlock = make(chan struct{})
	req, _ := http.NewRequest(http.MethodPost, e.proxy.URL+"/r/job/a/1/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("ct = %s", resp.Header.Get("Content-Type"))
	}
	<-e.sseReady
	chunks := make(chan []byte, 8)
	go func() {
		buf := make([]byte, 512)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				chunks <- append([]byte(nil), buf[:n]...)
			}
			if err != nil {
				close(chunks)
				return
			}
		}
	}()
	var got []byte
	deadline := time.After(2 * time.Second)
	for !strings.Contains(string(got), `"llo"`) {
		select {
		case c, ok := <-chunks:
			if !ok {
				t.Fatalf("eof before frames: %q", got)
			}
			got = append(got, c...)
		case <-deadline:
			t.Fatalf("timeout waiting for frames: %q", got)
		}
	}
	if !strings.Contains(string(got), `"he"`) || strings.Contains(string(got), "[DONE]") {
		t.Fatalf("frames before commit = %q", got)
	}
	close(e.sseBlock)
	for c := range chunks {
		got = append(got, c...)
	}
	if !strings.Contains(string(got), "data: [DONE]") {
		t.Fatalf("missing [DONE]: %q", got)
	}

	st, replay, h := read(t, post(t, e, "/r/job/a/2/v1/chat/completions", `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, ""))
	if st != 200 || h.Get("X-Wirejournal-Replay") != "1" || !strings.Contains(replay, `"he"`) || !strings.Contains(replay, "[DONE]") {
		t.Fatalf("replay: %d %s %v", st, replay, h)
	}
	if e.hits.Load() != 1 {
		t.Fatalf("billed %d", e.hits.Load())
	}
	rn, _ := e.store.Run("job", false)
	stt := rn.Status()
	at := stt.Attempts[len(stt.Attempts)-1]
	if at.TokensSaved != 13 || at.Replayed != 1 {
		t.Fatalf("status = %+v", at)
	}
}

func TestStreamedHTTPErrorWritesBody(t *testing.T) {
	e := startEnv(t, false, 429, `{"error":"slow"}`)
	body := `{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}]}`
	st, b, _ := read(t, post(t, e, "/r/job/a/1/v1/chat/completions", body, ""))
	if st != 429 || !strings.Contains(b, "slow") {
		t.Fatalf("live streamed 429: %d %s", st, b)
	}
	st, b2, h := read(t, post(t, e, "/r/job/a/2/v1/chat/completions", body, ""))
	if st != 429 || !strings.Contains(b2, "slow") || h.Get("X-Wirejournal-Replay") != "1" {
		t.Fatalf("replay streamed 429: %d %s %v", st, b2, h)
	}
	if e.hits.Load() != 1 {
		t.Fatalf("billed %d", e.hits.Load())
	}
}

func TestHTTPErrorIsJournaled(t *testing.T) {
	e := startEnv(t, false, 429, `{"error":"slow"}`)
	body := `{"model":"m","messages":[{"role":"user","content":"x"}]}`
	st, b, _ := read(t, post(t, e, "/r/job/a/1/v1/chat/completions", body, ""))
	if st != 429 || !strings.Contains(b, "slow") {
		t.Fatalf("live 429: %d %s", st, b)
	}
	st, b2, h := read(t, post(t, e, "/r/job/a/2/v1/chat/completions", body, ""))
	if st != 429 || b2 != b || h.Get("X-Wirejournal-Replay") != "1" {
		t.Fatalf("replay 429: %d %s %v", st, b2, h)
	}
	if e.hits.Load() != 1 {
		t.Fatalf("billed %d", e.hits.Load())
	}
}

func TestTransportFailureIsNotJournaled(t *testing.T) {
	dir := t.TempDir()
	st, err := run.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	u, _ := url.Parse("http://127.0.0.1:1")
	h := &Handler{Store: st, Upstream: u}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rn, err := st.Run("job", true)
		if err != nil {
			t.Error(err)
			return
		}
		h.ServeChat(w, r, rn, "")
	}))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/ignored", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	stt, b, _ := read(t, resp)
	if stt != 502 || !strings.Contains(b, "upstream") {
		t.Fatalf("transport: %d %s", stt, b)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "job.wj"))
	if strings.Contains(string(raw), "commit") {
		t.Fatal("transport failure was journaled")
	}
}

func TestAttemptIDAndDivergenceOnHTTP(t *testing.T) {
	e := startEnv(t, false, 200, "")
	a := `{"model":"m","messages":[{"role":"user","content":"one"}]}`
	b := `{"model":"m","messages":[{"role":"user","content":"two"}]}`
	read(t, post(t, e, "/r/job/a/1/v1/chat/completions", a, ""))
	read(t, post(t, e, "/r/job/a/1/v1/chat/completions", b, ""))
	st, _, h := read(t, post(t, e, "/r/job/a/2/v1/chat/completions", a, ""))
	if st != 200 || h.Get("X-Wirejournal-Replay") != "1" {
		t.Fatalf("restart replay: %d %v", st, h)
	}
	prime := `{"model":"m","messages":[{"role":"user","content":"TWO"}]}`
	st, _, h = read(t, post(t, e, "/r/job/a/2/v1/chat/completions", prime, ""))
	if st != 200 || h.Get("X-Wirejournal-Replay") == "1" {
		t.Fatalf("diverged call should go live: %d %v", st, h)
	}
	resp, err := http.Get(e.proxy.URL + "/r/job/status")
	if err != nil {
		t.Fatal(err)
	}
	_, body, _ := read(t, resp)
	if !strings.Contains(body, `"/messages/0/content"`) && !strings.Contains(body, `"pointer"`) {
		t.Fatalf("status missing divergence: %s", body)
	}
	if e.hits.Load() != 3 {
		t.Fatalf("billed %d, want 3 (two live in a1, one live after divergence)", e.hits.Load())
	}
}

func TestBadRunID(t *testing.T) {
	e := startEnv(t, false, 200, "")
	st, _, _ := read(t, post(t, e, "/r/../v1/chat/completions", `{"model":"m"}`, ""))
	if st != 400 && st != 404 {
		t.Fatalf("status %d", st)
	}
}

func TestCommitHoldsDONE(t *testing.T) {
	cf := journal.NewCrashFile()
	gate := make(chan struct{})
	started := make(chan struct{})
	n := 0
	wrapped := &syncFile{File: cf, beforeSync: func() {
		n++
		if n == 1 {
			return
		}
		close(started)
		<-gate
	}}
	rn, err := run.Open("job", wrapped)
	if err != nil {
		t.Fatal(err)
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"v\":1}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	h := &Handler{Upstream: u}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeChat(w, r, rn, "")
	}))
	defer srv.Close()

	got := make(chan string, 1)
	go func() {
		resp, err := http.Post(srv.URL, "application/json", strings.NewReader(`{"model":"m","stream":true}`))
		if err != nil {
			got <- err.Error()
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		got <- string(b)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("commit fsync never started")
	}
	select {
	case s := <-got:
		t.Fatalf("client finished before commit: %s", s)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	body := <-got
	if !strings.Contains(body, "[DONE]") || !strings.Contains(body, `"v":1`) {
		t.Fatalf("body = %s", body)
	}
}

type syncFile struct {
	journal.File
	beforeSync func()
}

func (s *syncFile) Sync() error {
	if s.beforeSync != nil {
		s.beforeSync()
	}
	return s.File.Sync()
}
