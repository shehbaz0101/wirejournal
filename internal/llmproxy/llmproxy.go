// Package llmproxy adapts OpenAI-compatible chat completions to run.Do.
// It owns canonicalization of chat requests, SSE splitting, and usage
// extraction. It does not decide when to commit or replay.
package llmproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/shehbaz0101/wirejournal/internal/canon"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

const maxBody = 32 << 20

// DefaultRules drop fields that OpenAI SDKs fill with per-process values.
func DefaultRules() canon.Rules {
	return canon.Rules{Drop: []string{"user", "metadata"}}
}

// Handler serves journaled POST .../v1/chat/completions for one store.
type Handler struct {
	Store    *run.Store
	Upstream *url.URL
	Rules    canon.Rules
	Client   *http.Client
}

func (h *Handler) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return http.DefaultClient
}

func (h *Handler) rules() canon.Rules {
	if len(h.Rules.Drop) == 0 && len(h.Rules.Masks) == 0 {
		return DefaultRules()
	}
	return h.Rules
}

// ServeChat handles one already-routed chat completions request.
func (h *Handler) ServeChat(w http.ResponseWriter, r *http.Request, rn *run.Run, attempt string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readAll(r.Body, maxBody+1)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > maxBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	canonical, model, stream, err := Canonicalize(body, h.rules())
	if err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := run.Call{Kind: run.KindLLM, Canonical: canonical, Label: model}
	wr := newWriter(w, stream)
	up := h.upstream(r, body, stream)
	if _, err := rn.Do(r.Context(), attempt, c, wr, up); err != nil {
		if wr.wrote {
			return
		}
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
	}
}

// Canonicalize drops volatile fields, applies masks, and keeps stream flags
// because they change the response shape.
func Canonicalize(body []byte, rules canon.Rules) (canonical []byte, model string, stream bool, err error) {
	canonical, err = canon.Canonical(body, rules)
	if err != nil {
		return nil, "", false, err
	}
	var head struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(canonical, &head); err != nil {
		return nil, "", false, err
	}
	return canonical, head.Model, head.Stream, nil
}

func (h *Handler) upstream(req *http.Request, body []byte, stream bool) run.Upstream {
	return func(ctx context.Context, _ string, e run.Emitter) (*run.Response, error) {
		u := h.Upstream.JoinPath("v1", "chat", "completions")
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if ct := req.Header.Get("Content-Type"); ct != "" {
			r.Header.Set("Content-Type", ct)
		} else {
			r.Header.Set("Content-Type", "application/json")
		}
		if auth := req.Header.Get("Authorization"); auth != "" {
			r.Header.Set("Authorization", auth)
		}
		if org := req.Header.Get("OpenAI-Organization"); org != "" {
			r.Header.Set("OpenAI-Organization", org)
		}
		resp, err := h.client().Do(r)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		ct := resp.Header.Get("Content-Type")
		if stream && resp.StatusCode == http.StatusOK && strings.Contains(ct, "text/event-stream") {
			return readStream(resp, e)
		}
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return &run.Response{
			Status:      resp.StatusCode,
			ContentType: firstCT(ct, "application/json"),
			Body:        b,
			Usage:       usageFromJSON(b),
		}, nil
	}
}

func readStream(resp *http.Response, e run.Emitter) (*run.Response, error) {
	e.Head(resp.StatusCode, firstCT(resp.Header.Get("Content-Type"), "text/event-stream"))
	var frames [][]byte
	var leftover []byte
	var done []byte
	var usage run.Usage
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			leftover = append(leftover, buf[:n]...)
			var more [][]byte
			var term []byte
			more, leftover, term = splitSSE(leftover)
			if len(term) > 0 {
				done = term
			}
			for _, fr := range more {
				e.Frame(fr)
				frames = append(frames, fr)
				if u := usageFromEvent(fr); u.Total() > 0 {
					usage = u
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if len(leftover) > 0 && !onlyWS(leftover) {
		return nil, fmt.Errorf("truncated SSE: %d bytes after last event", len(leftover))
	}
	if len(done) == 0 {
		return nil, fmt.Errorf("SSE ended without %s", doneData)
	}
	return &run.Response{
		Status:      resp.StatusCode,
		ContentType: "text/event-stream",
		Frames:      frames,
		Tail:        done,
		Usage:       usage,
	}, nil
}

func onlyWS(b []byte) bool { return len(bytes.TrimSpace(b)) == 0 }

func firstCT(got, fallback string) string {
	if got == "" {
		return fallback
	}
	return got
}

type writer struct {
	w      http.ResponseWriter
	stream bool
	wrote  bool
	status int
	ct     string
}

func newWriter(w http.ResponseWriter, stream bool) *writer {
	return &writer{w: w, stream: stream}
}

func (wr *writer) Head(status int, ct string, replay bool) {
	if wr.wrote {
		return
	}
	wr.status, wr.ct = status, ct
	if wr.stream {
		wr.flushHead(replay)
	} else if replay {
		wr.w.Header().Set("X-Wirejournal-Replay", "1")
	}
}

func (wr *writer) Frame(p []byte) {
	if !wr.wrote {
		wr.flushHead(false)
	}
	_, _ = wr.w.Write(p)
	flush(wr.w)
}

func (wr *writer) Finish(r *run.Response) {
	if wr.stream && (len(r.Frames) > 0 || len(r.Tail) > 0) {
		if !wr.wrote {
			wr.flushHead(false)
		}
		if len(r.Tail) > 0 {
			_, _ = wr.w.Write(r.Tail)
			flush(wr.w)
		}
		return
	}
	if wr.status == 0 {
		wr.status = r.Status
		wr.ct = r.ContentType
	}
	if wr.ct != "" {
		wr.w.Header().Set("Content-Type", wr.ct)
	}
	wr.wrote = true
	wr.w.WriteHeader(wr.status)
	if len(r.Body) > 0 {
		_, _ = wr.w.Write(r.Body)
	}
}

func (wr *writer) flushHead(replay bool) {
	if wr.wrote {
		return
	}
	if wr.ct == "" {
		wr.ct = "text/event-stream"
	}
	wr.w.Header().Set("Content-Type", wr.ct)
	wr.w.Header().Set("Cache-Control", "no-cache")
	if replay {
		wr.w.Header().Set("X-Wirejournal-Replay", "1")
	}
	if wr.status == 0 {
		wr.status = http.StatusOK
	}
	wr.w.WriteHeader(wr.status)
	wr.wrote = true
	flush(wr.w)
}

func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}
