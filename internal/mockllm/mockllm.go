// Package mockllm is a deterministic OpenAI-compatible chat model. Billing
// is counted here, not in the proxy, so benchmark numbers are ground truth.
package mockllm

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/script"
)

type Stats struct {
	Calls            int `json:"calls"`
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type Server struct {
	Delay time.Duration
	After func(step int)

	mu    sync.Mutex
	stats Stats
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Server) Reset() {
	s.mu.Lock()
	s.stats = Stats{}
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/stats" && r.Method == http.MethodGet:
		json.NewEncoder(w).Encode(s.Stats())
	case r.URL.Path == "/reset" && r.Method == http.MethodPost:
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/chat/completions" && r.Method == http.MethodPost:
		s.chat(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	var req struct {
		Stream   bool             `json:"stream"`
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	step := script.StepOf(req.Messages)
	p, c := script.Usage(step)
	s.mu.Lock()
	s.stats.Calls++
	s.stats.PromptTokens += p
	s.stats.CompletionTokens += c
	s.mu.Unlock()
	if s.After != nil {
		s.After(step)
	}
	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}

	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, ev := range script.SSE(step) {
			w.Write(ev)
			if fl != nil {
				fl.Flush()
			}
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(script.AssistantJSON(step))
}
