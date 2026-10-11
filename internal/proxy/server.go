package proxy

import (
	"encoding/json"
	"net/http"

	"github.com/shehbaz0101/wirejournal/internal/run"
)

// Chat is the journaled chat-completions adapter.
type Chat interface {
	ServeChat(w http.ResponseWriter, r *http.Request, rn *run.Run, attempt string)
}

// Server is the process-wide HTTP handler: chat completions, status, and
// later the MCP gateway.
type Server struct {
	Store *run.Store
	LLM   Chat
	MCP   http.Handler
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	tg, ok := Parse(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if tg.Path == "status" {
		s.status(w, r, tg.Run)
		return
	}
	rn, err := s.Store.Run(tg.Run, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch {
	case tg.Path == "v1/chat/completions":
		s.LLM.ServeChat(w, r, rn, tg.Attempt)
	case tg.Path == "mcp" && s.MCP != nil:
		r = r.Clone(r.Context())
		r.Header.Set("X-Wirejournal-Run", tg.Run)
		r.Header.Set("X-Wirejournal-Attempt", tg.Attempt)
		s.MCP.ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rn, err := s.Store.Run(id, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rn.Status())
}
