// Package mockmail is an MCP tool server with search, get_ticket, and
// send_email. Deliveries are counted here; the proxy's own report is not
// ground truth. The same Idempotency-Key is one delivery.
package mockmail

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

type Message struct {
	To, Subject, Body, Key string
}

type Stats struct {
	Deliveries int       `json:"deliveries"`
	Inbox      []Message `json:"inbox"`
}

type Server struct {
	Delay time.Duration

	mu    sync.Mutex
	keys  map[string]struct{}
	inbox []Message
	id    int
}

func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Message, len(s.inbox))
	copy(out, s.inbox)
	return Stats{Deliveries: len(s.inbox), Inbox: out}
}

func (s *Server) Reset() {
	s.mu.Lock()
	s.keys, s.inbox = map[string]struct{}{}, nil
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/stats" && r.Method == http.MethodGet:
		json.NewEncoder(w).Encode(s.Stats())
	case r.URL.Path == "/reset" && r.Method == http.MethodPost:
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
	default:
		s.mcp(w, r)
	}
}

func (s *Server) mcp(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &req) != nil {
		http.Error(w, "bad json-rpc", 400)
		return
	}
	switch req.Method {
	case "initialize":
		s.id++
		w.Header().Set("Mcp-Session-Id", "mail-1")
		writeResult(w, req.ID, `{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"mockmail","version":"0"}}`)
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "tools/list":
		writeResult(w, req.ID, `{"tools":[`+
			`{"name":"search","description":"search docs","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}},`+
			`{"name":"get_ticket","description":"fetch a ticket","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":true}},`+
			`{"name":"send_email","description":"send an email","inputSchema":{"type":"object"},"annotations":{"readOnlyHint":false,"destructiveHint":true,"idempotentHint":false}}]}`)
	case "tools/call":
		s.call(w, r, req.ID, req.Params)
	default:
		writeErr(w, req.ID, "unknown method")
	}
}

func (s *Server) call(w http.ResponseWriter, r *http.Request, id, params json.RawMessage) {
	var p struct {
		Name      string            `json:"name"`
		Arguments map[string]string `json:"arguments"`
		Meta      map[string]string `json:"_meta"`
	}
	if json.Unmarshal(params, &p) != nil {
		writeErr(w, id, "bad params")
		return
	}
	switch p.Name {
	case "search":
		writeResult(w, id, toolText("search:"+p.Arguments["q"]))
	case "get_ticket":
		writeResult(w, id, toolText("ticket #"+p.Arguments["id"]+": password reset pending"))
	case "send_email":
		key := r.Header.Get("Idempotency-Key")
		if key == "" && p.Meta != nil {
			key = p.Meta["wirejournal/idempotency-key"]
		}
		s.mu.Lock()
		if s.keys == nil {
			s.keys = map[string]struct{}{}
		}
		_, dup := s.keys[key]
		if key == "" || !dup {
			if key != "" {
				s.keys[key] = struct{}{}
			}
			s.inbox = append(s.inbox, Message{To: p.Arguments["to"], Subject: p.Arguments["subject"], Body: p.Arguments["body"], Key: key})
		}
		s.mu.Unlock()
		if d := s.Delay; d > 0 {
			time.Sleep(d)
		}
		if dup {
			writeResult(w, id, toolText("duplicate suppressed"))
		} else {
			writeResult(w, id, toolText("sent"))
		}
	default:
		writeErr(w, id, "unknown tool")
	}
}

func toolText(s string) string {
	b, _ := json.Marshal(s)
	return `{"content":[{"type":"text","text":` + string(b) + `}],"isError":false}`
}

func writeResult(w http.ResponseWriter, id json.RawMessage, result string) {
	if len(id) == 0 {
		id = []byte("null")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(id) + `,"result":` + result + `}`))
}

func writeErr(w http.ResponseWriter, id json.RawMessage, msg string) {
	if len(id) == 0 {
		id = []byte("null")
	}
	b, _ := json.Marshal(msg)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(id) + `,"error":{"code":-32000,"message":` + string(b) + `}}`))
}
