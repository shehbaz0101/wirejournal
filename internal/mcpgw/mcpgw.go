// Package mcpgw is an MCP Streamable-HTTP gateway. To the agent it is an MCP
// server; to the upstream tool server it is an MCP client. Replay policy
// belongs to run.Do.
package mcpgw

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/shehbaz0101/wirejournal/internal/canon"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

const (
	// MetaKey is the params._meta field carrying the derived idempotency key.
	MetaKey     = "wirejournal/idempotency-key"
	maxBody     = 8 << 20
	protoLatest = "2024-11-05"
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

// Handler serves /r/{run}/mcp.
type Handler struct {
	Store    *run.Store
	Upstream *url.URL
	Rules    map[string]Rule
	Client   *http.Client

	mu     sync.Mutex
	anns   map[string]map[string]Annotations // run -> tool -> annotations
	upSess map[string]string                 // run -> upstream Mcp-Session-Id
}

func (h *Handler) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return http.DefaultClient
}

// ServeMCP handles one already-routed MCP request.
func (h *Handler) ServeMCP(w http.ResponseWriter, r *http.Request, rn *run.Run, attempt string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Method == "" {
		http.Error(w, "invalid json-rpc", http.StatusBadRequest)
		return
	}
	switch req.Method {
	case "initialize":
		h.initialize(w, req)
	case "notifications/initialized", "notifications/cancelled":
		w.WriteHeader(http.StatusAccepted)
	case "tools/list":
		h.journaled(w, r, rn, attempt, req, run.Call{Kind: run.KindMCP, Label: "tools/list", Class: run.ClassReadOnly, Canonical: []byte(`{"method":"tools/list"}`)})
	case "tools/call":
		h.call(w, r, rn, attempt, req)
	default:
		h.passthrough(w, r, rn, body)
	}
}

func (h *Handler) initialize(w http.ResponseWriter, req rpcRequest) {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(req.Params, &params)
	ver := protoLatest
	if params.ProtocolVersion != "" {
		ver = params.ProtocolVersion
	}
	id := req.ID
	if len(id) == 0 {
		id = []byte("1")
	}
	result := fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"wirejournal","version":"0"}}`, ver)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Mcp-Session-Id", "wj-local")
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(id) + `,"result":` + result + `}`))
}

func (h *Handler) call(w http.ResponseWriter, r *http.Request, rn *run.Run, attempt string, req rpcRequest) {
	canonical, name, err := Canonicalize(req.Params)
	if err != nil {
		http.Error(w, "invalid tools/call: "+err.Error(), http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	ann := h.anns[rn.ID()][name]
	h.mu.Unlock()
	class, honor := Classify(name, ann, h.Rules)
	c := run.Call{Kind: run.KindTool, Canonical: canonical, Label: name, Class: class, HonorKey: honor}
	h.journaled(w, r, rn, attempt, req, c)
}

func (h *Handler) journaled(w http.ResponseWriter, r *http.Request, rn *run.Run, attempt string, req rpcRequest, c run.Call) {
	wr := &rpcWriter{w: w, id: req.ID}
	if c.Kind == run.KindMCP {
		wr.onBody = func(b []byte) { h.noteTools(rn.ID(), b) }
	}
	up := h.upstream(rn.ID(), req, c)
	out, err := rn.Do(r.Context(), attempt, c, wr, up)
	if err != nil && !wr.wrote {
		http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	if out.Blocked {
		writeRPC(w, req.ID, []byte(`{"content":[{"type":"text","text":`+jsonString(out.Message)+`}],"isError":true}`), nil)
	}
}

func (h *Handler) upstream(runID string, req rpcRequest, c run.Call) run.Upstream {
	return func(ctx context.Context, key string, e run.Emitter) (*run.Response, error) {
		body := req
		if c.Kind == run.KindTool && key != "" {
			patched, err := injectKey(req.Params, key)
			if err != nil {
				return nil, err
			}
			body.Params = patched
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		resp, err := h.roundTrip(ctx, runID, raw, key)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if c.Kind == run.KindMCP {
			h.noteTools(runID, b)
		}
		return &run.Response{Status: resp.StatusCode, ContentType: "application/json", Body: stripID(b)}, nil
	}
}

func (h *Handler) passthrough(w http.ResponseWriter, r *http.Request, rn *run.Run, body []byte) {
	resp, err := h.roundTrip(r.Context(), rn.ID(), body, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if k == "Mcp-Session-Id" {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (h *Handler) roundTrip(ctx context.Context, runID string, body []byte, idem string) (*http.Response, error) {
	if err := h.ensureUpstream(ctx, runID); err != nil {
		return nil, err
	}
	u := *h.Upstream
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	h.mu.Lock()
	sid := h.upSess[runID]
	h.mu.Unlock()
	if sid != "" {
		req.Header.Set("Mcp-Session-Id", sid)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	return h.client().Do(req)
}

func (h *Handler) ensureUpstream(ctx context.Context, runID string) error {
	h.mu.Lock()
	if h.upSess[runID] != "" {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	init := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"wirejournal","version":"0"}}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.Upstream.String(), bytes.NewReader(init))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := h.client().Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	sid := resp.Header.Get("Mcp-Session-Id")
	if sid == "" {
		sid = "up-anon"
	}
	h.mu.Lock()
	if h.upSess == nil {
		h.upSess = map[string]string{}
	}
	h.upSess[runID] = sid
	h.mu.Unlock()
	return nil
}

func (h *Handler) noteTools(runID string, raw []byte) {
	var env rpcResponse
	if json.Unmarshal(raw, &env) != nil {
		return
	}
	var result struct {
		Tools []struct {
			Name        string      `json:"name"`
			Annotations Annotations `json:"annotations"`
		} `json:"tools"`
	}
	if json.Unmarshal(env.Result, &result) != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.anns == nil {
		h.anns = map[string]map[string]Annotations{}
	}
	m := h.anns[runID]
	if m == nil {
		m = map[string]Annotations{}
		h.anns[runID] = m
	}
	for _, t := range result.Tools {
		m[t.Name] = t.Annotations
	}
}

// Canonicalize returns sorted {"arguments":…,"name":…} with _meta removed.
func Canonicalize(params json.RawMessage) ([]byte, string, error) {
	if len(params) == 0 {
		params = []byte(`{}`)
	}
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, "", err
	}
	if p.Name == "" {
		return nil, "", fmt.Errorf("missing tool name")
	}
	args := p.Arguments
	if len(args) == 0 {
		args = []byte(`{}`)
	}
	body := []byte(`{"arguments":` + string(args) + `,"name":` + jsonString(p.Name) + `}`)
	out, err := canon.Canonical(body, canon.Rules{})
	return out, p.Name, err
}

func injectKey(params json.RawMessage, key string) (json.RawMessage, error) {
	var m map[string]any
	if len(params) == 0 {
		m = map[string]any{}
	} else if err := json.Unmarshal(params, &m); err != nil {
		return nil, err
	}
	meta, _ := m["_meta"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta[MetaKey] = key
	m["_meta"] = meta
	return json.Marshal(m)
}

func stripID(b []byte) []byte {
	var env map[string]json.RawMessage
	if json.Unmarshal(b, &env) != nil {
		return b
	}
	delete(env, "id")
	out, err := json.Marshal(env)
	if err != nil {
		return b
	}
	return out
}

func rewriteID(body []byte, id json.RawMessage) []byte {
	var env map[string]json.RawMessage
	if json.Unmarshal(body, &env) != nil {
		return body
	}
	if len(id) == 0 {
		id = []byte("null")
	}
	env["id"] = id
	if _, ok := env["jsonrpc"]; !ok {
		env["jsonrpc"] = []byte(`"2.0"`)
	}
	out, err := json.Marshal(env)
	if err != nil {
		return body
	}
	return out
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

type rpcWriter struct {
	w      http.ResponseWriter
	id     json.RawMessage
	wrote  bool
	onBody func([]byte)
}

func (w *rpcWriter) Head(status int, ct string, replay bool) {
	if replay {
		w.w.Header().Set("X-Wirejournal-Replay", "1")
	}
}

func (w *rpcWriter) Frame([]byte) {}

func (w *rpcWriter) Finish(r *run.Response) {
	if w.onBody != nil {
		w.onBody(r.Body)
	}
	body := r.Body
	if r.Status >= 400 {
		w.w.Header().Set("Content-Type", "application/json")
		w.wrote = true
		w.w.WriteHeader(r.Status)
		_, _ = w.w.Write(body)
		return
	}
	writeRPC(w.w, w.id, nil, rewriteID(body, w.id))
	w.wrote = true
}

func writeRPC(w http.ResponseWriter, id json.RawMessage, result, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	if raw != nil {
		_, _ = w.Write(raw)
		return
	}
	if len(id) == 0 {
		id = []byte("null")
	}
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(id) + `,"result":` + string(result) + `}`))
}
