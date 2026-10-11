package mcpgw

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/proxy"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

type mailStub struct {
	inits    atomic.Int64
	lists    atomic.Int64
	sends    atomic.Int64
	lastKey  string
	lastMeta string
}

func (m *mailStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var req rpcRequest
	_ = json.Unmarshal(b, &req)
	switch req.Method {
	case "initialize":
		m.inits.Add(1)
		w.Header().Set("Mcp-Session-Id", "up-1")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"mail","version":"0"}}}`)
	case "tools/list":
		m.lists.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"search","annotations":{"readOnlyHint":true}},{"name":"send_email","annotations":{"readOnlyHint":false,"idempotentHint":false}}]}}`)
	case "tools/call":
		m.sends.Add(1)
		m.lastKey = r.Header.Get("Idempotency-Key")
		var p struct {
			Name string            `json:"name"`
			Meta map[string]string `json:"_meta"`
		}
		_ = json.Unmarshal(req.Params, &p)
		m.lastMeta = p.Meta[MetaKey]
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"ok"}],"isError":false}}`)
	default:
		http.NotFound(w, r)
	}
}

func mcpEnv(t *testing.T, rules map[string]Rule) (*httptest.Server, *mailStub, *run.Store) {
	t.Helper()
	stub := &mailStub{}
	up := httptest.NewServer(stub)
	t.Cleanup(up.Close)
	st, err := run.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	u, _ := url.Parse(up.URL)
	h := &Handler{Store: st, Upstream: u, Rules: rules}
	srv := httptest.NewServer(&proxy.Server{Store: st, MCP: h})
	t.Cleanup(srv.Close)
	return srv, stub, st
}

func postMCP(t *testing.T, srv *httptest.Server, path, body string) (int, string, http.Header) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b), resp.Header
}

func TestInitializeIsLocal(t *testing.T) {
	srv, stub, _ := mcpEnv(t, nil)
	st, body, h := postMCP(t, srv, "/r/job/mcp", `{"jsonrpc":"2.0","id":7,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	if st != 200 || !strings.Contains(body, `"id":7`) || !strings.Contains(body, "wirejournal") || h.Get("Mcp-Session-Id") == "" {
		t.Fatalf("%d %s %v", st, body, h)
	}
	if stub.inits.Load() != 0 {
		t.Fatal("initialize was forwarded upstream")
	}
}

func TestToolsListReplayAndAnnotations(t *testing.T) {
	srv, stub, _ := mcpEnv(t, nil)
	list := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	st, a, _ := postMCP(t, srv, "/r/job/a/1/mcp", list)
	if st != 200 || !strings.Contains(a, "send_email") {
		t.Fatalf("list: %d %s", st, a)
	}
	st, b, h := postMCP(t, srv, "/r/job/a/2/mcp", `{"jsonrpc":"2.0","id":99,"method":"tools/list","params":{}}`)
	if st != 200 || h.Get("X-Wirejournal-Replay") != "1" || !strings.Contains(b, `"id":99`) || !strings.Contains(b, "send_email") {
		t.Fatalf("replay: %d %s %v", st, b, h)
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("lists = %d", stub.lists.Load())
	}
}

func TestToolsCallAtMostOnceAndRewritesID(t *testing.T) {
	srv, stub, _ := mcpEnv(t, map[string]Rule{"send_email": {Class: run.ClassSideEffect, HonorKey: true}})
	postMCP(t, srv, "/r/job/a/1/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	call := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"send_email","arguments":{"to":"a@b"}}}`
	st, body, _ := postMCP(t, srv, "/r/job/a/1/mcp", call)
	if st != 200 || !strings.Contains(body, `"id":2`) || stub.sends.Load() != 1 {
		t.Fatalf("live: %d %s sends=%d", st, body, stub.sends.Load())
	}
	if stub.lastKey == "" || stub.lastKey != stub.lastMeta || !strings.HasPrefix(stub.lastKey, "wj1-") {
		t.Fatalf("key header=%q meta=%q", stub.lastKey, stub.lastMeta)
	}
	st, body, h := postMCP(t, srv, "/r/job/a/2/mcp", `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"send_email","arguments":{"to":"a@b"}}}`)
	if st != 200 || h.Get("X-Wirejournal-Replay") != "1" || !strings.Contains(body, `"id":8`) || stub.sends.Load() != 1 {
		t.Fatalf("replay: %d %s sends=%d %v", st, body, stub.sends.Load(), h)
	}
}

func TestReadOnlyToolUsesAnnotation(t *testing.T) {
	srv, stub, _ := mcpEnv(t, nil)
	postMCP(t, srv, "/r/job/a/1/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	search := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search","arguments":{"q":"x"}}}`
	postMCP(t, srv, "/r/job/a/1/mcp", search)
	postMCP(t, srv, "/r/job/a/2/mcp", search)
	if stub.sends.Load() != 1 {
		t.Fatalf("search hits %d", stub.sends.Load())
	}
}

func TestCanonicalizeDropsMeta(t *testing.T) {
	a, n, err := Canonicalize([]byte(`{"name":"send_email","arguments":{"to":"a"},"_meta":{"x":1}}`))
	if err != nil || n != "send_email" {
		t.Fatal(err)
	}
	b, _, _ := Canonicalize([]byte(`{"arguments":{"to":"a"},"name":"send_email"}`))
	if string(a) != string(b) || strings.Contains(string(a), "_meta") {
		t.Fatalf("%s vs %s", a, b)
	}
}

func TestBlockedCallIsMCPErrorNotHTTPError(t *testing.T) {
	w := httptest.NewRecorder()
	writeRPC(w, []byte("3"), []byte(`{"content":[{"type":"text","text":"no"}],"isError":true}`), nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"id":3`) || !strings.Contains(w.Body.String(), `"isError":true`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
