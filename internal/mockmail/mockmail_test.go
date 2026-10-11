package mockmail

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rpc(t *testing.T, srv *httptest.Server, body string, hdr map[string]string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(b)
}

func TestDedupeAndList(t *testing.T) {
	s := &Server{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	rpc(t, srv, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`, nil)
	list := rpc(t, srv, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, nil)
	if !strings.Contains(list, "send_email") || !strings.Contains(list, "readOnlyHint") {
		t.Fatalf("list = %s", list)
	}
	call := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"send_email","arguments":{"to":"a@b","subject":"s","body":"x"}}}`
	rpc(t, srv, call, map[string]string{"Idempotency-Key": "k1"})
	rpc(t, srv, call, map[string]string{"Idempotency-Key": "k1"})
	rpc(t, srv, call, map[string]string{"Idempotency-Key": "k2"})
	st := s.Stats()
	if st.Deliveries != 2 {
		t.Fatalf("deliveries = %d, want 2", st.Deliveries)
	}
}
