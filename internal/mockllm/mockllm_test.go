package mockllm

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/script"
)

func TestChatAndStats(t *testing.T) {
	s := &Server{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	body := `{"model":"m","messages":[{"role":"system"},{"role":"user"}]}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "search") {
		t.Fatalf("step0 = %s", b)
	}
	body = `{"model":"m","messages":[{"role":"system"},{"role":"user"},{"role":"assistant"},{"role":"tool"}]}`
	resp, err = http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(b), `"id":"s0"`) {
		t.Fatal("second turn reused step 0")
	}
	resp, err = http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var st Stats
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	p0, c0 := script.Usage(0)
	p1, c1 := script.Usage(1)
	if st.Calls != 2 || st.PromptTokens != p0+p1 || st.CompletionTokens != c0+c1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestStreamEndsWithDONE(t *testing.T) {
	s := &Server{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "data: [DONE]") {
		t.Fatalf("%s", b)
	}
}
