package agent

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/llmproxy"
	"github.com/shehbaz0101/wirejournal/internal/mcpgw"
	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/proxy"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

func TestAgentThroughProxyReplaysWithoutRebilling(t *testing.T) {
	t.Setenv("WJ_STEPS", "5")
	llmS := &mockllm.Server{}
	mailS := &mockmail.Server{}
	llm := httptest.NewServer(llmS)
	mail := httptest.NewServer(mailS)
	t.Cleanup(llm.Close)
	t.Cleanup(mail.Close)
	st, err := run.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	lu, _ := url.Parse(llm.URL)
	mu, _ := url.Parse(mail.URL)
	px := httptest.NewServer(&proxy.Server{
		Store: st,
		LLM:   &llmproxy.Handler{Upstream: lu},
		MCP:   &mcpgw.Handler{Store: st, Upstream: mu, Rules: map[string]mcpgw.Rule{"send_email": {Class: run.ClassSideEffect, HonorKey: true}}},
	})
	t.Cleanup(px.Close)

	res, err := Run(Config{LLM: px.URL + "/r/job/a/1/v1", MCP: px.URL + "/r/job/a/1/mcp", Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Emails != 1 || mailS.Stats().Deliveries != 1 {
		t.Fatalf("first run res=%+v deliveries=%d", res, mailS.Stats().Deliveries)
	}
	billed := llmS.Stats().Calls
	res2, err := Run(Config{LLM: px.URL + "/r/job/a/2/v1", MCP: px.URL + "/r/job/a/2/mcp", Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Emails != 1 || mailS.Stats().Deliveries != 1 {
		t.Fatalf("replay duplicated email: res=%+v deliveries=%d", res2, mailS.Stats().Deliveries)
	}
	if llmS.Stats().Calls != billed {
		t.Fatalf("replay re-billed: %d -> %d", billed, llmS.Stats().Calls)
	}
}
