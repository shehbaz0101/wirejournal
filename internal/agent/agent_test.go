package agent

import (
	"net/http/httptest"
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/script"
)

func TestAgentCompletesAgainstMocks(t *testing.T) {
	t.Setenv("WJ_STEPS", "5")
	if script.Steps() != 5 {
		t.Fatalf("steps = %d", script.Steps())
	}
	llm := httptest.NewServer(&mockllm.Server{})
	mail := httptest.NewServer(&mockmail.Server{})
	t.Cleanup(llm.Close)
	t.Cleanup(mail.Close)

	res, err := Run(Config{LLM: llm.URL + "/v1", MCP: mail.URL, Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.ModelCalls != 5 || res.Emails != 1 || res.ToolCalls != 4 {
		t.Fatalf("result = %+v", res)
	}
}

func TestAgentUnaryAlsoWorks(t *testing.T) {
	t.Setenv("WJ_STEPS", "3")
	llmS := &mockllm.Server{}
	mailS := &mockmail.Server{}
	llm := httptest.NewServer(llmS)
	mail := httptest.NewServer(mailS)
	t.Cleanup(llm.Close)
	t.Cleanup(mail.Close)
	res, err := Run(Config{LLM: llm.URL + "/v1", MCP: mail.URL, Stream: false})
	if err != nil {
		t.Fatal(err)
	}
	if res.Emails != 1 || mailS.Stats().Deliveries != 1 {
		t.Fatalf("res=%+v inbox=%d", res, mailS.Stats().Deliveries)
	}
	if llmS.Stats().Calls != 3 {
		t.Fatalf("billed %d", llmS.Stats().Calls)
	}
}
