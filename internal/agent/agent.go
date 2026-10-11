// Package agent is an SDK-free ticket-triage loop. It speaks OpenAI chat
// completions and MCP Streamable HTTP over net/http.
package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/script"
)

type Config struct {
	LLM, MCP string
	Auth     string
	Stream   bool
	Client   *http.Client
}

type Result struct {
	ModelCalls int
	ToolCalls  int
	Emails     int
	LastText   string
}

func (c Config) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return &http.Client{Timeout: 2 * time.Minute}
}

func Run(cfg Config) (Result, error) {
	if err := mcpInit(cfg); err != nil {
		return Result{}, err
	}
	msgs := []map[string]any{
		{"role": "system", "content": script.System},
		{"role": "user", "content": script.User},
	}
	var res Result
	for i := 0; i < script.Steps()+2; i++ {
		asst, err := complete(cfg, msgs)
		if err != nil {
			return res, err
		}
		res.ModelCalls++
		msgs = append(msgs, asst)
		calls, _ := asst["tool_calls"].([]any)
		if len(calls) == 0 {
			if s, _ := asst["content"].(string); s != "" {
				res.LastText = s
			}
			return res, nil
		}
		for _, raw := range calls {
			tc, _ := raw.(map[string]any)
			fn, _ := tc["function"].(map[string]any)
			name, _ := fn["name"].(string)
			args, _ := fn["arguments"].(string)
			id, _ := tc["id"].(string)
			text, err := callTool(cfg, name, args)
			if err != nil {
				return res, err
			}
			res.ToolCalls++
			if name == "send_email" {
				res.Emails++
			}
			msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": id, "content": text})
		}
	}
	return res, fmt.Errorf("agent: exceeded %d model calls", script.Steps())
}

func complete(cfg Config, msgs []map[string]any) (map[string]any, error) {
	body, _ := json.Marshal(map[string]any{"model": "mock", "stream": cfg.Stream, "messages": msgs})
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(cfg.LLM, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Auth != "" {
		req.Header.Set("Authorization", cfg.Auth)
	}
	resp, err := cfg.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("llm %d: %s", resp.StatusCode, raw)
	}
	if cfg.Stream || strings.Contains(resp.Header.Get("Content-Type"), "event-stream") {
		return parseSSE(raw)
	}
	var env struct {
		Choices []struct {
			Message map[string]any `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || len(env.Choices) == 0 {
		return nil, fmt.Errorf("llm decode: %w", err)
	}
	return env.Choices[0].Message, nil
}

func parseSSE(raw []byte) (map[string]any, error) {
	var content strings.Builder
	type call struct{ id, name, args string }
	calls := map[int]*call{}
	for _, ev := range strings.Split(string(raw), "\n") {
		line := strings.TrimSpace(ev)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" || data == "" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil || len(chunk.Choices) == 0 {
			continue
		}
		d := chunk.Choices[0].Delta
		content.WriteString(d.Content)
		for _, tc := range d.ToolCalls {
			c := calls[tc.Index]
			if c == nil {
				c = &call{}
				calls[tc.Index] = c
			}
			if tc.ID != "" {
				c.id = tc.ID
			}
			if tc.Function.Name != "" {
				c.name = tc.Function.Name
			}
			c.args += tc.Function.Arguments
		}
	}
	msg := map[string]any{"role": "assistant", "content": content.String()}
	if len(calls) == 0 {
		return msg, nil
	}
	var list []any
	for i := 0; i < len(calls); i++ {
		c := calls[i]
		list = append(list, map[string]any{
			"id": c.id, "type": "function",
			"function": map[string]any{"name": c.name, "arguments": c.args},
		})
	}
	msg["tool_calls"] = list
	return msg, nil
}

func mcpInit(cfg Config) error {
	_, err := mcp(cfg, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"wj-agent","version":"0"}}}`)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, cfg.MCP, strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cfg.client().Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	_, err = mcp(cfg, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	return err
}

func callTool(cfg Config, name, args string) (string, error) {
	if args == "" {
		args = "{}"
	}
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, name, args)
	raw, err := mcp(cfg, payload)
	if err != nil {
		return "", err
	}
	var env struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", err
	}
	if env.Error != nil {
		return "", fmt.Errorf("tool %s: %s", name, env.Error.Message)
	}
	if len(env.Result.Content) == 0 {
		return "", nil
	}
	return env.Result.Content[0].Text, nil
}

func mcp(cfg Config, body string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, cfg.MCP, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := cfg.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("mcp %d: %s", resp.StatusCode, raw)
	}
	return raw, nil
}
