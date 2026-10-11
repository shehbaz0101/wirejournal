// Package script is the shared ticket-triage conversation. The mock LLM
// answers from the request, not from a process counter, so a restarted mock
// still returns the right step.
package script

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const defaultSteps = 200

func Steps() int {
	if s := os.Getenv("WJ_STEPS"); s != "" {
		n, err := strconv.Atoi(s)
		if err == nil && n >= 3 && n <= 10_000 {
			return n
		}
	}
	return defaultSteps
}

func EmailStep() int { return Steps() - 2 }
func DoneStep() int  { return Steps() - 1 }

const (
	System = "You are a ticket triage agent. Investigate, then email the user once."
	User   = "Triage ticket #42: customer cannot log in after the password reset."
)

type ToolCall struct {
	Name string
	Args map[string]string
}

func StepOf(messages []map[string]any) int {
	n := 0
	for _, m := range messages {
		if m["role"] == "assistant" {
			n++
		}
	}
	return n
}

func Tool(step int) (ToolCall, bool) {
	if step == DoneStep() {
		return ToolCall{}, false
	}
	if step == EmailStep() {
		return ToolCall{Name: "send_email", Args: map[string]string{
			"to": "user@example.com", "subject": "Ticket #42",
			"body": "Your login is restored. Reset the password once more if it fails.",
		}}, true
	}
	switch step % 3 {
	case 0:
		return ToolCall{Name: "search", Args: map[string]string{"q": fmt.Sprintf("ticket-42 step %d auth logs", step)}}, true
	case 1:
		return ToolCall{Name: "get_ticket", Args: map[string]string{"id": "42"}}, true
	default:
		return ToolCall{Name: "search", Args: map[string]string{"q": fmt.Sprintf("password reset runbook %d", step)}}, true
	}
}

func Usage(step int) (prompt, completion int) {
	return 20 + 4*step, 8
}

func AssistantJSON(step int) []byte {
	tc, ok := Tool(step)
	p, c := Usage(step)
	if !ok {
		return []byte(fmt.Sprintf(`{"id":"s%d","choices":[{"message":{"role":"assistant","content":"Ticket #42 resolved. Emailed the customer."},"finish_reason":"stop"}],"usage":{"prompt_tokens":%d,"completion_tokens":%d}}`, step, p, c))
	}
	args, _ := json.Marshal(tc.Args)
	return []byte(fmt.Sprintf(`{"id":"s%d","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"c%d","type":"function","function":{"name":%q,"arguments":%q}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":%d,"completion_tokens":%d}}`, step, step, tc.Name, args, p, c))
}

func SSE(step int) [][]byte {
	tc, ok := Tool(step)
	p, c := Usage(step)
	if !ok {
		return [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Ticket #42 resolved. \"}}]}\n\n"),
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"Emailed the customer.\"}}]}\n\n"),
			[]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d}}\n\n", p, c)),
			[]byte("data: [DONE]\n\n"),
		}
	}
	args, _ := json.Marshal(tc.Args)
	esc, _ := json.Marshal(string(args))
	return [][]byte{
		[]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c%d\",\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":\"\"}}]}}]}\n\n", step, tc.Name)),
		[]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":%s}}]}}]}\n\n", esc)),
		[]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d}}\n\n", p, c)),
		[]byte("data: [DONE]\n\n"),
	}
}
