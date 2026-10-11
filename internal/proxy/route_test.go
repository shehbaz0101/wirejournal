package proxy

import "testing"

func TestParse(t *testing.T) {
	ok := []struct {
		in, run, attempt, path string
	}{
		{"/r/job-42/v1/chat/completions", "job-42", "", "v1/chat/completions"},
		{"/r/job-42/a/uuid-1/v1/chat/completions", "job-42", "uuid-1", "v1/chat/completions"},
		{"/r/job-42/status", "job-42", "", "status"},
		{"/r/job-42/a/x/mcp", "job-42", "x", "mcp"},
		{"/r/a.b_c-1/mcp", "a.b_c-1", "", "mcp"},
	}
	for _, c := range ok {
		g, good := Parse(c.in)
		if !good || g.Run != c.run || g.Attempt != c.attempt || g.Path != c.path {
			t.Errorf("Parse(%q) = %+v, %v", c.in, g, good)
		}
	}
	for _, in := range []string{"/", "/v1/chat", "/r/", "/r/job", "/r/job/", "/r/job/a/", "/r/job/a/x", "/r//v1", "/r/job/v1//x"} {
		if _, good := Parse(in); good {
			t.Errorf("Parse(%q) accepted", in)
		}
	}
}
