package mcpgw

import (
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/run"
)

func TestClassify(t *testing.T) {
	yes, no := boolPtr(true), boolPtr(false)
	rules := map[string]Rule{
		"send_email": {Class: run.ClassSideEffect, HonorKey: true},
		"forced_ro":  {Class: run.ClassReadOnly},
	}
	cases := []struct {
		name  string
		tool  string
		ann   Annotations
		class run.Class
		honor bool
	}{
		{"config wins over side-effect annotations", "send_email", Annotations{ReadOnlyHint: yes}, run.ClassSideEffect, true},
		{"config class override", "forced_ro", Annotations{}, run.ClassReadOnly, false},
		{"readOnlyHint", "search", Annotations{ReadOnlyHint: yes}, run.ClassReadOnly, false},
		{"idempotentHint", "put", Annotations{IdempotentHint: yes}, run.ClassIdempotent, false},
		{"readOnly beats idempotent", "x", Annotations{ReadOnlyHint: yes, IdempotentHint: yes}, run.ClassReadOnly, false},
		{"absent is side-effect", "mail", Annotations{}, run.ClassSideEffect, false},
		{"explicit false is side-effect", "mail", Annotations{ReadOnlyHint: no, IdempotentHint: no}, run.ClassSideEffect, false},
	}
	for _, c := range cases {
		got, honor := Classify(c.tool, c.ann, rules)
		if got != c.class || honor != c.honor {
			t.Errorf("%s: %v honor=%v, want %v honor=%v", c.name, got, honor, c.class, c.honor)
		}
	}
}
