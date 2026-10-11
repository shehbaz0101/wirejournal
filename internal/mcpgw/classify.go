package mcpgw

import "github.com/shehbaz0101/wirejournal/internal/run"

// Rule is an optional config override for one tool name.
type Rule struct {
	Class    run.Class
	HonorKey bool
}

// Annotations is the subset of MCP ToolAnnotations that affects class.
// A nil pointer means the field was absent, which is not the same as false.
type Annotations struct {
	ReadOnlyHint    *bool `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool `json:"idempotentHint,omitempty"`
}

// Classify: config wins, then readOnlyHint, then idempotentHint, else
// side-effect (the MCP spec default).
func Classify(name string, ann Annotations, rules map[string]Rule) (run.Class, bool) {
	if r, ok := rules[name]; ok && r.Class != 0 {
		return r.Class, r.HonorKey
	}
	if ann.ReadOnlyHint != nil && *ann.ReadOnlyHint {
		return run.ClassReadOnly, false
	}
	if ann.IdempotentHint != nil && *ann.IdempotentHint {
		return run.ClassIdempotent, false
	}
	if r, ok := rules[name]; ok {
		return run.ClassSideEffect, r.HonorKey
	}
	return run.ClassSideEffect, false
}

func boolPtr(v bool) *bool { return &v }
