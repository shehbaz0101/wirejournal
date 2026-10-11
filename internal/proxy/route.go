// Package proxy is the HTTP surface: URL layout, the process-wide server,
// and the status endpoint. Protocol details live in llmproxy and mcpgw.
package proxy

import "strings"

// Target is a request addressed at one run.
type Target struct {
	Run, Attempt, Path string
}

// Parse splits /r/{run}/{path} and /r/{run}/a/{attempt}/{path}.
func Parse(path string) (Target, bool) {
	p := strings.TrimPrefix(path, "/")
	parts := strings.Split(p, "/")
	if len(parts) < 3 || parts[0] != "r" || parts[1] == "" {
		return Target{}, false
	}
	run, rest := parts[1], parts[2:]
	attempt := ""
	if rest[0] == "a" {
		if len(rest) < 3 || rest[1] == "" {
			return Target{}, false
		}
		attempt = rest[1]
		rest = rest[2:]
	}
	for _, s := range rest {
		if s == "" {
			return Target{}, false
		}
	}
	return Target{Run: run, Attempt: attempt, Path: strings.Join(rest, "/")}, true
}
