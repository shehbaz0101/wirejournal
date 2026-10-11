package proxy

import (
	"encoding/json"
	"os"
	"regexp"

	"github.com/shehbaz0101/wirejournal/internal/canon"
	"github.com/shehbaz0101/wirejournal/internal/mcpgw"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

type File struct {
	Canonical struct {
		DropFields []string `json:"drop_fields"`
		Masks      []string `json:"masks"`
	} `json:"canonical"`
	Tools map[string]struct {
		Class          string `json:"class"`
		IdempotencyKey bool   `json:"idempotency_key"`
	} `json:"tools"`
}

func LoadConfig(path string) (canon.Rules, map[string]mcpgw.Rule, error) {
	rules := canon.Rules{Drop: []string{"user", "metadata"}}
	tools := map[string]mcpgw.Rule{}
	if path == "" {
		return rules, tools, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return rules, tools, err
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return rules, tools, err
	}
	if len(f.Canonical.DropFields) > 0 {
		rules.Drop = f.Canonical.DropFields
	}
	for _, m := range f.Canonical.Masks {
		re, err := regexp.Compile(m)
		if err != nil {
			return rules, tools, err
		}
		rules.Masks = append(rules.Masks, re)
	}
	for name, t := range f.Tools {
		r := mcpgw.Rule{HonorKey: t.IdempotencyKey}
		switch t.Class {
		case "read_only":
			r.Class = run.ClassReadOnly
		case "idempotent":
			r.Class = run.ClassIdempotent
		case "side_effect":
			r.Class = run.ClassSideEffect
		}
		tools[name] = r
	}
	return rules, tools, nil
}
