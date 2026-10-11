package proxy

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shehbaz0101/wirejournal/internal/run"
)

func TestLoadConfig(t *testing.T) {
	p := filepath.Join(t.TempDir(), "wirejournal.json")
	os.WriteFile(p, []byte(`{"canonical":{"drop_fields":["user"],"masks":["abc"]},"tools":{"send_email":{"class":"side_effect","idempotency_key":true}}}`), 0o644)
	rules, tools, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules.Drop) != 1 || rules.Drop[0] != "user" || len(rules.Masks) != 1 {
		t.Fatalf("rules = %+v", rules)
	}
	if tools["send_email"].Class != run.ClassSideEffect || !tools["send_email"].HonorKey {
		t.Fatalf("tools = %+v", tools)
	}
}
