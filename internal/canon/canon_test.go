package canon

import (
	"regexp"
	"strings"
	"testing"
)

func mustCanon(t *testing.T, r Rules, in string) string {
	t.Helper()
	out, err := Canonical([]byte(in), r)
	if err != nil {
		t.Fatalf("Canonical(%s): %v", in, err)
	}
	return string(out)
}

func TestCanonicalIgnoresKeyOrderAndWhitespace(t *testing.T) {
	a := mustCanon(t, Rules{}, `{"b": 1, "a": {"y": [1, 2], "x": "s"}}`)
	b := mustCanon(t, Rules{}, "{\"a\":{\"x\":\"s\",\n\"y\":[1,2]},\"b\":1}")
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
	if a != `{"a":{"x":"s","y":[1,2]},"b":1}` {
		t.Fatalf("canonical form = %s", a)
	}
}

func TestCanonicalKeepsNumbersExact(t *testing.T) {
	got := mustCanon(t, Rules{}, `{"seed": 12345678901234567890, "t": 0.70}`)
	if got != `{"seed":12345678901234567890,"t":0.70}` {
		t.Fatalf("numbers changed: %s", got)
	}
}

func TestCanonicalDoesNotEscapeHTML(t *testing.T) {
	if got := mustCanon(t, Rules{}, `{"q":"a<b && c>d"}`); got != `{"q":"a<b && c>d"}` {
		t.Fatalf("got %s", got)
	}
}

func TestCanonicalDropsTopLevelFields(t *testing.T) {
	r := Rules{Drop: []string{"user", "metadata"}}
	a := mustCanon(t, r, `{"model":"m","user":"u-1","metadata":{"trace":"abc"}}`)
	b := mustCanon(t, r, `{"model":"m","user":"u-2"}`)
	if a != b || a != `{"model":"m"}` {
		t.Fatalf("a=%s b=%s", a, b)
	}
	nested := mustCanon(t, r, `{"model":"m","messages":[{"user":"kept"}]}`)
	if nested != `{"messages":[{"user":"kept"}],"model":"m"}` {
		t.Fatalf("dropped a nested field: %s", nested)
	}
}

func TestCanonicalMasksEveryStringLeaf(t *testing.T) {
	r := Rules{Masks: []*regexp.Regexp{regexp.MustCompile(`\d{4}-\d{2}-\d{2}T[0-9:.]+Z`)}}
	a := mustCanon(t, r, `{"messages":[{"content":"now is 2026-10-11T03:00:00Z, go"}],"k":"2026-01-01T00:00:00Z"}`)
	b := mustCanon(t, r, `{"messages":[{"content":"now is 2027-01-02T09:30:15.123Z, go"}],"k":"1999-12-31T23:59:59Z"}`)
	if a != b {
		t.Fatalf("masked forms differ:\n%s\n%s", a, b)
	}
}

func TestCanonicalRejectsInvalidJSON(t *testing.T) {
	for _, in := range []string{``, `{`, `{"a":1} trailing`, `{"a":1}{"b":2}`, `{"a":1}}`} {
		if _, err := Canonical([]byte(in), Rules{}); err == nil {
			t.Errorf("Canonical(%q) accepted invalid JSON", in)
		}
	}
}

func TestFirstDiff(t *testing.T) {
	cases := []struct {
		name, a, b, ptr, av, bv string
	}{
		{"equal", `{"a":[1,2]}`, `{"a":[1,2]}`, "", "", ""},
		{"nested leaf", `{"m":[{"c":"hi"},{"c":"old"}]}`, `{"m":[{"c":"hi"},{"c":"new"}]}`, "/m/1/c", `"old"`, `"new"`},
		{"missing key", `{"a":1,"b":2}`, `{"a":1}`, "/b", `2`, `(absent)`},
		{"extra key", `{"a":1}`, `{"a":1,"z":true}`, "/z", `(absent)`, `true`},
		{"longer array", `{"m":[1]}`, `{"m":[1,2]}`, "/m/1", `(absent)`, `2`},
		{"type change", `{"a":{"x":1}}`, `{"a":[1]}`, "/a", `{"x":1}`, `[1]`},
		{"escaped pointer", `{"a/b":{"c~d":1}}`, `{"a/b":{"c~d":2}}`, "/a~1b/c~0d", `1`, `2`},
		{"first in key order", `{"b":1,"a":1}`, `{"b":2,"a":2}`, "/a", `1`, `2`},
		{"root", `1`, `2`, "", `1`, `2`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ok := FirstDiff([]byte(c.a), []byte(c.b))
			if c.name == "equal" {
				if ok {
					t.Fatalf("equal documents reported a diff: %+v", d)
				}
				return
			}
			if !ok {
				t.Fatal("no diff reported")
			}
			if d.Pointer != c.ptr || d.A != c.av || d.B != c.bv {
				t.Fatalf("diff = %+v, want pointer %q a=%s b=%s", d, c.ptr, c.av, c.bv)
			}
		})
	}
}

func TestFirstDiffTruncatesExcerpts(t *testing.T) {
	long := `"` + strings.Repeat("x", 500) + `"`
	d, ok := FirstDiff([]byte(`{"a":`+long+`}`), []byte(`{"a":"y"}`))
	if !ok || len(d.A) > maxExcerpt+len("…") {
		t.Fatalf("excerpt not truncated: %d bytes", len(d.A))
	}
}
