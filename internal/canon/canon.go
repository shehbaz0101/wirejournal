// Package canon turns a JSON request into the bytes wirejournal hashes, and
// finds where two such requests first differ.
package canon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Rules say which parts of a request do not identify the call.
type Rules struct {
	// Drop lists top-level fields to delete, such as "user" or "metadata".
	Drop []string
	// Masks replace every match in every string leaf with a fixed token.
	Masks []*regexp.Regexp
}

const maskToken = "\x00masked"

// Canonical decodes body, applies r, and re-encodes it with sorted keys, no
// insignificant whitespace, and numbers kept exactly as written.
func Canonical(body []byte, r Rules) ([]byte, error) {
	v, err := decode(body)
	if err != nil {
		return nil, err
	}
	if obj, ok := v.(map[string]any); ok {
		for _, f := range r.Drop {
			delete(obj, f)
		}
	}
	if len(r.Masks) > 0 {
		v = mask(v, r.Masks)
	}
	return encode(v), nil
}

func decode(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("canon: %w", err)
	}
	if len(bytes.TrimSpace(b[dec.InputOffset():])) > 0 {
		return nil, errors.New("canon: trailing data after JSON value")
	}
	return v, nil
}

func mask(v any, masks []*regexp.Regexp) any {
	switch x := v.(type) {
	case string:
		for _, m := range masks {
			x = m.ReplaceAllLiteralString(x, maskToken)
		}
		return x
	case []any:
		for i := range x {
			x[i] = mask(x[i], masks)
		}
	case map[string]any:
		for k := range x {
			x[k] = mask(x[k], masks)
		}
	}
	return v
}

func encode(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic("canon: re-encoding a decoded value failed: " + err.Error())
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// Diff is the first place two documents differ.
type Diff struct {
	Pointer string // RFC 6901 JSON pointer; "" is the root
	A, B    string // short JSON excerpts; "(absent)" when one side lacks the key
}

const maxExcerpt = 160

// FirstDiff walks a and b in canonical order (object keys sorted, arrays by
// index) and returns the first difference. ok is false when they are equal
// or either is not JSON.
func FirstDiff(a, b []byte) (d Diff, ok bool) {
	va, err := decode(a)
	if err != nil {
		return Diff{}, false
	}
	vb, err := decode(b)
	if err != nil {
		return Diff{}, false
	}
	return firstDiff("", va, vb, true, true)
}

func firstDiff(ptr string, a, b any, hasA, hasB bool) (Diff, bool) {
	if !hasA || !hasB {
		return Diff{ptr, excerpt(a, hasA), excerpt(b, hasB)}, true
	}
	switch x := a.(type) {
	case map[string]any:
		y, same := b.(map[string]any)
		if !same {
			break
		}
		keys := make([]string, 0, len(x)+len(y))
		for k := range x {
			keys = append(keys, k)
		}
		for k := range y {
			if _, dup := x[k]; !dup {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			xa, okA := x[k]
			yb, okB := y[k]
			if d, ok := firstDiff(ptr+"/"+escape(k), xa, yb, okA, okB); ok {
				return d, true
			}
		}
		return Diff{}, false
	case []any:
		y, same := b.([]any)
		if !same {
			break
		}
		for i := 0; i < max(len(x), len(y)); i++ {
			var xa, yb any
			okA, okB := i < len(x), i < len(y)
			if okA {
				xa = x[i]
			}
			if okB {
				yb = y[i]
			}
			if d, ok := firstDiff(ptr+"/"+strconv.Itoa(i), xa, yb, okA, okB); ok {
				return d, true
			}
		}
		return Diff{}, false
	}
	if bytes.Equal(encode(a), encode(b)) {
		return Diff{}, false
	}
	return Diff{ptr, excerpt(a, true), excerpt(b, true)}, true
}

func escape(k string) string {
	return strings.ReplaceAll(strings.ReplaceAll(k, "~", "~0"), "/", "~1")
}

func excerpt(v any, present bool) string {
	if !present {
		return "(absent)"
	}
	s := string(encode(v))
	if len(s) > maxExcerpt {
		return s[:maxExcerpt] + "…"
	}
	return s
}
