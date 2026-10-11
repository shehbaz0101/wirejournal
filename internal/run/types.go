// Package run owns replay semantics: call identity, attempts, and every
// commit decision. Protocol adapters hand it canonical requests through
// Run.Do and never touch the journal.
package run

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

// Kind separates call namespaces. It is part of the hash input.
type Kind string

const (
	KindLLM  Kind = "llm"
	KindTool Kind = "tool"
	KindMCP  Kind = "mcp"
)

// Class is how a tool is treated at the ambiguous window.
type Class uint8

const (
	ClassReadOnly   Class = 1
	ClassIdempotent Class = 2
	ClassSideEffect Class = 3
)

// Call is one canonicalized request.
type Call struct {
	Kind      Kind
	Canonical []byte
	// Label names the call in reports: the model or the tool.
	Label string
	Class Class
	// HonorKey means the tool server dedupes on the derived idempotency key.
	HonorKey bool
}

type Usage struct {
	Prompt     int `json:"prompt_tokens"`
	Completion int `json:"completion_tokens"`
}

func (u Usage) Total() int { return u.Prompt + u.Completion }

// Response is exactly what the client sees, so it is what gets journaled and
// replayed. A streamed response is Frames followed by Tail; Tail is withheld
// from a live client until the response is durable.
type Response struct {
	Status      int      `json:"status"`
	ContentType string   `json:"ctype,omitempty"`
	Body        []byte   `json:"body,omitempty"`
	Frames      [][]byte `json:"frames,omitempty"`
	Tail        []byte   `json:"tail,omitempty"`
	Usage       Usage    `json:"usage"`
}

// Emitter receives a live response as it streams from upstream.
type Emitter interface {
	Head(status int, contentType string)
	Frame(p []byte)
}

// Writer delivers a response to the client. Do calls Head, then Frame zero
// or more times, then Finish. For a live call Finish comes only after the
// response is durable. Writers ignore errors from a client that has gone.
type Writer interface {
	Head(status int, contentType string, replay bool)
	Frame(p []byte)
	Finish(r *Response)
}

// Upstream performs a live call. It must emit streamed frames as they arrive
// and return the complete response. An error means no complete response
// arrived. ctx is detached from the client so a dying agent does not abort
// a call the next attempt will want.
type Upstream func(ctx context.Context, idempotencyKey string, e Emitter) (*Response, error)

type Outcome struct {
	Replay  bool
	Blocked bool
	Message string
}

var (
	ErrUpstream      = errors.New("run: upstream failed")
	ErrJournal       = errors.New("run: journal write failed")
	ErrCorruptRecord = errors.New("run: corrupt journal record")
	ErrBadRunID      = errors.New("run: invalid run id")
	ErrNoRun         = errors.New("run: no such run")
)

// Status is the read model behind GET /r/{run}/status.
type Status struct {
	Run         string          `json:"run"`
	Recorded    int             `json:"recorded_calls"`
	Quarantined bool            `json:"quarantined"`
	Ambiguous   []AmbiguousCall `json:"ambiguous,omitempty"`
	Attempts    []AttemptStatus `json:"attempts"`
}

type AmbiguousCall struct {
	Kind  Kind   `json:"kind"`
	Label string `json:"label"`
	K     int    `json:"k"`
}

type AttemptStatus struct {
	N         int       `json:"n"`
	ID        string    `json:"id,omitempty"`
	StartedAt time.Time `json:"started_at"`
	// Expected is the number of recorded model calls this attempt started
	// with; replaying all of them means it caught up to the crash point.
	Expected     int `json:"expected"`
	Replayed     int `json:"replayed"`
	Live         int `json:"live"`
	TokensSaved  int `json:"tokens_saved"`
	TokensBilled int `json:"tokens_billed"`
	// LiveAfterMS is the time from attempt start to its first live model
	// call: how long the fast-forward took.
	LiveAfterMS *float64    `json:"live_after_ms,omitempty"`
	Divergence  *Divergence `json:"divergence,omitempty"`
}

// Divergence describes the first model request in an attempt that matched
// nothing while recorded calls were still waiting to be replayed.
type Divergence struct {
	Step     int       `json:"step"`
	Label    string    `json:"label"`
	Pointer  string    `json:"pointer"`
	Recorded string    `json:"recorded"`
	Got      string    `json:"got"`
	At       time.Time `json:"at"`
}

const (
	recCommit = "commit"
	recIntent = "intent"
)

// record is the journal payload. Response bodies are base64 inside JSON.
type record struct {
	T        string    `json:"t"`
	Kind     Kind      `json:"kind"`
	Hash     string    `json:"hash"`
	K        int       `json:"k"`
	A        int       `json:"a"`
	Label    string    `json:"label,omitempty"`
	Canon    []byte    `json:"canon,omitempty"`
	Class    Class     `json:"class,omitempty"`
	HonorKey bool      `json:"honor,omitempty"`
	Resp     *Response `json:"resp,omitempty"`
	At       time.Time `json:"at"`
}

func hashOf(k Kind, canonical []byte) string {
	h := sha256.New()
	h.Write([]byte(k))
	h.Write([]byte{0})
	h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil))
}
