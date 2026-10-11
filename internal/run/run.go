package run

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/canon"
	"github.com/shehbaz0101/wirejournal/internal/journal"
)

const upstreamTimeout = 10 * time.Minute

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Store maps run ids to open runs. Each run is one journal file in dir, held
// under an exclusive lock for as long as the Store is open.
type Store struct {
	dir  string
	mu   sync.Mutex
	runs map[string]*Run
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir, runs: map[string]*Run{}}, nil
}

// Run returns the open run for id, recovering it from its journal on first
// use. With create false, a run that has no journal yet is ErrNoRun.
func (s *Store) Run(id string, create bool) (*Run, error) {
	if !runIDPattern.MatchString(id) {
		return nil, fmt.Errorf("%w: %q", ErrBadRunID, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.runs[id]; r != nil {
		return r, nil
	}
	path := filepath.Join(s.dir, id+".wj")
	if !create {
		if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoRun
		}
	}
	r := newRun(id)
	l, _, err := journal.Open(path, r.apply)
	if err != nil {
		return nil, err
	}
	r.log = l
	s.runs[id] = r
	return r, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for id, r := range s.runs {
		errs = append(errs, r.log.Close())
		delete(s.runs, id)
	}
	return errors.Join(errs...)
}

// Open recovers a run from an arbitrary journal file, without locking.
func Open(id string, f journal.File) (*Run, error) {
	r := newRun(id)
	l, _, err := journal.OpenFile(f, r.apply)
	if err != nil {
		return nil, err
	}
	r.log = l
	return r, nil
}

type key struct {
	kind Kind
	hash string
	k    int
}

type occKey struct {
	kind Kind
	hash string
}

type state uint8

const (
	statePending state = iota + 1
	stateCommitted
	stateAmbiguous
)

type entry struct {
	key      key
	state    state
	canon    []byte
	label    string
	class    Class
	honor    bool
	resp     *Response
	origin   *attempt      // nil for entries recovered from the journal
	done     chan struct{} // closed when a pending entry commits or is abandoned
	intended bool
}

// attempt is one life of the agent process. Occurrence counters restart
// with every attempt.
type attempt struct {
	n       int
	id      string
	started time.Time
	occ     map[occKey]int
	// expected holds the recorded model calls this attempt should replay, in
	// commit order; remaining counts the ones it has not consumed yet.
	expected  []*entry
	isExp     map[*entry]bool
	consumed  map[*entry]bool
	remaining int
	// lineage is every model entry this attempt replayed or committed.
	lineage    []*entry
	sawFirst   bool
	sawOther   bool
	modelSteps int
	stats      AttemptStatus
}

// Run is one job's ledger. entries is both the replay cache and the
// at-most-once ledger: every committed call from every attempt stays
// matchable.
type Run struct {
	id  string
	log *journal.Log

	mu          sync.Mutex
	entries     map[key]*entry
	model       []*entry // committed model entries in commit order
	firstModel  string   // hash of the run's first model request
	lastAttempt int
	cur         *attempt
	attempts    []*attempt
	quarantined bool
}

func newRun(id string) *Run {
	return &Run{id: id, entries: map[key]*entry{}}
}

func (r *Run) Close() error { return r.log.Close() }

// ID is the durable run identity.
func (r *Run) ID() string { return r.id }

func corrupt(off int64, format string, args ...any) error {
	return fmt.Errorf("%w at offset %d: %s", ErrCorruptRecord, off, fmt.Sprintf(format, args...))
}

// apply folds one journal record into the run during recovery.
func (r *Run) apply(off int64, payload []byte) error {
	var rec record
	if err := json.Unmarshal(payload, &rec); err != nil {
		return corrupt(off, "undecodable: %v", err)
	}
	if rec.K < 1 || rec.A < 1 {
		return corrupt(off, "occurrence %d, attempt %d", rec.K, rec.A)
	}
	k := key{rec.Kind, rec.Hash, rec.K}
	switch rec.T {
	case recIntent:
		if rec.Kind != KindTool || rec.Class != ClassSideEffect {
			return corrupt(off, "intent for %s class %d", rec.Kind, rec.Class)
		}
		if hashOf(rec.Kind, rec.Canon) != rec.Hash {
			return corrupt(off, "hash does not match the stored request")
		}
		if r.entries[k] != nil {
			return corrupt(off, "second intent for %s/%d", rec.Label, rec.K)
		}
		r.entries[k] = &entry{key: k, state: stateAmbiguous, canon: rec.Canon, label: rec.Label, class: rec.Class, honor: rec.HonorKey, intended: true}
		if !rec.HonorKey {
			r.quarantined = true
		}
	case recCommit:
		if rec.Resp == nil {
			return corrupt(off, "commit without a response")
		}
		if hashOf(rec.Kind, rec.Canon) != rec.Hash {
			return corrupt(off, "hash does not match the stored request")
		}
		e := r.entries[k]
		if e != nil && e.state == stateCommitted {
			return corrupt(off, "second commit for %s/%d", rec.Label, rec.K)
		}
		if e == nil {
			e = &entry{key: k, canon: rec.Canon, label: rec.Label, class: rec.Class, honor: rec.HonorKey}
			r.entries[k] = e
		}
		e.state, e.resp = stateCommitted, rec.Resp
		if rec.Kind == KindLLM {
			r.model = append(r.model, e)
			if r.firstModel == "" {
				r.firstModel = rec.Hash
			}
		}
	default:
		return corrupt(off, "unknown record type %q", rec.T)
	}
	r.lastAttempt = max(r.lastAttempt, rec.A)
	return nil
}

type action uint8

const (
	actLive action = iota + 1
	actWait
	actReplay
	actAmbiguous
)

func (a action) String() string {
	return [...]string{"?", "live", "wait", "replay", "ambiguous"}[a]
}

func decide(e *entry) action {
	switch {
	case e == nil:
		return actLive
	case e.state == statePending:
		return actWait
	case e.state == stateAmbiguous:
		return actAmbiguous
	default:
		return actReplay
	}
}

// Do serves one call: from the journal if this occurrence is recorded, by
// waiting if it is in flight, or live otherwise. A live response is
// journaled before the client sees it complete.
func (r *Run) Do(ctx context.Context, attemptID string, c Call, w Writer, up Upstream) (Outcome, error) {
	h := hashOf(c.Kind, c.Canonical)

	r.mu.Lock()
	at := r.selectAttempt(attemptID, c.Kind, h)
	ok := occKey{c.Kind, h}
	at.occ[ok]++
	k := key{c.Kind, h, at.occ[ok]}
	if c.Kind == KindLLM {
		at.modelSteps++
	}
	step := at.modelSteps
	for {
		e := r.entries[k]
		switch decide(e) {
		case actWait:
			done := e.done
			r.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return Outcome{}, ctx.Err()
			}
			r.mu.Lock()
		case actReplay:
			r.consume(at, e)
			r.mu.Unlock()
			w.Head(e.resp.Status, e.resp.ContentType, true)
			for _, f := range e.resp.Frames {
				w.Frame(f)
			}
			w.Finish(e.resp)
			return Outcome{Replay: true}, nil
		case actAmbiguous:
			if e.honor {
				e.state = statePending
				e.origin = at
				e.done = make(chan struct{})
				r.mu.Unlock()
				return r.live(ctx, e, w, up)
			}
			r.quarantined = true
			r.mu.Unlock()
			return Outcome{Blocked: true, Message: ambiguousMessage(e.label)}, nil
		case actLive:
			if c.Class == ClassSideEffect && r.quarantined {
				r.mu.Unlock()
				return Outcome{Blocked: true, Message: quarantineMessage}, nil
			}
			e = &entry{key: k, state: statePending, canon: c.Canonical, label: c.Label, class: c.Class, honor: c.HonorKey, origin: at, done: make(chan struct{})}
			r.entries[k] = e
			if c.Kind == KindLLM {
				r.miss(at, c, step)
			}
			r.mu.Unlock()
			return r.live(ctx, e, w, up)
		}
	}
}

// selectAttempt applies the three attempt rules: a new process, a new
// attempt id in the URL, or the run's first model request arriving again
// after this attempt has handled something else.
func (r *Run) selectAttempt(attemptID string, kind Kind, hash string) *attempt {
	if kind == KindLLM && r.firstModel == "" {
		r.firstModel = hash
	}
	isFirst := kind == KindLLM && hash == r.firstModel
	switch {
	case r.cur == nil,
		attemptID != "" && attemptID != r.cur.id,
		attemptID == "" && isFirst && r.cur.sawFirst && r.cur.sawOther:
		r.startAttempt(attemptID)
	}
	if isFirst {
		r.cur.sawFirst = true
	} else {
		r.cur.sawOther = true
	}
	return r.cur
}

func (r *Run) startAttempt(id string) {
	r.lastAttempt++
	at := &attempt{
		n: r.lastAttempt, id: id, started: time.Now(),
		occ: map[occKey]int{}, isExp: map[*entry]bool{}, consumed: map[*entry]bool{},
	}
	// The previous attempt in this process tells us which recorded calls are
	// still on the path: the ones it used, plus the ones it was still
	// expected to replay unless it diverged. A fresh process knows only the
	// journal, so it expects every recorded model call.
	keep := func(*entry) bool { return true }
	if prev := r.cur; prev != nil {
		onPath := map[*entry]bool{}
		for _, e := range prev.lineage {
			onPath[e] = true
		}
		if prev.stats.Divergence == nil {
			for _, e := range prev.expected {
				onPath[e] = true
			}
		}
		keep = func(e *entry) bool { return onPath[e] }
	}
	for _, e := range r.model {
		if keep(e) {
			at.expected = append(at.expected, e)
			at.isExp[e] = true
		}
	}
	at.remaining = len(at.expected)
	at.stats = AttemptStatus{N: at.n, ID: id, StartedAt: at.started, Expected: len(at.expected)}
	r.attempts = append(r.attempts, at)
	r.cur = at
}

func (r *Run) consume(at *attempt, e *entry) {
	at.stats.Replayed++
	if e.key.kind != KindLLM {
		return
	}
	at.stats.TokensSaved += e.resp.Usage.Total()
	at.lineage = append(at.lineage, e)
	if at.isExp[e] && !at.consumed[e] {
		at.consumed[e] = true
		at.remaining--
	}
}

// miss records a model request with no recorded match. With nothing left to
// replay, the agent has caught up. Otherwise it diverged: it asked for
// something new while recorded calls were still waiting.
func (r *Run) miss(at *attempt, c Call, step int) {
	if at.stats.LiveAfterMS == nil {
		ms := float64(time.Since(at.started).Microseconds()) / 1000
		at.stats.LiveAfterMS = &ms
	}
	if at.remaining == 0 || at.stats.Divergence != nil {
		return
	}
	var want *entry
	for _, e := range at.expected {
		if !at.consumed[e] {
			want = e
			break
		}
	}
	d := &Divergence{Step: step, Label: c.Label, At: time.Now()}
	if diff, ok := canon.FirstDiff(want.canon, c.Canonical); ok {
		d.Pointer, d.Recorded, d.Got = diff.Pointer, diff.A, diff.B
	} else {
		d.Recorded, d.Got = "(identical request)", "(an extra occurrence of it)"
	}
	at.stats.Divergence = d
	log.Printf("wirejournal: run %s attempt %d diverged at model step %d (%s): at %q recorded %s, got %s; continuing live",
		r.id, at.n, step, c.Label, d.Pointer, d.Recorded, d.Got)
}

type liveEmitter struct {
	w      Writer
	headed bool
}

func (l *liveEmitter) Head(status int, ct string) {
	l.headed = true
	l.w.Head(status, ct, false)
}

func (l *liveEmitter) Frame(p []byte) { l.w.Frame(p) }

func (r *Run) live(ctx context.Context, e *entry, w Writer, up Upstream) (Outcome, error) {
	if e.class == ClassSideEffect && !e.intended {
		if err := r.appendRecord(record{T: recIntent, Kind: e.key.kind, Hash: e.key.hash, K: e.key.k, A: e.origin.n,
			Label: e.label, Canon: e.canon, Class: e.class, HonorKey: e.honor, At: time.Now()}); err != nil {
			r.abandon(e)
			return Outcome{}, fmt.Errorf("%w: %v", ErrJournal, err)
		}
		e.intended = true
	}
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), upstreamTimeout)
	defer cancel()
	em := &liveEmitter{w: w}
	key := ""
	if e.class == ClassSideEffect {
		key = IdempotencyKey(r.id, e.key.kind, e.key.hash, e.key.k)
	}
	resp, err := up(uctx, key, em)
	if err != nil {
		if e.intended {
			r.markAmbiguous(e)
		} else {
			r.abandon(e)
		}
		return Outcome{}, fmt.Errorf("%w: %v", ErrUpstream, err)
	}

	rec := record{T: recCommit, Kind: e.key.kind, Hash: e.key.hash, K: e.key.k, A: e.origin.n,
		Label: e.label, Canon: e.canon, Class: e.class, HonorKey: e.honor, Resp: resp, At: time.Now()}
	if err := r.appendRecord(rec); err != nil {
		if e.intended {
			r.markAmbiguous(e)
		} else {
			r.abandon(e)
		}
		return Outcome{}, fmt.Errorf("%w: %v", ErrJournal, err)
	}

	r.mu.Lock()
	e.state, e.resp = stateCommitted, resp
	at := e.origin
	at.stats.Live++
	if e.key.kind == KindLLM {
		r.model = append(r.model, e)
		at.lineage = append(at.lineage, e)
		at.stats.TokensBilled += resp.Usage.Total()
	}
	close(e.done)
	r.mu.Unlock()

	if !em.headed {
		w.Head(resp.Status, resp.ContentType, false)
	}
	w.Finish(resp)
	return Outcome{}, nil
}

// abandon forgets a live call that produced no durable response, so the
// agent's retry gets the same occurrence number.
func (r *Run) abandon(e *entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries[e.key] == e {
		delete(r.entries, e.key)
	}
	ok := occKey{e.key.kind, e.key.hash}
	if e.origin.occ[ok] == e.key.k {
		e.origin.occ[ok]--
	}
	close(e.done)
}

func (r *Run) markAmbiguous(e *entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.state = stateAmbiguous
	if !e.honor {
		r.quarantined = true
	}
	if e.done != nil {
		select {
		case <-e.done:
		default:
			close(e.done)
		}
	}
}

func (r *Run) appendRecord(rec record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return r.log.Append(b)
}

// Status returns a snapshot for the status endpoint.
func (r *Run) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := Status{Run: r.id, Quarantined: r.quarantined, Attempts: []AttemptStatus{}}
	for _, e := range r.entries {
		switch e.state {
		case stateCommitted:
			st.Recorded++
		case stateAmbiguous:
			st.Ambiguous = append(st.Ambiguous, AmbiguousCall{Kind: e.key.kind, Label: e.label, K: e.key.k})
		}
	}
	for _, at := range r.attempts {
		st.Attempts = append(st.Attempts, at.stats)
	}
	return st
}

const quarantineMessage = "wirejournal: run quarantined; an earlier side-effecting call has an unknown outcome"

func ambiguousMessage(label string) string {
	return "wirejournal: " + label + " may already have run; the proxy died before recording the result and will not repeat it"
}

// IdempotencyKey is stable for (run, kind, hash, k) across attempts and crashes.
func IdempotencyKey(run string, kind Kind, hash string, k int) string {
	sum := sha256.Sum256([]byte(run + "\x00" + string(kind) + "\x00" + hash + "\x00" + fmt.Sprint(k)))
	return "wj1-" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:16])
}
