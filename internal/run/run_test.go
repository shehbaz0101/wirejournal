package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/journal"
)

// tape records what a client saw, in order.
type tape struct {
	mu  sync.Mutex
	ops []string
	// onFinish runs before Finish is recorded.
	onFinish func()
}

func (t *tape) Head(status int, ct string, replay bool) {
	t.add(fmt.Sprintf("head %d %s replay=%v", status, ct, replay))
}
func (t *tape) Frame(p []byte) { t.add("frame " + string(p)) }
func (t *tape) Finish(r *Response) {
	if t.onFinish != nil {
		t.onFinish()
	}
	t.add("finish " + string(r.Body) + string(r.Tail))
}
func (t *tape) add(s string) { t.mu.Lock(); t.ops = append(t.ops, s); t.mu.Unlock() }
func (t *tape) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.ops, " | ")
}

// fake is an upstream that bills one call per invocation.
type fake struct {
	calls atomic.Int64
	// respond builds the n-th response (1-based) for a request.
	respond func(n int64, c Call) (*Response, error)
}

func (f *fake) up(c Call) Upstream {
	return func(ctx context.Context, key string, e Emitter) (*Response, error) {
		n := f.calls.Add(1)
		r, err := f.respond(n, c)
		if err != nil {
			return nil, err
		}
		if len(r.Frames) > 0 {
			e.Head(r.Status, r.ContentType)
			for _, fr := range r.Frames {
				e.Frame(fr)
			}
		}
		return r, nil
	}
}

func unary(n int64, c Call) (*Response, error) {
	return &Response{Status: 200, ContentType: "application/json",
		Body: []byte(fmt.Sprintf(`{"n":%d,"label":%q}`, n, c.Label)), Usage: Usage{Prompt: 10, Completion: 5}}, nil
}

func llm(label, content string) Call {
	return Call{Kind: KindLLM, Label: label,
		Canonical: []byte(fmt.Sprintf(`{"messages":[{"content":"system"},{"content":%q}],"model":"m"}`, content))}
}

func do(t *testing.T, r *Run, attempt string, c Call, f *fake) (string, Outcome) {
	t.Helper()
	tp := &tape{}
	out, err := r.Do(context.Background(), attempt, c, tp, f.up(c))
	if err != nil {
		t.Fatalf("Do(%s): %v", c.Label, err)
	}
	return tp.String(), out
}

func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func openRun(t *testing.T, s *Store, id string) *Run {
	t.Helper()
	r, err := s.Run(id, true)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name string
		e    *entry
		want action
	}{
		{"absent", nil, actLive},
		{"pending", &entry{state: statePending}, actWait},
		{"committed", &entry{state: stateCommitted}, actReplay},
		{"ambiguous", &entry{state: stateAmbiguous}, actAmbiguous},
	}
	for _, c := range cases {
		if got := decide(c.e); got != c.want {
			t.Errorf("%s: decide = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestReplayAfterRestartIsByteIdentical(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: func(n int64, c Call) (*Response, error) {
		if c.Label == "stream" {
			return &Response{Status: 200, ContentType: "text/event-stream",
				Frames: [][]byte{[]byte("data: {\"d\":\"he\"}\n\n"), []byte("data: {\"d\":\"llo\"}\n\n")},
				Tail:   []byte("data: [DONE]\n\n"), Usage: Usage{Prompt: 7, Completion: 2}}, nil
		}
		return unary(n, c)
	}}
	calls := []Call{llm("a", "one"), llm("stream", "two"), llm("c", "three")}

	s := openStore(t, dir)
	r := openRun(t, s, "job-1")
	var first []string
	for _, c := range calls {
		got, out := do(t, r, "", c, f)
		if out.Replay {
			t.Fatalf("%s replayed on a fresh run", c.Label)
		}
		first = append(first, got)
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job-1")
	for i, c := range calls {
		got, out := do(t, r2, "", c, f)
		if !out.Replay {
			t.Fatalf("%s went live after restart", c.Label)
		}
		want := strings.ReplaceAll(first[i], "replay=false", "replay=true")
		if got != want {
			t.Fatalf("%s replay differs:\n got %s\nwant %s", c.Label, got, want)
		}
	}
	if n := f.calls.Load(); n != 3 {
		t.Fatalf("upstream billed %d calls, want 3", n)
	}
	st := r2.Status()
	a := st.Attempts[len(st.Attempts)-1]
	if a.Replayed != 3 || a.Live != 0 || a.ModelReplayed != 3 || a.ModelLive != 0 || a.TokensSaved != 15+9+15 || a.Expected != 3 {
		t.Fatalf("attempt status = %+v", a)
	}
}

func TestIdenticalRequestsMapToTheirOccurrence(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: unary}
	c := llm("same", "retry me")

	s := openStore(t, dir)
	r := openRun(t, s, "job")
	r1, _ := do(t, r, "", c, f)
	r2, _ := do(t, r, "", c, f)
	if r1 == r2 {
		t.Fatal("a retry of an identical request was served the first result")
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	rr := openRun(t, s2, "job")
	g1, o1 := do(t, rr, "", c, f)
	g2, o2 := do(t, rr, "", c, f)
	if !o1.Replay || !o2.Replay || !strings.Contains(g1, `"n":1`) || !strings.Contains(g2, `"n":2`) {
		t.Fatalf("occurrences replayed out of order: %s / %s", g1, g2)
	}
	if _, o3 := do(t, rr, "", c, f); o3.Replay {
		t.Fatal("third occurrence must go live")
	}
	if n := f.calls.Load(); n != 3 {
		t.Fatalf("billed %d, want 3", n)
	}
}

func TestFirstRequestRecurringStartsAnAttempt(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	r := openRun(t, s, "job")
	f := &fake{respond: unary}
	a, b := llm("a", "start"), llm("b", "next")

	do(t, r, "", a, f)
	do(t, r, "", b, f)
	if _, o := do(t, r, "", a, f); !o.Replay {
		t.Fatal("agent restart (first request again) did not replay")
	}
	if _, o := do(t, r, "", b, f); !o.Replay {
		t.Fatal("second step of the restarted agent did not replay")
	}
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("billed %d, want 2", n)
	}
	if got := len(r.Status().Attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestNaiveRetryOfFirstRequestIsNotARestart(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	r := openRun(t, s, "job")
	f := &fake{respond: unary}
	a := llm("a", "start")
	do(t, r, "", a, f)
	if _, o := do(t, r, "", a, f); o.Replay {
		t.Fatal("an immediate retry of the first request was replayed")
	}
	if got := len(r.Status().Attempts); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestAttemptIDStartsAnAttempt(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	r := openRun(t, s, "job")
	f := &fake{respond: unary}
	a := llm("a", "start")
	do(t, r, "x", a, f)
	do(t, r, "x", a, f)
	got, o := do(t, r, "y", a, f)
	if !o.Replay || !strings.Contains(got, `"n":1`) {
		t.Fatalf("new attempt id did not reset occurrences: %s", got)
	}
	if _, o := do(t, r, "y", a, f); !o.Replay {
		t.Fatal("second occurrence in attempt y must replay n=2")
	}
}

func TestDivergenceIsReportedAndGoesLive(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: unary}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	a, b, c := llm("a", "step one"), llm("b", "step two"), llm("c", "step three")
	for _, x := range []Call{a, b, c} {
		do(t, r, "", x, f)
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	do(t, r2, "", a, f)
	bPrime := llm("b", "step two, but the clock changed")
	if _, o := do(t, r2, "", bPrime, f); o.Replay {
		t.Fatal("a changed request was replayed")
	}
	if _, o := do(t, r2, "", c, f); !o.Replay {
		t.Fatal("an unchanged later request must still replay after a divergence")
	}
	at := r2.Status().Attempts[0]
	d := at.Divergence
	if d == nil {
		t.Fatalf("no divergence reported: %+v", at)
	}
	if d.Pointer != "/messages/1/content" || d.Recorded != `"step two"` || !strings.Contains(d.Got, "clock") || d.Step != 2 {
		t.Fatalf("divergence = %+v", d)
	}
}

func TestCatchingUpIsNotDivergence(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: unary}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	a, b := llm("a", "1"), llm("b", "2")
	do(t, r, "", a, f)
	do(t, r, "", b, f)
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	do(t, r2, "", a, f)
	do(t, r2, "", b, f)
	do(t, r2, "", llm("c", "3"), f)
	at := r2.Status().Attempts[0]
	if at.Divergence != nil || at.LiveAfterMS == nil || at.Replayed != 2 || at.Live != 1 {
		t.Fatalf("attempt = %+v", at)
	}
}

func TestTransportFailureIsNotJournaled(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: func(n int64, c Call) (*Response, error) {
		if n == 1 {
			return nil, errors.New("connection refused")
		}
		return unary(n, c)
	}}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	a := llm("a", "x")
	if _, err := r.Do(context.Background(), "", a, &tape{}, f.up(a)); !errors.Is(err, ErrUpstream) {
		t.Fatalf("Do = %v, want ErrUpstream", err)
	}
	do(t, r, "", a, f)
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	got, o := do(t, r2, "", a, f)
	if !o.Replay || !strings.Contains(got, `"n":2`) {
		t.Fatalf("after a failed attempt the retry must be occurrence 1: %s", got)
	}
}

func TestHTTPErrorsAreJournaled(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: func(n int64, c Call) (*Response, error) {
		if n == 1 {
			return &Response{Status: 429, ContentType: "application/json", Body: []byte(`{"error":"slow down"}`)}, nil
		}
		return unary(n, c)
	}}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	a := llm("a", "x")
	do(t, r, "", a, f)
	do(t, r, "", a, f)
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	g1, _ := do(t, r2, "", a, f)
	g2, _ := do(t, r2, "", a, f)
	if !strings.Contains(g1, "head 429") || !strings.Contains(g2, "head 200") {
		t.Fatalf("replay = %s / %s", g1, g2)
	}
	if f.calls.Load() != 2 {
		t.Fatalf("billed %d", f.calls.Load())
	}
}

// An agent killed mid-call restarts while the proxy is still waiting on the
// upstream. The new attempt must wait for that call rather than pay again.
func TestInFlightCallIsSharedWithTheNextAttempt(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	r := openRun(t, s, "job")
	gate := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	f := &fake{respond: func(n int64, c Call) (*Response, error) {
		once.Do(func() { close(started) })
		<-gate
		return unary(n, c)
	}}
	a := llm("a", "slow")

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() {
		_, err := r.Do(ctx1, "1", a, &tape{}, f.up(a))
		done1 <- err
	}()
	<-started
	cancel1()

	done2 := make(chan string, 1)
	go func() {
		tp := &tape{}
		out, err := r.Do(context.Background(), "2", a, tp, f.up(a))
		if err != nil || !out.Replay {
			done2 <- fmt.Sprintf("err=%v replay=%v", err, out.Replay)
			return
		}
		done2 <- tp.String()
	}()
	time.Sleep(20 * time.Millisecond)
	close(gate)

	if err := <-done1; err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	if got := <-done2; !strings.Contains(got, `"n":1`) {
		t.Fatalf("second attempt got %s", got)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("billed %d, want 1", f.calls.Load())
	}
}

func TestClientDisconnectDoesNotCancelUpstream(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	ctx, cancel := context.WithCancel(context.Background())
	a := llm("a", "x")
	var upErr error
	up := func(uctx context.Context, _ string, e Emitter) (*Response, error) {
		cancel()
		time.Sleep(10 * time.Millisecond)
		upErr = uctx.Err()
		return unary(1, a)
	}
	if _, err := r.Do(ctx, "", a, &tape{}, up); err != nil {
		t.Fatal(err)
	}
	if upErr != nil {
		t.Fatalf("upstream context was cancelled with the client: %v", upErr)
	}
	s.Close()
	s2 := openStore(t, dir)
	defer s2.Close()
	if _, o := do(t, openRun(t, s2, "job"), "", a, &fake{respond: unary}); !o.Replay {
		t.Fatal("call finished after the client left was not journaled")
	}
}

// The client must never see a complete response that is not durable.
func TestFinishHappensOnlyAfterCommitIsDurable(t *testing.T) {
	cf := journal.NewCrashFile()
	r, err := Open("job", cf)
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{respond: unary}
	for i := 0; i < 5; i++ {
		c := llm("a", fmt.Sprint(i))
		want := i + 1
		tp := &tape{onFinish: func() {
			n := 0
			if _, _, err := journal.OpenFile(cf.DurableCopy(), func(int64, []byte) error { n++; return nil }); err != nil {
				t.Errorf("durable image unreadable: %v", err)
			}
			if n != want {
				t.Errorf("Finish before commit was durable: %d records on disk, want %d", n, want)
			}
		}}
		if _, err := r.Do(context.Background(), "", c, tp, f.up(c)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryRejectsTamperedRecords(t *testing.T) {
	good := record{T: recCommit, Kind: KindLLM, K: 1, A: 1, Canon: []byte(`{"a":1}`),
		Resp: &Response{Status: 200}}
	good.Hash = hashOf(KindLLM, good.Canon)

	cases := map[string][]record{
		"hash mismatch":    {{T: recCommit, Kind: KindLLM, Hash: hashOf(KindLLM, []byte(`{"a":2}`)), K: 1, A: 1, Canon: good.Canon, Resp: good.Resp}},
		"duplicate commit": {good, good},
		"unknown type":     {{T: "mystery", Kind: KindLLM, Hash: good.Hash, K: 1}},
		"zero occurrence":  {{T: recCommit, Kind: KindLLM, Hash: good.Hash, K: 0, A: 1, Canon: good.Canon, Resp: good.Resp}},
	}
	for name, recs := range cases {
		t.Run(name, func(t *testing.T) {
			cf := journal.NewCrashFile()
			l, _, err := journal.OpenFile(cf, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range recs {
				b, _ := json.Marshal(rec)
				if err := l.Append(b); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Open("job", cf); !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("Open = %v, want ErrCorruptRecord", err)
			}
		})
	}
}

func TestStoreRejectsBadRunIDs(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for _, id := range []string{"", ".", "..", "a/b", "../x", strings.Repeat("x", 200), "has space"} {
		if _, err := s.Run(id, true); !errors.Is(err, ErrBadRunID) {
			t.Errorf("Run(%q) = %v, want ErrBadRunID", id, err)
		}
	}
	if _, err := s.Run("missing", false); !errors.Is(err, ErrNoRun) {
		t.Errorf("Run(missing, create=false) = %v, want ErrNoRun", err)
	}
}

func TestStoreReturnsLockedWhenAnotherStoreHoldsTheRun(t *testing.T) {
	dir := t.TempDir()
	s1 := openStore(t, dir)
	defer s1.Close()
	openRun(t, s1, "job")
	s2 := openStore(t, dir)
	defer s2.Close()
	if _, err := s2.Run("job", true); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("Run = %v, want journal.ErrLocked", err)
	}
}

func TestConcurrentDistinctCalls(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: unary}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	const workers, each = 16, 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				c := llm("x", fmt.Sprintf("w%d-%d", w, i))
				if _, err := r.Do(context.Background(), "", c, &tape{}, f.up(c)); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	for w := 0; w < workers; w++ {
		for i := 0; i < each; i++ {
			if _, o := do(t, r2, "", llm("x", fmt.Sprintf("w%d-%d", w, i)), f); !o.Replay {
				t.Fatalf("w%d-%d went live after restart", w, i)
			}
		}
	}
	if n := f.calls.Load(); n != workers*each {
		t.Fatalf("billed %d, want %d", n, workers*each)
	}
}

// Power fails during the commit of a random step. After recovery the agent
// runs again from the top: every acknowledged step replays, and each step is
// billed once more at most (only the one whose commit was lost).
func TestPowerLossDuringCommitProperty(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		steps := 1 + rng.Intn(15)
		failAt := rng.Intn(steps)
		cf := journal.NewCrashFile()
		r, err := Open("job", cf)
		if err != nil {
			t.Fatal(err)
		}
		f := &fake{respond: unary}
		acked := 0
		for i := 0; i < steps; i++ {
			c := llm("s", fmt.Sprint(i))
			if i == failAt {
				cf.FailSync = errors.New("power cut")
			}
			if _, err := r.Do(context.Background(), "", c, &tape{}, f.up(c)); err != nil {
				if !errors.Is(err, ErrJournal) {
					t.Fatalf("seed %d: Do = %v, want ErrJournal", seed, err)
				}
				break
			}
			acked++
		}
		cf.LosePower(rng.Intn(400), rng.Intn(2) == 0)
		cf.FailSync = nil

		r2, err := Open("job", cf)
		if err != nil {
			t.Fatalf("seed %d: recovery: %v", seed, err)
		}
		before := f.calls.Load()
		for i := 0; i < steps; i++ {
			c := llm("s", fmt.Sprint(i))
			out, err := r2.Do(context.Background(), "", c, &tape{}, f.up(c))
			if err != nil {
				t.Fatalf("seed %d: step %d after recovery: %v", seed, i, err)
			}
			if i < acked && !out.Replay {
				t.Fatalf("seed %d: acknowledged step %d was billed again", seed, i)
			}
		}
		// The step whose fsync failed may still have reached the disk.
		rebilled := f.calls.Load() - before
		if rebilled != int64(steps-acked) && rebilled != int64(steps-acked-1) {
			t.Fatalf("seed %d: %d live calls after recovery, want %d (or one fewer)", seed, rebilled, steps-acked)
		}
	}
}

func tool(name, args string, class Class, honor bool) Call {
	return Call{Kind: KindTool, Label: name, Class: class, HonorKey: honor,
		Canonical: []byte(fmt.Sprintf(`{"arguments":%s,"name":%q}`, args, name))}
}

func TestSideEffectReplaysAndIsAtMostOnce(t *testing.T) {
	dir := t.TempDir()
	f := &fake{respond: unary}
	s := openStore(t, dir)
	r := openRun(t, s, "job")
	c := tool("send_email", `{"to":"a@b"}`, ClassSideEffect, false)
	do(t, r, "", llm("a", "plan"), f)
	got, out := do(t, r, "", c, f)
	if out.Replay || !strings.Contains(got, "send_email") {
		t.Fatalf("live tool: %s %+v", got, out)
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	r2 := openRun(t, s2, "job")
	if _, o := do(t, r2, "", llm("a", "plan"), f); !o.Replay {
		t.Fatal("model should replay")
	}
	if _, o := do(t, r2, "", c, f); !o.Replay {
		t.Fatal("committed email was sent again")
	}
	if f.calls.Load() != 2 {
		t.Fatalf("billed %d, want 2", f.calls.Load())
	}
}

func TestIntentWithoutCommitFailsClosedAndQuarantines(t *testing.T) {
	// header, model commit, intent, then the result commit fails.
	cf := journal.NewCrashFile()
	r, err := Open("job", &nthFail{File: cf, fail: 4})
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{respond: unary}
	do(t, r, "", llm("a", "plan"), f)
	c := tool("send_email", `{"to":"a@b"}`, ClassSideEffect, false)
	calls := 0
	up := func(ctx context.Context, key string, e Emitter) (*Response, error) {
		calls++
		if key == "" {
			t.Error("side-effect dispatch missing idempotency key")
		}
		return unary(1, c)
	}
	if _, err := r.Do(context.Background(), "", c, &tape{}, up); err == nil {
		t.Fatal("commit failure returned nil")
	}
	cf.LosePower(0, false)

	r2, err := Open("job", cf)
	if err != nil {
		t.Fatal(err)
	}
	out, err := r2.Do(context.Background(), "", c, &tape{}, up)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Blocked || !strings.Contains(out.Message, "will not repeat") {
		t.Fatalf("ambiguous = %+v", out)
	}
	if calls != 1 {
		t.Fatalf("dispatched %d times, want 1", calls)
	}
	c2 := tool("send_email", `{"to":"other@b"}`, ClassSideEffect, false)
	out, err = r2.Do(context.Background(), "", c2, &tape{}, up)
	if err != nil || !out.Blocked || !strings.Contains(out.Message, "quarantined") {
		t.Fatalf("quarantine = %+v err=%v", out, err)
	}
	if !r2.Status().Quarantined || len(r2.Status().Ambiguous) != 1 {
		t.Fatalf("status = %+v", r2.Status())
	}
	if _, o := do(t, r2, "", llm("a", "plan"), f); !o.Replay {
		t.Fatal("quarantine must not block replay of committed model calls")
	}
}

func TestAmbiguousWithIdempotencyKeyRedispatches(t *testing.T) {
	cf := journal.NewCrashFile()
	r, err := Open("job", &nthFail{File: cf, fail: 4})
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{respond: unary}
	do(t, r, "", llm("a", "plan"), f)
	c := tool("send_email", `{"to":"a@b"}`, ClassSideEffect, true)
	var keys []string
	up := func(ctx context.Context, key string, e Emitter) (*Response, error) {
		keys = append(keys, key)
		return unary(int64(len(keys)), c)
	}
	if _, err := r.Do(context.Background(), "", c, &tape{}, up); err == nil {
		t.Fatal("expected commit failure")
	}
	cf.LosePower(0, false)
	r2, err := Open("job", cf)
	if err != nil {
		t.Fatal(err)
	}
	tp := &tape{}
	out, err := r2.Do(context.Background(), "", c, tp, up)
	if err != nil {
		t.Fatal(err)
	}
	if out.Blocked || out.Replay || !strings.Contains(tp.String(), `"n":2`) {
		t.Fatalf("redispatch = %s %+v", tp.String(), out)
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("keys = %v", keys)
	}
}

func TestReadOnlyHasNoIntentAndReruns(t *testing.T) {
	cf := journal.NewCrashFile()
	r, err := Open("job", &nthFail{File: cf, fail: 2})
	if err != nil {
		t.Fatal(err)
	}
	c := tool("search", `{"q":"x"}`, ClassReadOnly, false)
	if _, err := r.Do(context.Background(), "", c, &tape{}, func(ctx context.Context, key string, e Emitter) (*Response, error) {
		return unary(1, c)
	}); err == nil {
		t.Fatal("expected failure")
	}
	cf.LosePower(0, false)
	r2, err := Open("job", cf)
	if err != nil {
		t.Fatal(err)
	}
	if _, o := do(t, r2, "", c, &fake{respond: unary}); o.Replay || o.Blocked {
		t.Fatalf("read-only should re-run, got %+v", o)
	}
	if r2.Status().Quarantined {
		t.Fatal("read-only miss must not quarantine")
	}
}

func TestToolBeforeFirstModelDoesNotStartAnAttempt(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	r := openRun(t, s, "job")
	f := &fake{respond: unary}
	do(t, r, "", tool("search", `{"q":"x"}`, ClassReadOnly, false), f)
	do(t, r, "", llm("a", "start"), f)
	if got := len(r.Status().Attempts); got != 1 {
		t.Fatalf("attempts = %d, want 1 (tools/list before the first model is not a restart)", got)
	}
}

func TestIdempotencyKeyStableAndShaped(t *testing.T) {
	a := IdempotencyKey("job", KindTool, "abc", 1)
	b := IdempotencyKey("job", KindTool, "abc", 1)
	c := IdempotencyKey("job", KindTool, "abc", 2)
	if a != b || a == c || !strings.HasPrefix(a, "wj1-") || len(a) != 4+26 {
		t.Fatalf("keys %q %q %q", a, b, c)
	}
}

// nthFail fails the n-th Sync (1-based) without making it durable.
type nthFail struct {
	journal.File
	n, fail int
}

func (f *nthFail) Sync() error {
	f.n++
	if f.n == f.fail {
		return errors.New("injected fsync failure")
	}
	return f.File.Sync()
}
