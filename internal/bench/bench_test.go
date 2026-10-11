package bench

import (
	"testing"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/lane"
	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/script"
)

func TestCrashRecoveryVsBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("SIGKILL integration")
	}
	t.Setenv("WJ_STEPS", "6")
	bin, err := Build(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Run(bin, 3, 6, "kill-both")
	if err != nil {
		t.Fatal(err)
	}
	if rep.WJ.Trials < 3 || rep.Baseline.Trials < 3 {
		t.Fatalf("trials wj=%d base=%d", rep.WJ.Trials, rep.Baseline.Trials)
	}
	if rep.WJ.MedianExtra != 0 {
		t.Fatalf("wirejournal re-billed to crash point: median extra=%d (trials=%v)", rep.WJ.MedianExtra, filter(rep.Trials, "wirejournal"))
	}
	if rep.Baseline.MedianExtra < 1 {
		t.Fatalf("baseline did not re-bill: %+v", rep.Baseline)
	}
	if rep.WJ.MaxMail > 1 {
		t.Fatalf("wirejournal duplicate email: max deliveries=%d", rep.WJ.MaxMail)
	}
}

func TestAgentOnlyDoesNotRebillInFlight(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Setenv("WJ_STEPS", "6")
	bin, err := Build(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	llmS := &mockllm.Server{Delay: 40 * time.Millisecond}
	mailS := &mockmail.Server{}
	llmURL, stop1 := serve(llmS)
	mailURL, stop2 := serve(mailS)
	defer stop1()
	defer stop2()
	cfg := writeConfig()
	data := t.TempDir()
	spec := lane.Spec{Bin: bin, Data: data, LLM: llmURL, MCP: mailURL, RunID: "job", Attempt: "1", Journal: true, Stream: true, Config: cfg, ExtraEnv: []string{"WJ_STEPS=6"}}
	p, err := lane.Start(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitCalls(llmURL, 2, 20*time.Second); err != nil {
		p.Kill()
		t.Fatal(err)
	}
	p.KillAgent()
	before := llmS.Stats()
	spec.Attempt = "2"
	spec.ExistingProxy = p.ProxyURL
	spec.Journal = false
	p2, err := lane.Start(spec)
	if err != nil {
		p.Kill()
		t.Fatal(err)
	}
	_ = p2.WaitAgent()
	p.Kill()
	p2.Kill()
	after := llmS.Stats()
	if after.Calls < script.Steps() {
		t.Fatalf("did not finish: calls=%d", after.Calls)
	}
	// The in-flight call at kill time may already have been billed. Nothing
	// already billed should be billed again.
	if after.Calls > script.Steps() {
		t.Fatalf("re-billed: have %d want <= %d (before kill %d)", after.Calls, script.Steps(), before.Calls)
	}
	if mailS.Stats().Deliveries != 1 {
		t.Fatalf("deliveries=%d", mailS.Stats().Deliveries)
	}
}

func TestDispatchWindowHonorKeyNoDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Setenv("WJ_STEPS", "5")
	bin, err := Build(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hit := make(chan struct{}, 1)
	llmS := &mockllm.Server{}
	mailS := &mockmail.Server{Delay: 80 * time.Millisecond, After: func() {
		select {
		case hit <- struct{}{}:
		default:
		}
	}}
	llmURL, stop1 := serve(llmS)
	mailURL, stop2 := serve(mailS)
	defer stop1()
	defer stop2()
	cfg := writeConfig()
	data := t.TempDir()
	spec := lane.Spec{Bin: bin, Data: data, LLM: llmURL, MCP: mailURL, RunID: "job", Attempt: "1", Journal: true, Stream: true, Config: cfg, ExtraEnv: []string{"WJ_STEPS=5"}}
	p, err := lane.Start(spec)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-hit:
	case <-time.After(20 * time.Second):
		p.Kill()
		t.Fatal("email never dispatched")
	}
	p.Kill()
	spec.Attempt = "2"
	p2, err := lane.Start(spec)
	if err != nil {
		t.Fatal(err)
	}
	_ = p2.WaitAgent()
	p2.Kill()
	if n := mailS.Stats().Deliveries; n != 1 {
		t.Fatalf("deliveries=%d, want 1", n)
	}
}

func TestBuildBinary(t *testing.T) {
	if _, err := Build(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
