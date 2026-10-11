// Package bench is the crash-recovery benchmark. Numbers come from the mock
// LLM billing ledger and the mock mail delivery counter, never from the
// proxy's own status.
package bench

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/lane"
	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/script"
)

type Trial struct {
	Seed       int64   `json:"seed"`
	CrashAt    int     `json:"crash_at"`
	Arm        string  `json:"arm"`
	Scenario   string  `json:"scenario"`
	ResumeMS   float64 `json:"resume_ms"`
	ExtraCalls int     `json:"extra_calls"`
	ExtraTok   int     `json:"extra_tokens"`
	Deliveries int     `json:"deliveries"`
	OK         bool    `json:"ok"`
	Err        string  `json:"error,omitempty"`
}

type Summary struct {
	Arm            string  `json:"arm"`
	Trials         int     `json:"trials"`
	MedianResumeMS float64 `json:"median_resume_ms"`
	MedianExtra    int     `json:"median_extra_calls"`
	MaxExtra       int     `json:"max_extra_calls"`
	MedianMail     int     `json:"median_deliveries"`
	MaxMail        int     `json:"max_deliveries"`
}

type Report struct {
	Steps    int     `json:"steps"`
	Host     string  `json:"host"`
	Method   string  `json:"method"`
	WJ       Summary `json:"wirejournal"`
	Baseline Summary `json:"baseline"`
	Trials   []Trial `json:"trials"`
}

func Main(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 30, "random crash-point trials per arm")
	steps := fs.Int("steps", script.Steps(), "agent steps")
	out := fs.String("out", "", "write JSON report")
	bin := fs.String("bin", "", "wirejournal binary (default: this executable)")
	fs.Parse(args)
	b, err := resolveBin(*bin)
	if err != nil {
		return err
	}
	os.Setenv("WJ_STEPS", fmt.Sprint(*steps))
	rep, err := Run(b, *n, *steps, "kill-both")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		return err
	}
	if *out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		return os.WriteFile(*out, b, 0o644)
	}
	return nil
}

func resolveBin(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	return os.Executable()
}

func Build(dir string) (string, error) {
	bin := filepath.Join(dir, "wirejournal")
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "../..")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/wirejournal")
	cmd.Dir = root
	cmd.Stderr = os.Stderr
	return bin, cmd.Run()
}

func Run(bin string, n, steps int, scenario string) (*Report, error) {
	cfg := writeConfig()
	rep := &Report{
		Steps: steps,
		Host:  fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH),
		Method: "parent holds mocks; SIGKILL children at mockllm.Calls==crashAt; " +
			"resume_ms is time from restart until the next live mockllm call; " +
			"extra_calls is mockllm.Calls after resume minus Steps; extra_tokens is mock prompt+completion minus one full run; deliveries are the mock inbox length",
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		crashAt := 1 + rng.Intn(max(1, steps-1))
		seed := int64(i + 1)
		wj, err := one(bin, cfg, steps, seed, crashAt, true, scenario, "agent+proxy")
		if err != nil {
			wj.Err = err.Error()
		}
		base, err := one(bin, cfg, steps, seed, crashAt, false, scenario, "agent+proxy")
		if err != nil {
			base.Err = err.Error()
		}
		rep.Trials = append(rep.Trials, wj, base)
	}
	rep.WJ = summarize("wirejournal", filter(rep.Trials, "wirejournal"))
	rep.Baseline = summarize("baseline", filter(rep.Trials, "baseline"))
	return rep, nil
}

func one(bin, cfg string, steps int, seed int64, crashAt int, journal bool, scenario, killWhat string) (Trial, error) {
	arm := "baseline"
	if journal {
		arm = "wirejournal"
	}
	tr := Trial{Seed: seed, CrashAt: crashAt, Arm: arm, Scenario: scenario}
	llmS := &mockllm.Server{}
	mailS := &mockmail.Server{}
	llmURL, stopLLM := serve(llmS)
	mailURL, stopMail := serve(mailS)
	defer stopLLM()
	defer stopMail()

	data, err := os.MkdirTemp("", "wj-bench-")
	if err != nil {
		return tr, err
	}
	defer os.RemoveAll(data)

	env := []string{"WJ_STEPS=" + os.Getenv("WJ_STEPS")}
	llm := llmURL
	if !journal {
		llm = llmURL + "/v1"
	}
	spec := lane.Spec{Bin: bin, Data: data, LLM: llm, MCP: mailURL, RunID: "job", Attempt: "1", Journal: journal, Stream: true, Config: cfg, ExtraEnv: env}
	p, err := lane.Start(spec)
	if err != nil {
		return tr, err
	}
	defer p.Kill()
	if err := waitCalls(llmURL, crashAt, 60*time.Second); err != nil {
		p.Kill()
		return tr, err
	}
	if killWhat == "agent" {
		p.KillAgent()
	} else {
		p.Kill()
	}
	before := llmS.Stats()
	t0 := time.Now()
	spec.Attempt = "2"
	if killWhat == "agent" && journal && p.ProxyURL != "" {
		spec.ExistingProxy = p.ProxyURL
		spec.Journal = false
	}
	p2, err := lane.Start(spec)
	if err != nil {
		return tr, err
	}
	defer p2.Kill()
	target := before.Calls + 1
	if !journal {
		target = before.Calls + crashAt
	}
	waitErr := waitCalls(llmURL, target, 60*time.Second)
	if waitErr == nil {
		tr.ResumeMS = float64(time.Since(t0).Microseconds()) / 1000
	}
	_ = p2.WaitAgent()
	after := llmS.Stats()
	tr.ExtraCalls = after.Calls - steps
	if tr.ExtraCalls < 0 {
		tr.ExtraCalls = 0
	}
	full := tokensFor(0, steps)
	gotTok := after.PromptTokens + after.CompletionTokens
	tr.ExtraTok = gotTok - full
	if tr.ExtraTok < 0 {
		tr.ExtraTok = 0
	}
	tr.Deliveries = mailS.Stats().Deliveries
	tr.OK = waitErr == nil && after.Calls >= steps
	return tr, nil
}

func tokensFor(from, to int) int {
	n := 0
	for i := from; i < to; i++ {
		p, c := script.Usage(i)
		n += p + c
	}
	return n
}

func writeConfig() string {
	p := filepath.Join(os.TempDir(), "wj-bench-config.json")
	_ = os.WriteFile(p, []byte(`{"tools":{"send_email":{"class":"side_effect","idempotency_key":true}}}`), 0o644)
	return p
}

func serve(h http.Handler) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() { srv.Close() }
}

func waitCalls(llm string, n int, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if stats(llm).Calls >= n {
			return nil
		}
		time.Sleep(3 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %d model calls (have %d)", n, stats(llm).Calls)
}

func stats(llm string) mockllm.Stats {
	resp, err := http.Get(llm + "/stats")
	if err != nil {
		return mockllm.Stats{}
	}
	defer resp.Body.Close()
	var s mockllm.Stats
	json.NewDecoder(resp.Body).Decode(&s)
	io.Copy(io.Discard, resp.Body)
	return s
}

func filter(all []Trial, arm string) []Trial {
	var out []Trial
	for _, t := range all {
		if t.Arm == arm && t.OK {
			out = append(out, t)
		}
	}
	return out
}

func summarize(arm string, ts []Trial) Summary {
	s := Summary{Arm: arm, Trials: len(ts)}
	if len(ts) == 0 {
		return s
	}
	resumes, extras, mails := make([]float64, len(ts)), make([]int, len(ts)), make([]int, len(ts))
	for i, t := range ts {
		resumes[i], extras[i], mails[i] = t.ResumeMS, t.ExtraCalls, t.Deliveries
		if t.ExtraCalls > s.MaxExtra {
			s.MaxExtra = t.ExtraCalls
		}
		if t.Deliveries > s.MaxMail {
			s.MaxMail = t.Deliveries
		}
	}
	s.MedianResumeMS = medianF(resumes)
	s.MedianExtra = medianI(extras)
	s.MedianMail = medianI(mails)
	return s
}

func medianF(v []float64) float64 {
	c := append([]float64(nil), v...)
	for i := 0; i < len(c); i++ {
		for j := i + 1; j < len(c); j++ {
			if c[j] < c[i] {
				c[i], c[j] = c[j], c[i]
			}
		}
	}
	return c[len(c)/2]
}

func medianI(v []int) int {
	c := append([]int(nil), v...)
	for i := 0; i < len(c); i++ {
		for j := i + 1; j < len(c); j++ {
			if c[j] < c[i] {
				c[i], c[j] = c[j], c[i]
			}
		}
	}
	return c[len(c)/2]
}
