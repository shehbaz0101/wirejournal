// Package demo is the two-lane web demo. The agent processes are real
// children; Crash sends them SIGKILL.
package demo

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/lane"
	"github.com/shehbaz0101/wirejournal/internal/llmproxy"
	"github.com/shehbaz0101/wirejournal/internal/mcpgw"
	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/proxy"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

//go:embed ui/*
var uiFS embed.FS

type Message struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Dup     bool   `json:"dup,omitempty"`
}

type LaneView struct {
	Step         int       `json:"step"`
	Steps        int       `json:"steps"`
	Replayed     int       `json:"replayed"`
	Live         int       `json:"live"`
	TokensBilled int       `json:"tokens_billed"`
	TokensSaved  int       `json:"tokens_saved"`
	Deliveries   int       `json:"deliveries"`
	Inbox        []Message `json:"inbox"`
	State        string    `json:"state"`
}

type snapshot struct {
	Wirejournal LaneView `json:"wirejournal"`
	Baseline    LaneView `json:"baseline"`
}

type laneState struct {
	name         string
	journal      bool
	llm          *mockllm.Server
	mail         *mockmail.Server
	llmURL       string
	mailURL      string
	store        *run.Store
	pxURL        string
	px           *proxy.Server
	mcp          *mcpgw.Handler
	bin          string
	cfg          string
	data         string
	proc         *lane.Proc
	attempt      int
	gen          int
	state        string
	callsAtStart int
}

type Demo struct {
	steps int
	wj    *laneState
	base  *laneState
	mu    sync.Mutex
	subs  map[chan []byte]struct{}
}

func Main(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	listen := fs.String("listen", "", "bind address (default 0.0.0.0:$PORT or :8080)")
	bin := fs.String("bin", "", "wirejournal binary")
	steps := fs.Int("steps", 16, "agent steps (keep small so the UI is readable)")
	delay := fs.Duration("delay", 1400*time.Millisecond, "mock LLM think time per step")
	fs.Parse(args)
	if *listen == "" {
		p := os.Getenv("PORT")
		if p == "" {
			p = "8080"
		}
		*listen = "0.0.0.0:" + p
	}
	exe := *bin
	if exe == "" {
		var err error
		exe, err = os.Executable()
		if err != nil {
			return err
		}
	}
	d, err := New(exe, *steps)
	if err != nil {
		return err
	}
	d.wj.llm.Delay = *delay
	d.base.llm.Delay = *delay
	d.startBoth()
	log.Printf("wirejournal demo on %s", *listen)
	return http.ListenAndServe(*listen, d.Handler())
}

func New(bin string, steps int) (*Demo, error) {
	os.Setenv("WJ_STEPS", fmt.Sprint(steps))
	cfg := filepath.Join(os.TempDir(), "wj-demo.json")
	if err := os.WriteFile(cfg, []byte(`{"tools":{"send_email":{"class":"side_effect","idempotency_key":true}}}`), 0o644); err != nil {
		return nil, err
	}
	wj, err := newLane("wirejournal", bin, cfg, true)
	if err != nil {
		return nil, err
	}
	base, err := newLane("baseline", bin, cfg, false)
	if err != nil {
		return nil, err
	}
	return &Demo{steps: steps, wj: wj, base: base, subs: map[chan []byte]struct{}{}}, nil
}

func newLane(name, bin, cfg string, journal bool) (*laneState, error) {
	l := &laneState{name: name, journal: journal, bin: bin, cfg: cfg, llm: &mockllm.Server{Delay: 220 * time.Millisecond}, mail: &mockmail.Server{}, state: "idle"}
	l.llmURL = serveLocal(l.llm)
	l.mailURL = serveLocal(l.mail)
	if journal {
		dir, err := os.MkdirTemp("", "wj-demo-")
		if err != nil {
			return nil, err
		}
		l.data = dir
		st, err := run.NewStore(dir)
		if err != nil {
			return nil, err
		}
		l.store = st
		lu, _ := url.Parse(l.llmURL)
		mu, _ := url.Parse(l.mailURL)
		mcp := &mcpgw.Handler{Store: st, Upstream: mu, Rules: map[string]mcpgw.Rule{"send_email": {Class: run.ClassSideEffect, HonorKey: true}}}
		h := &proxy.Server{
			Store: st,
			LLM:   &llmproxy.Handler{Upstream: lu},
			MCP:   mcp,
		}
		l.px, l.mcp = h, mcp
		l.pxURL = serveLocal(h)
	}
	return l, nil
}

func serveLocal(h http.Handler) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	return "http://" + ln.Addr().String()
}

func (d *Demo) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/reset", d.reset)
	mux.HandleFunc("/crash", d.crash)
	mux.HandleFunc("/events", d.events)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.snap())
	})
	sub, err := fs.Sub(uiFS, "ui")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return mux
}

func (d *Demo) startBoth() {
	d.start(d.wj)
	d.start(d.base)
}

func (d *Demo) start(l *laneState) {
	d.mu.Lock()
	l.attempt++
	attempt := l.attempt
	callsAtStart := l.llm.Stats().Calls
	d.mu.Unlock()
	llm, mcp := l.llmURL+"/v1", l.mailURL
	if l.journal {
		llm = l.pxURL + "/r/job/a/" + fmt.Sprint(attempt) + "/v1"
		mcp = l.pxURL + "/r/job/a/" + fmt.Sprint(attempt) + "/mcp"
	}
	p, err := lane.Start(lane.Spec{
		Bin: l.bin, LLM: llm, MCP: mcp, Stream: true,
		ExtraEnv: []string{"WJ_STEPS=" + fmt.Sprint(d.steps)},
	})
	d.mu.Lock()
	if err != nil {
		log.Printf("%s start: %v", l.name, err)
		l.state = "dead"
		d.mu.Unlock()
		return
	}
	l.proc = p
	l.state = "running"
	l.callsAtStart = callsAtStart
	l.gen++
	gen := l.gen
	d.mu.Unlock()
	go func() {
		_ = p.WaitAgent()
		d.mu.Lock()
		if l.gen == gen && (l.state == "running" || l.state == "replay" || l.state == "live") {
			l.state = "done"
		}
		d.mu.Unlock()
		d.broadcast()
	}()
}

func (d *Demo) reset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST", http.StatusMethodNotAllowed)
		return
	}
	d.mu.Lock()
	if d.wj.proc != nil {
		d.wj.proc.KillAgent()
	}
	if d.base.proc != nil {
		d.base.proc.KillAgent()
	}
	d.wj.llm.Reset()
	d.base.llm.Reset()
	d.wj.mail.Reset()
	d.base.mail.Reset()
	d.wj.attempt, d.base.attempt = 0, 0
	d.wj.state, d.base.state = "idle", "idle"
	if d.wj.store != nil {
		d.wj.store.Close()
		dir, err := os.MkdirTemp("", "wj-demo-")
		if err != nil {
			d.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		st, err := run.NewStore(dir)
		if err != nil {
			d.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		d.wj.data = dir
		d.wj.store = st
		mu, _ := url.Parse(d.wj.mailURL)
		mcp := &mcpgw.Handler{Store: st, Upstream: mu, Rules: map[string]mcpgw.Rule{"send_email": {Class: run.ClassSideEffect, HonorKey: true}}}
		if d.wj.px != nil {
			d.wj.px.Store = st
			d.wj.px.MCP = mcp
		}
		d.wj.mcp = mcp
	}
	d.mu.Unlock()
	d.startBoth()
	d.broadcast()
	w.WriteHeader(http.StatusAccepted)
}

func (d *Demo) crash(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST", http.StatusMethodNotAllowed)
		return
	}
	d.mu.Lock()
	d.wj.state, d.base.state = "dead", "dead"
	if d.wj.proc != nil {
		d.wj.proc.KillAgent()
	}
	if d.base.proc != nil {
		d.base.proc.KillAgent()
	}
	d.mu.Unlock()
	d.broadcast()
	go func() {
		time.Sleep(400 * time.Millisecond)
		d.start(d.wj)
		d.start(d.base)
		d.broadcast()
	}()
	w.WriteHeader(http.StatusAccepted)
}

func (d *Demo) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no flush", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch := make(chan []byte, 8)
	d.mu.Lock()
	d.subs[ch] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.subs, ch)
		d.mu.Unlock()
	}()
	send := func() {
		b, _ := json.Marshal(d.snap())
		fmt.Fprintf(w, "data: %s\n\n", b)
		fl.Flush()
	}
	send()
	tick := time.NewTicker(80 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			send()
		case b := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		}
	}
}

func (d *Demo) broadcast() {
	b, _ := json.Marshal(d.snap())
	d.mu.Lock()
	defer d.mu.Unlock()
	for ch := range d.subs {
		select {
		case ch <- b:
		default:
		}
	}
}

func (d *Demo) snap() snapshot {
	return snapshot{Wirejournal: d.view(d.wj), Baseline: d.view(d.base)}
}

func (d *Demo) view(l *laneState) LaneView {
	d.mu.Lock()
	state := l.state
	callsAtStart := l.callsAtStart
	var replayed, live, saved int
	if l.journal && l.store != nil {
		if rn, err := l.store.Run("job", false); err == nil {
			s := rn.Status()
			if n := len(s.Attempts); n > 0 {
				a := s.Attempts[n-1]
				replayed, live, saved = a.ModelReplayed, a.ModelLive, a.TokensSaved
			}
		}
	}
	d.mu.Unlock()
	st := l.llm.Stats()
	mail := l.mail.Stats()
	v := LaneView{
		Steps: d.steps, TokensBilled: st.PromptTokens + st.CompletionTokens,
		Deliveries: mail.Deliveries, State: state,
		Replayed: replayed, Live: live, TokensSaved: saved,
	}
	for i, m := range mail.Inbox {
		v.Inbox = append(v.Inbox, Message{To: m.To, Subject: m.Subject, Dup: i > 0})
	}
	if l.journal {
		v.Step = v.Replayed + v.Live
	} else {
		v.Step = st.Calls - callsAtStart
		if v.Step < 0 {
			v.Step = 0
		}
		v.Live = v.Step
	}
	if v.Step > d.steps {
		v.Step = d.steps
	}
	if state == "dead" {
		v.State = "dead"
	} else if v.Replayed > 0 && v.Live == 0 && v.Step < d.steps {
		v.State = "replay"
	} else if v.Replayed > 0 && v.Live > 0 && v.Step < d.steps {
		v.State = "live"
	} else if v.Step >= d.steps && state != "idle" {
		v.State = "done"
	}
	return v
}
