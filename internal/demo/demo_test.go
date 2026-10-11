package demo

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shehbaz0101/wirejournal/internal/bench"
)

func TestDemoCrashAndResume(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Setenv("WJ_STEPS", "8")
	bin, err := bench.Build(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(bin, 8)
	if err != nil {
		t.Fatal(err)
	}
	d.startBoth()
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "ok" {
		t.Fatalf("healthz %s", b)
	}
	resp, err = http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(page), "Crash (kill -9)") {
		t.Fatal("ui missing crash button")
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		s := getStatus(t, srv.URL)
		if s.Wirejournal.Deliveries >= 1 && s.Baseline.Deliveries >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("email never sent: %+v", s)
		}
		time.Sleep(40 * time.Millisecond)
	}
	before := getStatus(t, srv.URL)
	resp, err = http.Post(srv.URL+"/crash", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	deadline = time.Now().Add(25 * time.Second)
	for {
		s := getStatus(t, srv.URL)
		if s.Wirejournal.State == "done" && s.Wirejournal.Step >= 8 && s.Baseline.State == "done" {
			if s.Wirejournal.Deliveries != 1 {
				t.Fatalf("wj inbox %d", s.Wirejournal.Deliveries)
			}
			if s.Baseline.Deliveries != 2 {
				t.Fatalf("baseline inbox %d want 2 (duplicate send) before=%+v after=%+v", s.Baseline.Deliveries, before, s.Baseline)
			}
			if s.Wirejournal.Replayed < 6 {
				t.Fatalf("wj did not replay the prefix: %+v before=%+v", s.Wirejournal, before)
			}
			if s.Wirejournal.TokensBilled != 336 {
				t.Fatalf("wj tokens=%d want 336 (one run) before=%+v", s.Wirejournal.TokensBilled, before)
			}
			if s.Baseline.TokensBilled <= 336 {
				t.Fatalf("baseline did not re-bill: tokens=%d", s.Baseline.TokensBilled)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not finish after crash: %+v", s)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func getStatus(t *testing.T, base string) snapshot {
	t.Helper()
	resp, err := http.Get(base + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
