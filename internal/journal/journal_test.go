package journal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func collect(t *testing.T, path string) ([][]byte, Report) {
	t.Helper()
	var got [][]byte
	l, rep, err := Open(path, func(off int64, p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return got, rep
}

func appendAll(t *testing.T, l *Log, recs ...string) {
	t.Helper()
	for _, r := range recs {
		if err := l.Append([]byte(r)); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
}

func writeJournal(t *testing.T, recs ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "run.wj")
	l, _, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, l, recs...)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func asStrings(b [][]byte) []string {
	s := make([]string, len(b))
	for i, x := range b {
		s[i] = string(x)
	}
	return s
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wj")
	l, rep, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Created || rep.Records != 0 {
		t.Fatalf("first open report = %+v, want Created and 0 records", rep)
	}
	appendAll(t, l, "one", "two", strings.Repeat("x", 100_000))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}

	got, rep := collect(t, path)
	want := []string{"one", "two", strings.Repeat("x", 100_000)}
	if fmt.Sprint(asStrings(got)) != fmt.Sprint(want) {
		t.Fatalf("records = %q, want %q", got, want)
	}
	if rep.Created || rep.Records != 3 || rep.TruncatedBytes != 0 {
		t.Fatalf("reopen report = %+v", rep)
	}
}

func TestAppendRejectsBadSize(t *testing.T) {
	l, _, err := OpenFile(NewCrashFile(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil); !errors.Is(err, ErrRecordSize) {
		t.Fatalf("empty Append = %v, want ErrRecordSize", err)
	}
	if err := l.Append(make([]byte, MaxRecord+1)); !errors.Is(err, ErrRecordSize) {
		t.Fatalf("oversized Append = %v, want ErrRecordSize", err)
	}
	if err := l.Append([]byte("ok")); err != nil {
		t.Fatalf("Append after a rejected size must still work: %v", err)
	}
}

// Every prefix of a valid journal is what a crash during the last write can
// leave behind. Each must open, keep exactly the complete frames, and cut
// the rest.
func TestTornTailAtEveryOffset(t *testing.T) {
	recs := []string{"alpha", "bravo-bravo", strings.Repeat("c", 300)}
	full, err := os.ReadFile(writeJournal(t, recs...))
	if err != nil {
		t.Fatal(err)
	}
	var bounds []int
	off := len(fileHeader)
	for _, r := range recs {
		off += frameHeaderSize + len(r)
		bounds = append(bounds, off)
	}

	for cut := 0; cut <= len(full); cut++ {
		path := filepath.Join(t.TempDir(), "cut.wj")
		if err := os.WriteFile(path, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		got, rep := collect(t, path)

		complete, boundary := 0, len(fileHeader)
		for i, b := range bounds {
			if b <= cut {
				complete, boundary = i+1, b
			}
		}
		if cut < len(fileHeader) {
			if !rep.Created || len(got) != 0 {
				t.Fatalf("cut=%d: partial header must reinitialize, got report %+v", cut, rep)
			}
			continue
		}
		if fmt.Sprint(asStrings(got)) != fmt.Sprint(recs[:complete]) {
			t.Fatalf("cut=%d: records = %q, want %q", cut, got, recs[:complete])
		}
		if rep.TruncatedBytes != int64(cut-boundary) {
			t.Fatalf("cut=%d: truncated %d bytes, want %d", cut, rep.TruncatedBytes, cut-boundary)
		}
		st, _ := os.Stat(path)
		if st.Size() != int64(boundary) {
			t.Fatalf("cut=%d: file size after recovery = %d, want %d", cut, st.Size(), boundary)
		}
	}
}

func TestAppendAfterTornTailRecovery(t *testing.T) {
	path := writeJournal(t, "a", "b")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("WJR1\x05\x00")) // a frame header cut mid-length
	f.Close()

	l, rep, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.TruncatedBytes != 6 {
		t.Fatalf("truncated %d, want 6", rep.TruncatedBytes)
	}
	appendAll(t, l, "c")
	l.Close()

	got, _ := collect(t, path)
	if fmt.Sprint(asStrings(got)) != "[a b c]" {
		t.Fatalf("records = %q", got)
	}
}

func TestBitFlipInLastRecordIsTruncated(t *testing.T) {
	path := writeJournal(t, "first", "second", "third")
	b, _ := os.ReadFile(path)
	b[len(b)-2] ^= 0x10
	os.WriteFile(path, b, 0o644)

	got, rep := collect(t, path)
	if fmt.Sprint(asStrings(got)) != "[first second]" {
		t.Fatalf("records = %q", got)
	}
	if rep.TruncatedBytes != int64(frameHeaderSize+len("third")) {
		t.Fatalf("truncated %d bytes", rep.TruncatedBytes)
	}
}

func TestBitFlipInMiddleRecordIsCorrupt(t *testing.T) {
	for _, target := range []string{"payload", "length", "magic"} {
		t.Run(target, func(t *testing.T) {
			path := writeJournal(t, "first", "second", "third")
			b, _ := os.ReadFile(path)
			second := len(fileHeader) + frameHeaderSize + len("first")
			switch target {
			case "payload":
				b[second+frameHeaderSize+1] ^= 0x01
			case "length":
				b[second+4] ^= 0x01
			case "magic":
				b[second] ^= 0x01
			}
			os.WriteFile(path, b, 0o644)

			_, _, err := Open(path, nil)
			var ce *CorruptError
			if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open = %v, want ErrCorrupt", err)
			}
			if ce.Offset != int64(second) {
				t.Fatalf("corrupt offset = %d, want %d", ce.Offset, second)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(after, b) {
				t.Fatal("Open changed a corrupt file")
			}
		})
	}
}

// Filesystems can extend a file with zeros when power fails after a size
// update but before the data blocks land.
func TestZeroFilledTailIsTorn(t *testing.T) {
	path := writeJournal(t, "a", "b")
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(make([]byte, 4096))
	f.Close()

	got, rep := collect(t, path)
	if len(got) != 2 || rep.TruncatedBytes != 4096 {
		t.Fatalf("records = %q, report = %+v", got, rep)
	}
}

func TestBadHeader(t *testing.T) {
	for name, content := range map[string]string{
		"wrong magic":      "NOTAJRNL",
		"wrong version":    "WJNL0002",
		"short wrong data": "WJx",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.wj")
			os.WriteFile(path, []byte(content), 0o644)
			if _, _, err := Open(path, nil); !errors.Is(err, ErrBadHeader) {
				t.Fatalf("Open = %v, want ErrBadHeader", err)
			}
			after, _ := os.ReadFile(path)
			if string(after) != content {
				t.Fatal("Open changed a file with a bad header")
			}
		})
	}
}

func TestVisitErrorAbortsOpenAndReleasesLock(t *testing.T) {
	path := writeJournal(t, "a", "b")
	boom := errors.New("bad record")
	_, _, err := Open(path, func(off int64, p []byte) error {
		if string(p) == "b" {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Open = %v, want the visit error", err)
	}
	if got, _ := collect(t, path); len(got) != 2 {
		t.Fatalf("reopen after visit error got %d records", len(got))
	}
}

func TestVisitSeesOffsetsInOrder(t *testing.T) {
	path := writeJournal(t, "a", "bb", "ccc")
	var offs []int64
	l, _, err := Open(path, func(off int64, p []byte) error {
		offs = append(offs, off)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	h := int64(len(fileHeader))
	want := []int64{h, h + frameHeaderSize + 1, h + 2*frameHeaderSize + 3}
	if fmt.Sprint(offs) != fmt.Sprint(want) {
		t.Fatalf("offsets = %v, want %v", offs, want)
	}
}

func TestSyncFailurePoisons(t *testing.T) {
	cf := NewCrashFile()
	l, _, err := OpenFile(cf, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, l, "ok")
	cf.FailSync = errors.New("EIO")
	if err := l.Append([]byte("lost")); err == nil {
		t.Fatal("Append with failing fsync returned nil")
	}
	cf.FailSync = nil
	if err := l.Append([]byte("after")); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Append after fsync failure = %v, want ErrPoisoned", err)
	}
}

func TestWriteFailurePoisons(t *testing.T) {
	cf := NewCrashFile()
	l, _, err := OpenFile(cf, nil)
	if err != nil {
		t.Fatal(err)
	}
	cf.FailWrite = errors.New("ENOSPC")
	if err := l.Append([]byte("x")); err == nil {
		t.Fatal("Append with failing write returned nil")
	}
	cf.FailWrite = nil
	if err := l.Append([]byte("y")); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("Append after write failure = %v, want ErrPoisoned", err)
	}
}

// Power loss model: every acknowledged Append must survive, the record in
// flight at the moment of power loss may or may not, and nothing else may
// appear. Recovery must never fail on a power-loss image.
func TestPowerLossProperty(t *testing.T) {
	for seed := int64(0); seed < 2000; seed++ {
		rng := rand.New(rand.NewSource(seed))
		cf := NewCrashFile()
		l, _, err := OpenFile(cf, nil)
		if err != nil {
			t.Fatal(err)
		}

		var acked []string
		n := rng.Intn(12)
		for i := 0; i < n; i++ {
			rec := fmt.Sprintf("s%d-r%d-%s", seed, i, strings.Repeat("z", rng.Intn(200)))
			appendAll(t, l, rec)
			acked = append(acked, rec)
		}
		inflight := fmt.Sprintf("s%d-inflight-%s", seed, strings.Repeat("q", rng.Intn(200)))
		cf.FailSync = errors.New("power cut")
		if err := l.Append([]byte(inflight)); err == nil {
			t.Fatalf("seed %d: Append during power cut returned nil", seed)
		}
		tear := rng.Intn(frameHeaderSize + len(inflight) + 20)
		cf.LosePower(tear, rng.Intn(2) == 0)
		cf.FailSync = nil

		var got []string
		l2, _, err := OpenFile(cf, func(off int64, p []byte) error {
			got = append(got, string(p))
			return nil
		})
		if err != nil {
			t.Fatalf("seed %d: recovery failed on a power-loss image: %v", seed, err)
		}
		wantA := fmt.Sprint(acked)
		wantB := fmt.Sprint(append(append([]string(nil), acked...), inflight))
		if g := fmt.Sprint(got); g != wantA && g != wantB {
			t.Fatalf("seed %d: recovered %q, want acked records %q (optionally + in-flight)", seed, got, acked)
		}
		appendAll(t, l2, "post-recovery")
	}
}

func TestCrashFileDropsUnsyncedTruncate(t *testing.T) {
	cf := NewCrashFile()
	cf.WriteAt([]byte("abcdef"), 0)
	cf.Sync()
	cf.Truncate(2)
	cf.LosePower(0, false)
	if n, _ := cf.Size(); n != 6 {
		t.Fatalf("size after losing an unsynced truncate = %d, want 6", n)
	}
}

func TestConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wj")
	l, _, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := l.Append([]byte(fmt.Sprintf("w%d-%03d", w, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	l.Close()

	got, _ := collect(t, path)
	if len(got) != writers*each {
		t.Fatalf("got %d records, want %d", len(got), writers*each)
	}
	next := map[string]int{}
	for _, r := range got {
		var w, i int
		if _, err := fmt.Sscanf(string(r), "w%d-%d", &w, &i); err != nil {
			t.Fatalf("garbled record %q", r)
		}
		key := fmt.Sprint(w)
		if i != next[key] {
			t.Fatalf("writer %d: record %d out of order (want %d)", w, i, next[key])
		}
		next[key]++
	}
}

func TestLockedInProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wj")
	l, _, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	l.Close()
	l2, _, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open after Close = %v", err)
	}
	l2.Close()
}

const helperEnv = "WJ_JOURNAL_HELPER_PATH"

// TestHelperHoldLock is not a test. TestLockReleasedOnKill9 runs it in a
// child process that appends a record, reports, and blocks until killed.
func TestHelperHoldLock(t *testing.T) {
	path := os.Getenv(helperEnv)
	if path == "" {
		t.Skip("helper process only")
	}
	l, _, err := Open(path, nil)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
	if err := l.Append([]byte("from-child")); err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
	fmt.Println("ready")
	time.Sleep(time.Minute)
	os.Exit(3)
}

func TestLockReleasedOnKill9(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wj")
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldLock$")
	cmd.Env = append(os.Environ(), helperEnv+"="+path)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := out.Read(buf)
	if !strings.Contains(string(buf[:n]), "ready") {
		cmd.Process.Kill()
		t.Fatalf("helper said %q", buf[:n])
	}

	if _, _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open while child holds the lock = %v, want ErrLocked", err)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()

	got, _ := collect(t, path)
	if fmt.Sprint(asStrings(got)) != "[from-child]" {
		t.Fatalf("records after kill -9 = %q, want the child's acknowledged record", got)
	}
}
