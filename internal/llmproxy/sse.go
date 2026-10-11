package llmproxy

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/shehbaz0101/wirejournal/internal/run"
)

const doneData = "[DONE]"

// splitSSE pulls complete events out of buf. An event ends at a blank line
// (\n\n or \r\n\r\n). leftover is a partial event. done is the [DONE]
// terminator when it was the last complete event.
func splitSSE(buf []byte) (frames [][]byte, leftover, done []byte) {
	for {
		n, ok := eventLen(buf)
		if !ok {
			return frames, buf, done
		}
		ev := append([]byte(nil), buf[:n]...)
		buf = buf[n:]
		if isDone(ev) {
			done = ev
			continue
		}
		frames = append(frames, ev)
	}
}

func eventLen(b []byte) (int, bool) {
	if i := bytes.Index(b, []byte("\r\n\r\n")); i >= 0 {
		return i + 4, true
	}
	if i := bytes.Index(b, []byte("\n\n")); i >= 0 {
		return i + 2, true
	}
	return 0, false
}

func isDone(ev []byte) bool {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if rest, ok := bytes.CutPrefix(line, []byte("data:")); ok && strings.TrimSpace(string(rest)) == doneData {
			return true
		}
	}
	return false
}

func usageFromJSON(b []byte) run.Usage {
	var wrap struct {
		Usage *struct {
			Prompt     int `json:"prompt_tokens"`
			Completion int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &wrap) != nil || wrap.Usage == nil {
		return run.Usage{}
	}
	return run.Usage{Prompt: wrap.Usage.Prompt, Completion: wrap.Usage.Completion}
}

func usageFromEvent(ev []byte) run.Usage {
	var u run.Usage
	for _, line := range bytes.Split(ev, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		rest, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		if g := usageFromJSON(bytes.TrimSpace(rest)); g.Total() > 0 {
			u = g
		}
	}
	return u
}

func readAll(r io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}
