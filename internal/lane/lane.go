// Package lane starts, kills, and restarts one agent (and optionally its
// proxy) as OS processes so crashes are real SIGKILLs.
package lane

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type Spec struct {
	Bin, Data   string
	LLM, MCP    string
	RunID       string
	Attempt     string
	Journal     bool
	Stream      bool
	Config      string
	ExtraEnv    []string
	ProxyListen string
	// ExistingProxy, when set, skips starting a proxy and points the agent at it.
	ExistingProxy string
}

type Proc struct {
	ProxyURL string
	proxy    *exec.Cmd
	agent    *exec.Cmd
}

func FreeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr, nil
}

func Start(s Spec) (*Proc, error) {
	p := &Proc{}
	llm, mcp := s.LLM, s.MCP
	if s.ExistingProxy != "" {
		llm = s.ExistingProxy + "/r/" + s.RunID + "/a/" + s.Attempt + "/v1"
		mcp = s.ExistingProxy + "/r/" + s.RunID + "/a/" + s.Attempt + "/mcp"
	} else if s.Journal {
		addr := s.ProxyListen
		if addr == "" {
			a, err := FreeAddr()
			if err != nil {
				return nil, err
			}
			addr = a
		}
		args := []string{"proxy", "-listen", addr, "-data", s.Data, "-llm-upstream", s.LLM, "-mcp-upstream", s.MCP}
		if s.Config != "" {
			args = append(args, "-config", s.Config)
		}
		p.proxy = exec.Command(s.Bin, args...)
		p.proxy.Env = append(os.Environ(), s.ExtraEnv...)
		p.proxy.Stdout, p.proxy.Stderr = stdio()
		if err := p.proxy.Start(); err != nil {
			return nil, err
		}
		p.ProxyURL = "http://" + addr
		if err := waitHTTP(p.ProxyURL+"/healthz", 5*time.Second); err != nil {
			p.Kill()
			return nil, fmt.Errorf("proxy: %w", err)
		}
		llm = p.ProxyURL + "/r/" + s.RunID + "/a/" + s.Attempt + "/v1"
		mcp = p.ProxyURL + "/r/" + s.RunID + "/a/" + s.Attempt + "/mcp"
	}
	stream := "true"
	if !s.Stream {
		stream = "false"
	}
	p.agent = exec.Command(s.Bin, "agent", "-llm", llm, "-mcp", mcp, "-stream", stream)
	p.agent.Env = append(os.Environ(), s.ExtraEnv...)
	p.agent.Stdout, p.agent.Stderr = stdio()
	if err := p.agent.Start(); err != nil {
		p.Kill()
		return nil, err
	}
	return p, nil
}

func (p *Proc) Kill() {
	kill(p.agent)
	kill(p.proxy)
}

func (p *Proc) KillAgent() { kill(p.agent) }

func (p *Proc) WaitAgent() error {
	if p.agent == nil {
		return nil
	}
	return p.agent.Wait()
}

func kill(c *exec.Cmd) {
	if c == nil || c.Process == nil {
		return
	}
	_ = c.Process.Signal(syscall.SIGKILL)
	_, _ = c.Process.Wait()
}

func stdio() (io.Writer, io.Writer) {
	if os.Getenv("WJ_LANE_LOG") != "" {
		return os.Stdout, os.Stderr
	}
	return io.Discard, io.Discard
}

func waitHTTP(url string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}
