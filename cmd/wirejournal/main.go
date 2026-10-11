package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/shehbaz0101/wirejournal/internal/agent"
	"github.com/shehbaz0101/wirejournal/internal/llmproxy"
	"github.com/shehbaz0101/wirejournal/internal/mcpgw"
	"github.com/shehbaz0101/wirejournal/internal/mockllm"
	"github.com/shehbaz0101/wirejournal/internal/mockmail"
	"github.com/shehbaz0101/wirejournal/internal/proxy"
	"github.com/shehbaz0101/wirejournal/internal/run"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "proxy":
		err = cmdProxy(os.Args[2:])
	case "mockllm":
		err = cmdListen(os.Args[2:], &mockllm.Server{})
	case "mockmail":
		err = cmdListen(os.Args[2:], &mockmail.Server{})
	case "agent":
		err = cmdAgent(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: wirejournal <proxy|mockllm|mockmail|agent|bench|demo> [flags]\n")
}

func cmdProxy(args []string) error {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:7070", "bind address")
	data := fs.String("data", "./wj-data", "journal directory")
	llm := fs.String("llm-upstream", "", "OpenAI-compatible origin, e.g. https://api.openai.com")
	mcp := fs.String("mcp-upstream", "", "upstream MCP server URL")
	cfg := fs.String("config", "", "optional wirejournal.json")
	fs.Parse(args)
	if *llm == "" || *mcp == "" {
		return fmt.Errorf("proxy: -llm-upstream and -mcp-upstream are required")
	}
	lu, err := url.Parse(*llm)
	if err != nil {
		return err
	}
	mu, err := url.Parse(*mcp)
	if err != nil {
		return err
	}
	rules, tools, err := proxy.LoadConfig(*cfg)
	if err != nil {
		return err
	}
	st, err := run.NewStore(*data)
	if err != nil {
		return err
	}
	defer st.Close()
	s := &proxy.Server{
		Store: st,
		LLM:   &llmproxy.Handler{Upstream: lu, Rules: rules},
		MCP:   &mcpgw.Handler{Store: st, Upstream: mu, Rules: tools},
	}
	log.Printf("wirejournal proxy on %s", *listen)
	return http.ListenAndServe(*listen, s)
}

func cmdListen(args []string, h http.Handler) error {
	fs := flag.NewFlagSet("listen", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "bind address")
	fs.Parse(args)
	log.Printf("listen %s", *listen)
	return http.ListenAndServe(*listen, h)
}

func cmdAgent(args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	llm := fs.String("llm", env("OPENAI_BASE_URL", "http://127.0.0.1:7070/r/job/v1"), "chat completions base URL")
	mcp := fs.String("mcp", env("MCP_URL", "http://127.0.0.1:7070/r/job/mcp"), "MCP URL")
	stream := fs.Bool("stream", true, "use SSE chat completions")
	fs.Parse(args)
	res, err := agent.Run(agent.Config{LLM: *llm, MCP: *mcp, Auth: os.Getenv("OPENAI_API_KEY"), Stream: *stream})
	if err != nil {
		return err
	}
	fmt.Printf("model_calls=%d tool_calls=%d emails=%d last=%q\n", res.ModelCalls, res.ToolCalls, res.Emails, res.LastText)
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
