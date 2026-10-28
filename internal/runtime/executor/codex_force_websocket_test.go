package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

type codexForceWebsocketRecorder struct {
	mu           sync.Mutex
	handshakes   int
	httpRequests int
	hints        []string
	tiers        []string
}

func (r *codexForceWebsocketRecorder) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handshakes, r.httpRequests
}

func newCodexForceWebsocketTestServer(t *testing.T, rec *codexForceWebsocketRecorder) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			conn, errUpgrade := upgrader.Upgrade(w, r, nil)
			if errUpgrade != nil {
				t.Errorf("upgrade websocket: %v", errUpgrade)
				return
			}
			rec.mu.Lock()
			rec.handshakes++
			rec.hints = append(rec.hints, r.Header.Get(codexRoutingHintHeader))
			rec.mu.Unlock()
			defer func() { _ = conn.Close() }()
			for {
				_, payload, errRead := conn.ReadMessage()
				if errRead != nil {
					return
				}
				rec.mu.Lock()
				rec.tiers = append(rec.tiers, gjson.GetBytes(payload, "service_tier").String())
				rec.mu.Unlock()
				completed := []byte(`{"type":"response.completed","response":{"id":"resp_force","object":"response","status":"completed","model":"gpt-5.5","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
					return
				}
			}
		}
		rec.mu.Lock()
		rec.httpRequests++
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_http","object":"response","status":"completed","model":"gpt-5.5","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"))
	}))
}

func codexForceWebsocketRequest() cliproxyexecutor.Request {
	return cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`),
	}
}

func codexForceWebsocketOptions(stream bool) cliproxyexecutor.Options {
	return cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       stream,
	}
}

func runCodexForceWebsocketExecute(t *testing.T, exec *CodexAutoExecutor, ctx context.Context, auth *cliproxyauth.Auth, stream bool) error {
	t.Helper()
	if !stream {
		_, errExecute := exec.Execute(ctx, auth, codexForceWebsocketRequest(), codexForceWebsocketOptions(false))
		return errExecute
	}
	result, errExecuteStream := exec.ExecuteStream(ctx, auth, codexForceWebsocketRequest(), codexForceWebsocketOptions(true))
	if errExecuteStream != nil {
		return errExecuteStream
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			return chunk.Err
		}
	}
	return nil
}

func TestCodexForceUpstreamWebsocketRoutesHTTPClientToWebsocket(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "non-stream"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			var rec codexForceWebsocketRecorder
			server := newCodexForceWebsocketTestServer(t, &rec)
			defer server.Close()

			exec := NewCodexAutoExecutor(&config.Config{
				SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
				Codex:     config.CodexConfig{ForceUpstreamWebsocket: true},
			})
			auth := codexOAuthTestAuth(server.URL)
			auth.Attributes["websockets"] = "true"

			if errExec := runCodexForceWebsocketExecute(t, exec, context.Background(), auth, stream); errExec != nil {
				t.Fatalf("Execute(stream=%v) error: %v", stream, errExec)
			}
			handshakes, httpRequests := rec.counts()
			if handshakes != 1 || httpRequests != 0 {
				t.Fatalf("handshakes=%d httpRequests=%d, want 1/0", handshakes, httpRequests)
			}
		})
	}
}

func TestCodexForceUpstreamWebsocketCarriesPriorityTierFromPayloadRule(t *testing.T) {
	var rec codexForceWebsocketRecorder
	server := newCodexForceWebsocketTestServer(t, &rec)
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Codex:     config.CodexConfig{ForceUpstreamWebsocket: true},
		Payload: config.PayloadConfig{Override: []config.PayloadRule{{
			Models: []config.PayloadModelRule{{Name: "gpt-5.5"}},
			Params: map[string]any{"service_tier": "priority"},
		}}},
	})
	auth := codexOAuthTestAuth(server.URL)
	auth.Attributes["websockets"] = "true"

	if errExec := runCodexForceWebsocketExecute(t, exec, context.Background(), auth, true); errExec != nil {
		t.Fatalf("ExecuteStream error: %v", errExec)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.hints) != 1 || rec.hints[0] != "model=gpt-5.5;tier=priority" {
		t.Fatalf("routing hints = %q, want [\"model=gpt-5.5;tier=priority\"]", rec.hints)
	}
	if len(rec.tiers) != 1 || rec.tiers[0] != "priority" {
		t.Fatalf("frame tiers = %q, want [\"priority\"]", rec.tiers)
	}
}

func TestCodexForceUpstreamWebsocketRequiresCredentialOptIn(t *testing.T) {
	var rec codexForceWebsocketRecorder
	server := newCodexForceWebsocketTestServer(t, &rec)
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Codex:     config.CodexConfig{ForceUpstreamWebsocket: true},
	})
	auth := codexOAuthTestAuth(server.URL)

	if errExec := runCodexForceWebsocketExecute(t, exec, context.Background(), auth, true); errExec != nil {
		t.Fatalf("ExecuteStream error: %v", errExec)
	}
	handshakes, httpRequests := rec.counts()
	if handshakes != 0 || httpRequests != 1 {
		t.Fatalf("handshakes=%d httpRequests=%d, want 0/1", handshakes, httpRequests)
	}
}

func TestCodexForceUpstreamWebsocketDisabledKeepsHTTPClientOnHTTP(t *testing.T) {
	var rec codexForceWebsocketRecorder
	server := newCodexForceWebsocketTestServer(t, &rec)
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
	})
	auth := codexOAuthTestAuth(server.URL)
	auth.Attributes["websockets"] = "true"

	if errExec := runCodexForceWebsocketExecute(t, exec, context.Background(), auth, true); errExec != nil {
		t.Fatalf("ExecuteStream error: %v", errExec)
	}
	handshakes, httpRequests := rec.counts()
	if handshakes != 0 || httpRequests != 1 {
		t.Fatalf("handshakes=%d httpRequests=%d, want 0/1", handshakes, httpRequests)
	}
}

func TestCodexForceUpstreamWebsocketRequiredReplayFailsClosedWithoutCredential(t *testing.T) {
	var rec codexForceWebsocketRecorder
	server := newCodexForceWebsocketTestServer(t, &rec)
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{
		SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll},
		Codex:     config.CodexConfig{ForceUpstreamWebsocket: true},
	})
	auth := codexOAuthTestAuth(server.URL)
	ctx := cliproxyexecutor.WithRequiredUpstreamWebsocket(context.Background())

	for _, stream := range []bool{false, true} {
		if errExec := runCodexForceWebsocketExecute(t, exec, ctx, auth, stream); !cliproxyexecutor.IsUpstreamWebsocketReplayRequired(errExec) {
			t.Fatalf("Execute(stream=%v) error = %v, want replay required", stream, errExec)
		}
	}
	handshakes, httpRequests := rec.counts()
	if handshakes != 0 || httpRequests != 0 {
		t.Fatalf("handshakes=%d httpRequests=%d, want 0/0", handshakes, httpRequests)
	}
}

func TestCodexAutoExecutorShouldUseUpstreamWebsocketNilSafe(t *testing.T) {
	var nilExec *CodexAutoExecutor
	if nilExec.shouldUseUpstreamWebsocket(context.Background(), nil) {
		t.Fatal("nil executor must not route to websocket")
	}

	nilCfgExec := NewCodexAutoExecutor(nil)
	auth := &cliproxyauth.Auth{Attributes: map[string]string{}}
	if nilCfgExec.shouldUseUpstreamWebsocket(context.Background(), auth) {
		t.Fatal("nil config must not force the websocket upstream")
	}

	exec := NewCodexAutoExecutor(&config.Config{Codex: config.CodexConfig{ForceUpstreamWebsocket: true}})
	if exec.shouldUseUpstreamWebsocket(context.Background(), nil) {
		t.Fatal("nil auth must not route to websocket")
	}
	if exec.shouldUseUpstreamWebsocket(context.Background(), auth) {
		t.Fatal("force-upstream-websocket must require credential websockets")
	}
	auth.Attributes["websockets"] = "false"
	if exec.shouldUseUpstreamWebsocket(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), auth) {
		t.Fatal("disabled credential websockets must win over a downstream websocket")
	}
	auth.Attributes["websockets"] = "true"
	if !exec.shouldUseUpstreamWebsocket(context.Background(), auth) {
		t.Fatal("force-upstream-websocket should route an HTTP client to websocket")
	}
	if !exec.shouldUseUpstreamWebsocket(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), auth) {
		t.Fatal("downstream websocket should route to websocket")
	}
}
