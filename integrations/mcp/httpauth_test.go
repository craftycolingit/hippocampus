package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/fastbean-au/hippocampus/observability"
)

// The streamable-HTTP transport hands whoever reaches it the bridge's own service token - by default
// a writer, which can delete memories. It used to listen on every interface with no inbound check,
// and the compose profile published it on 0.0.0.0 (TODO-3 item 160).

func TestRegisterFlags_HTTPAddressDefaultsToLoopback(t *testing.T) {
	resetViper(t)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	if err := registerFlags(fs, nil); err != nil {
		t.Fatalf("registerFlags: %v", err)
	}

	if got := viper.GetString("http-address"); got != "127.0.0.1:8090" {
		t.Errorf("http-address default = %q, want 127.0.0.1:8090", got)
	}
}

func TestHTTPAuth_RequiresTheConfiguredToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served")
	})

	handler := requireBearer("s3cret", next)

	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"wrong token", "Bearer nope", http.StatusUnauthorized},
		{"wrong scheme", "Basic s3cret", http.StatusUnauthorized},
		{"a prefix of the token", "Bearer s3cre", http.StatusUnauthorized},
		{"the token", "Bearer s3cret", http.StatusOK},
		{"the scheme in another case", "bearer s3cret", http.StatusOK},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))

			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d", rec.Code, c.want)
			}

			if c.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("a 401 carries no WWW-Authenticate challenge")
			}
		})
	}
}

func TestHTTPAuth_NoTokenServesUnchanged(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	rec := httptest.NewRecorder()
	requireBearer("", next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want the wrapped handler's own", rec.Code)
	}
}

// TestServeHTTP_RefusesAnUnauthenticatedNonLoopbackBind: with no inbound token, only a loopback
// listener is served unless the operator says otherwise - an exposed one is anyone on the network
// holding the bridge's service token.
func TestServeHTTP_RefusesAnUnauthenticatedNonLoopbackBind(t *testing.T) {
	server := newServer(newBridge(&fakeClient{}), "test")

	refused := []string{":8090", "0.0.0.0:8090", "[::]:8090", "192.0.2.10:8090", "mcp.example.com:8090"}

	for _, address := range refused {
		err := serveHTTP(context.Background(), server, httpConfig{address: address})
		if err == nil || !strings.Contains(err.Error(), "--http-token") {
			t.Errorf("serveHTTP(%q) with no token = %v, want a refusal naming --http-token", address, err)
		}
	}
}

func TestIsLoopbackAddress(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8090": true,
		"127.0.0.2:8090": true,
		"[::1]:8090":     true,
		"localhost:8090": true,
		":8090":          false,
		"0.0.0.0:8090":   false,
		"[::]:8090":      false,
		"10.0.0.1:8090":  false,
		"example.com:80": false,
		"not-an-address": false,
	}

	for address, want := range cases {
		if got := isLoopbackAddress(address); got != want {
			t.Errorf("isLoopbackAddress(%q) = %v, want %v", address, got, want)
		}
	}
}

// TestRun_HTTPTokenIsEnforcedEndToEnd drives the real listener: the token reaches the handler the
// transport serves, not merely a helper.
func TestRun_HTTPTokenIsEnforcedEndToEnd(t *testing.T) {
	server := newServer(newBridge(&fakeClient{}), "test")

	handler := httpHandler(server, "s3cret", nil)

	ts := httptest.NewServer(handler)
	defer ts.Close()

	res, err := http.Post(ts.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	_ = res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("an unauthenticated MCP request = %d, want 401", res.StatusCode)
	}
}

// TestServeHTTP_AllowUnauthenticatedServesAnExposedBind: the opt-out serves (with a warning) rather
// than refusing, for a deployment whose network is the boundary.
func TestServeHTTP_AllowUnauthenticatedServesAnExposedBind(t *testing.T) {
	server := newServer(newBridge(&fakeClient{}), "test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := serveHTTP(ctx, server, httpConfig{address: ":0", allowUnauthenticated: true}); err != nil {
		t.Errorf("serveHTTP with --allow-unauthenticated-http = %v, want it served", err)
	}
}

// TestHTTPHandler_ServesLifecycleProbesWithoutTheToken: a kubelet probe carries no credentials, so
// /livez, /readyz and /healthz answer ahead of the token check while the MCP endpoint still demands
// it, and /readyz reports the Hippocampus end of the bridge.
func TestHTTPHandler_ServesLifecycleProbesWithoutTheToken(t *testing.T) {
	server := newServer(newBridge(&fakeClient{}), "test")

	var backendDown atomic.Bool

	health := observability.NewHealthServer(observability.HealthConfig{
		Component: "hippocampus-mcp",
		CacheTTL:  time.Nanosecond,
		Checks: map[string]observability.Check{
			"hippocampus": func(context.Context) error {
				if backendDown.Load() {
					return errors.New("connection refused")
				}

				return nil
			},
		},
	})

	ts := httptest.NewServer(httpHandler(server, "s3cret", health))
	defer ts.Close()

	get := func(path string) int {
		t.Helper()

		res, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}

		_ = res.Body.Close()

		return res.StatusCode
	}

	for _, path := range []string{"/livez", "/readyz", "/healthz"} {
		if code := get(path); code != http.StatusOK {
			t.Errorf("GET %s with no token = %d, want 200", path, code)
		}
	}

	if code := get("/"); code != http.StatusUnauthorized {
		t.Errorf("GET / with no token = %d, want 401", code)
	}

	backendDown.Store(true)

	if code := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz with the backend down = %d, want 503", code)
	}

	if code := get("/livez"); code != http.StatusOK {
		t.Errorf("GET /livez with the backend down = %d, want 200", code)
	}

	health.Drain()
	backendDown.Store(false)

	if code := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz while draining = %d, want 503", code)
	}
}
