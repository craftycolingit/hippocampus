package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

// The lifecycle endpoint paths, shared by the service's gateway and every client daemon so a probe
// written for one works on all of them.
const (
	LivenessPath     = "/livez"
	ReadinessPath    = "/readyz"
	LegacyHealthPath = "/healthz"
)

// StatusShuttingDown is the /readyz status of a process that has begun shutting down.
const StatusShuttingDown = "shutting down"

// Health endpoint defaults. The timeouts mirror the service's own gateway server, so a probe
// endpoint cannot be held open indefinitely.
const (
	defaultCheckTimeout  = 2 * time.Second
	defaultCacheTTL      = 3 * time.Second
	defaultReadTimeout   = 5 * time.Second
	defaultWriteTimeout  = 10 * time.Second
	defaultIdleTimeout   = 60 * time.Second
	defaultHeaderTimeout = 5 * time.Second
)

// Check reports whether one dependency is usable. A nil error means ready.
type Check func(ctx context.Context) error

// HealthConfig configures the probe listener.
type HealthConfig struct {
	// Port is the TCP port to serve on. Zero disables the listener entirely, matching the service's
	// gateway.port convention.
	Port int

	// BindAddress restricts the interface; empty binds all of them.
	BindAddress string

	// Version is reported in the /livez, /healthz and /readyz bodies, as the service's own do.
	Version string

	// Component names the binary in log lines and in the probe bodies.
	Component string

	// Checks are the readiness checks, keyed by the dependency name reported in the /readyz body.
	// An empty map makes /readyz equivalent to /livez until Drain is called - honest for a component
	// with no dependencies, rather than a readiness endpoint that silently means nothing.
	Checks map[string]Check

	// CheckTimeout bounds one check; CacheTTL is how long a result is reused. Non-positive values
	// select the package defaults.
	CheckTimeout time.Duration
	CacheTTL     time.Duration

	// MetricsHandler, when non-nil, is served at DefaultMetricsPath alongside the probes - this is
	// how the client daemons expose a Prometheus scrape endpoint. It rides on the probe listener
	// rather than a port of its own because these processes are already asking an operator for one
	// port they did not previously need, and everything served here is the same kind of thing: a
	// private operational surface, never the component's actual work. The service takes the other
	// route (observability.MetricsServer) because its own listener is the public API.
	//
	// It is a handler rather than a bool so the caller decides - PrometheusHandler returns nil when
	// no scrape reader was installed, and a nil handler mounts no route at all rather than one that
	// answers with nothing.
	MetricsHandler http.Handler
}

// HealthServer serves the Kubernetes lifecycle endpoints for the client-side daemons - the
// ingestor, the broker bridges, the object gateway and reaper, and the MCP bridge over HTTP -
// which otherwise give an orchestrator nothing to probe:
//
//   - /livez answers "is this process alive", so a Hippocampus instance being briefly unreachable
//     must NOT make it fail and get the container kill-looped. It is the liveness and startup
//     probe's target.
//   - /readyz answers "can this process do its job right now", which for a bridge or an ingestor
//     means "can I reach both ends". It also fails from the moment Drain is called, so a shutting
//     down process is taken out of rotation while it finishes its in-flight work.
//   - /healthz is /livez under its older name. Kubernetes itself deprecated it in favour of the
//     split pair, but every probe and script written before /livez existed points at it.
//
// The split is the same one the service makes on its gateway.
type HealthServer struct {
	cfg    HealthConfig
	server *http.Server

	// draining is set by Drain and never cleared: a process that has begun shutting down does not
	// become ready again.
	draining atomic.Bool

	mu      sync.Mutex
	results map[string]checkResult
}

// checkResult caches one check's outcome so a burst of probes collapses into at most one call per
// CacheTTL - a readiness probe must not become its own load on the thing it is probing.
type checkResult struct {
	err       error
	checkedAt time.Time
}

// NewHealthServer builds the probe server. It does not listen until Start is called.
func NewHealthServer(cfg HealthConfig) *HealthServer {
	if cfg.CheckTimeout <= 0 {
		cfg.CheckTimeout = defaultCheckTimeout
	}

	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = defaultCacheTTL
	}

	if cfg.Component == "" {
		cfg.Component = "hippocampus"
	}

	return &HealthServer{cfg: cfg, results: map[string]checkResult{}}
}

// Handler is the probe mux, exported so a test can drive the endpoints without binding a port.
func (h *HealthServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(LivenessPath, h.liveness)
	mux.HandleFunc(LegacyHealthPath, h.liveness)
	mux.HandleFunc(ReadinessPath, h.readiness)

	if h.cfg.MetricsHandler != nil {
		mux.Handle(DefaultMetricsPath, h.cfg.MetricsHandler)
	}

	return mux
}

// Start binds the listener and serves in the background. A zero port disables the server and
// returns nil, so a caller need not branch. The bind failure is returned rather than logged,
// because a probe port already in use is a configuration error worth failing startup on: silently
// running without probes is how a deployment ends up believing it has health checks it does not.
func (h *HealthServer) Start() error {
	if h.cfg.Port == 0 {
		log.Warnf("%s: health endpoints disabled (port 0): no /livez, /readyz or /healthz for an orchestrator to probe", h.cfg.Component)

		// Said separately, because the scrape endpoint is served from this listener: turning the
		// probe port off silently takes the metrics with it, and a scraper's view of that is
		// indistinguishable from a process that is not running.
		if h.cfg.MetricsHandler != nil {
			log.Warnf("%s: the Prometheus scrape endpoint is served on the health port, so it is disabled too", h.cfg.Component)
		}

		return nil
	}

	address := fmt.Sprintf("%s:%d", h.cfg.BindAddress, h.cfg.Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("binding the health listener on %s: %w", address, err)
	}

	h.server = &http.Server{
		Handler:           h.Handler(),
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		ReadHeaderTimeout: defaultHeaderTimeout,
	}

	go func() {
		if err := h.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("%s: health listener stopped: %s", h.cfg.Component, err.Error())
		}
	}()

	paths := LivenessPath + ", " + ReadinessPath + ", " + LegacyHealthPath

	if h.cfg.MetricsHandler != nil {
		paths += ", " + DefaultMetricsPath
	}

	log.Infof("%s: health endpoints on %s (%s)", h.cfg.Component, address, paths)

	return nil
}

// Drain marks the process as shutting down: /readyz answers 503 from now on, whatever its checks
// say, while /livez keeps answering 200. A daemon calls it as soon as its shutdown signal arrives,
// so a load balancer or a Service stops routing new work to it before its listeners close. It is
// safe to call more than once, and on a server that was never started.
func (h *HealthServer) Drain() {
	if h.draining.Swap(true) {
		return
	}

	log.Infof("%s: draining - /readyz now reports not ready", h.cfg.Component)
}

// DrainWhenDone calls Drain as soon as ctx is done. A daemon passes its signal context, so /readyz
// starts failing the moment SIGTERM arrives rather than only once its serving loop has unwound and
// it reaches Shutdown - which, for a daemon finishing in-flight work, can be several seconds later.
func (h *HealthServer) DrainWhenDone(ctx context.Context) {
	context.AfterFunc(ctx, h.Drain)
}

// Shutdown drains (see Drain) and stops the listener. It is safe to call when Start was a no-op.
func (h *HealthServer) Shutdown(ctx context.Context) error {
	h.Drain()

	if h.server == nil {
		return nil
	}

	return h.server.Shutdown(ctx)
}

// liveness answers only "is this process running", so a probe never fails because a dependency is
// briefly unreachable - that is what /readyz is for. It serves both /livez and /healthz.
func (h *HealthServer) liveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "ok",
		"component": h.cfg.Component,
		"version":   h.cfg.Version,
	})
}

// readiness runs every check and reports 200 only when all of them pass, with a per-dependency
// breakdown so a failing probe says WHICH end is unreachable rather than only that one is. Once
// draining it answers 503 without running the checks: the answer can no longer change, and a
// shutting-down process has no business adding load to its dependencies.
func (h *HealthServer) readiness(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":    StatusShuttingDown,
			"component": h.cfg.Component,
			"version":   h.cfg.Version,
		})

		return
	}

	statuses := make(map[string]string, len(h.cfg.Checks))
	ready := true

	for _, name := range h.checkNames() {
		if err := h.check(r.Context(), name); err != nil {
			log.Debugf("%s: readiness check %q failed: %s", h.cfg.Component, name, err.Error())

			statuses[name] = "unreachable"
			ready = false

			continue
		}

		statuses[name] = "ok"
	}

	status := http.StatusOK
	state := "ready"

	if !ready {
		status = http.StatusServiceUnavailable
		state = "not ready"
	}

	// The version rides on /readyz as well as /healthz because /readyz is what the service's
	// topology prober asks a declared component - so carrying it here puts the component's build on
	// the deployment diagram at no cost of a second request.
	writeJSON(w, status, map[string]any{
		"status":       state,
		"component":    h.cfg.Component,
		"version":      h.cfg.Version,
		"dependencies": statuses,
	})
}

// checkNames returns the check names in a stable order, so the response body does not reorder
// itself between probes.
func (h *HealthServer) checkNames() []string {
	names := make([]string, 0, len(h.cfg.Checks))
	for name := range h.cfg.Checks {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// check runs one check, reusing a cached result within CacheTTL. The check itself runs outside the
// lock so a slow dependency never blocks a concurrent probe; the worst case is a few overlapping
// calls on a cold cache, which is harmless.
func (h *HealthServer) check(ctx context.Context, name string) error {
	h.mu.Lock()
	cached, ok := h.results[name]
	h.mu.Unlock()

	if ok && time.Since(cached.checkedAt) < h.cfg.CacheTTL {
		return cached.err
	}

	checkCtx, cancel := context.WithTimeout(ctx, h.cfg.CheckTimeout)
	defer cancel()

	err := h.cfg.Checks[name](checkCtx)

	h.mu.Lock()
	h.results[name] = checkResult{err: err, checkedAt: time.Now()}
	h.mu.Unlock()

	return err
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(body)
}

// GRPCHealthCheck builds a readiness Check that calls the standard gRPC health service on a
// Hippocampus connection.
//
// It probes grpc.health.v1.Health rather than any Hippocampus RPC on purpose. That service is
// exempt from the auth interceptor, so the check works whatever tier (or absence) of token the
// component holds and cannot itself be the thing that fails; it touches no stored data; and on the
// service side it is driven by the same readiness probe that watches the database, so it reports
// "this instance can actually serve" rather than merely "the TCP connection came up".
//
// A NOT_SERVING response is a failure, as is any transport error - including Unimplemented, which
// cmd/hippocampus registers the health service unconditionally to rule out: getting it means the far
// end is not a Hippocampus instance at all, and reporting ready while pointed at the wrong port is
// the one answer a probe must never give.
func GRPCHealthCheck(conn grpc.ClientConnInterface) Check {
	client := healthgrpc.NewHealthClient(conn)

	return func(ctx context.Context) error {
		res, err := client.Check(ctx, &healthgrpc.HealthCheckRequest{Service: "hippocampus"})
		if err != nil {
			return err
		}

		if res.GetStatus() != healthgrpc.HealthCheckResponse_SERVING {
			return fmt.Errorf("health status %s", res.GetStatus())
		}

		return nil
	}
}
