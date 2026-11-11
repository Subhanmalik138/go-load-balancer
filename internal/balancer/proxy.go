package balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"
)

// Balancer holds the backend pool and the selection strategy.
// It is safe for concurrent use.
type Balancer struct {
	mu       sync.RWMutex
	backends []*Backend
	strategy Strategy
}

// NewBalancer builds a balancer over backends using the named strategy.
// Valid names: "round-robin", "least-connections", "weighted-round-robin".
func NewBalancer(backends []*Backend, strategyName string) (*Balancer, error) {
	var s Strategy
	switch strategyName {
	case "", "round-robin":
		s = &RoundRobin{}
	case "least-connections":
		s = LeastConnections{}
	case "weighted-round-robin":
		s = WeightedRoundRobin{}
	default:
		return nil, fmt.Errorf("unknown strategy %q (want round-robin, least-connections or weighted-round-robin)", strategyName)
	}
	return &Balancer{backends: backends, strategy: s}, nil
}

// StrategyName returns the active strategy's name.
func (lb *Balancer) StrategyName() string { return lb.strategy.Name() }

// Backends returns a snapshot of the backend pool.
func (lb *Balancer) Backends() []*Backend {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	out := make([]*Backend, len(lb.backends))
	copy(out, lb.backends)
	return out
}

// AddBackend registers a new backend at runtime.
func (lb *Balancer) AddBackend(b *Backend) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.backends = append(lb.backends, b)
}

// next picks a backend for one attempt.
func (lb *Balancer) next() *Backend {
	return lb.strategy.Next(lb.Backends())
}

// ServeHTTP implements http.Handler. It forwards the request to a chosen
// backend and fails over to another alive backend if the first one errors.
// The X-Forwarded-For header carries the original client address.
func (lb *Balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A status endpoint lives on the balancer itself.
	if r.URL.Path == "/__lb/stats" {
		lb.serveStats(w, r)
		return
	}

	// One attempt per alive backend at most: a dead backend is never
	// retried within the same request.
	attempts := len(lb.Backends())
	for i := 0; i < attempts; i++ {
		backend := lb.next()
		if backend == nil {
			break
		}
		if lb.tryServe(w, r, backend) {
			return
		}
	}
	http.Error(w, "no healthy backends available", http.StatusBadGateway)
}

// tryServe proxies one request to backend. It returns false when the
// backend failed and the caller should try another one. Nothing is
// written to the client on failure, so a retry can still send a clean
// response.
func (lb *Balancer) tryServe(w http.ResponseWriter, r *http.Request, backend *Backend) (ok bool) {
	proxy := httputil.NewSingleHostReverseProxy(backend.URL)
	var proxyErr error
	proxy.ErrorHandler = func(_ http.ResponseWriter, _ *http.Request, err error) {
		proxyErr = err
		backend.fail()
		backend.SetAlive(false)
	}

	backend.begin()
	defer backend.end()

	// Clone: the director rewrites the request URL in place, and a
	// failed attempt must not leak that rewrite into the next backend.
	proxy.ServeHTTP(w, r.Clone(r.Context()))
	return proxyErr == nil
}

// Stats is the JSON shape served by /__lb/stats.
type Stats struct {
	Strategy string         `json:"strategy"`
	Backends []BackendStats `json:"backends"`
}

// BackendStats is one backend's counters for the stats endpoint.
type BackendStats struct {
	URL      string `json:"url"`
	Weight   int    `json:"weight"`
	Alive    bool   `json:"alive"`
	InFlight int64  `json:"in_flight"`
	Requests uint64 `json:"requests"`
	Errors   uint64 `json:"errors"`
}

func (lb *Balancer) serveStats(w http.ResponseWriter, r *http.Request) {
	stats := Stats{Strategy: lb.StrategyName()}
	for _, b := range lb.Backends() {
		stats.Backends = append(stats.Backends, BackendStats{
			URL:      b.URL.String(),
			Weight:   b.Weight,
			Alive:    b.IsAlive(),
			InFlight: b.InFlight(),
			Requests: b.Requests(),
			Errors:   b.Errors(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// HealthChecker periodically probes every backend and flips its alive
// flag. A backend that recovers starts receiving traffic again on the
// next probe — no restart required.
type HealthChecker struct {
	backends []*Backend
	path     string
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	logf     func(format string, args ...any)
}

// HealthConfig tunes the checker.
type HealthConfig struct {
	Path     string
	Interval time.Duration
	Timeout  time.Duration
}

// NewHealthChecker builds a checker; interval/timeout of 0 get defaults.
func NewHealthChecker(backends []*Backend, cfg HealthConfig) *HealthChecker {
	if cfg.Path == "" {
		cfg.Path = "/"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	return &HealthChecker{
		backends: backends,
		path:     cfg.Path,
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		client:   &http.Client{Timeout: cfg.Timeout},
		logf:     log.Printf,
	}
}

// Start probes in the background until ctx is cancelled.
func (h *HealthChecker) Start(ctx context.Context) {
	h.checkAll(ctx)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.checkAll(ctx)
		}
	}
}

// CheckAll runs one probe round now (useful for tests and the first pass).
func (h *HealthChecker) CheckAll(ctx context.Context) { h.checkAll(ctx) }

func (h *HealthChecker) checkAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range h.backends {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			alive := h.probe(ctx, b)
			if b.SetAlive(alive) {
				state := "down"
				if alive {
					state = "up"
				}
				h.logf("load-balancer: backend %s is %s", b.URL, state)
			}
		}(b)
	}
	wg.Wait()
}

func (h *HealthChecker) probe(ctx context.Context, b *Backend) bool {
	u := *b.URL
	u.Path = singleJoiningSlash(u.Path, h.path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode < http.StatusInternalServerError
}

// singleJoiningSlash joins two path fragments with exactly one slash,
// mirroring httputil's helper.
func singleJoiningSlash(a, b string) string {
	aslash := len(a) > 0 && a[len(a)-1] == '/'
	bslash := len(b) > 0 && b[0] == '/'
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}
