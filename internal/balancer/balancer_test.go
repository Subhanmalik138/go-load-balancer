package balancer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustBackend(t *testing.T, rawurl string, weight int) *Backend {
	t.Helper()
	b, err := NewBackend(rawurl, weight)
	if err != nil {
		t.Fatalf("NewBackend(%q): %v", rawurl, err)
	}
	return b
}

func TestNewBackendDefaultsWeight(t *testing.T) {
	b := mustBackend(t, "http://localhost:9001", 0)
	if b.Weight != 1 {
		t.Fatalf("weight = %d, want 1", b.Weight)
	}
	if !b.IsAlive() {
		t.Fatal("new backend should start alive")
	}
}

func TestNewBackendBadURL(t *testing.T) {
	if _, err := NewBackend("://bad url", 1); err == nil {
		t.Fatal("expected error for malformed URL")
	}
}

func TestRoundRobinCyclesEvenly(t *testing.T) {
	rr := &RoundRobin{}
	bs := []*Backend{
		mustBackend(t, "http://localhost:9001", 1),
		mustBackend(t, "http://localhost:9002", 1),
		mustBackend(t, "http://localhost:9003", 1),
	}
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		seen[rr.Next(bs).URL.Host]++
	}
	for host, n := range seen {
		if n != 2 {
			t.Fatalf("host %s got %d requests, want 2", host, n)
		}
	}
}

func TestRoundRobinSkipsDeadBackends(t *testing.T) {
	rr := &RoundRobin{}
	dead := mustBackend(t, "http://localhost:9001", 1)
	dead.SetAlive(false)
	bs := []*Backend{dead, mustBackend(t, "http://localhost:9002", 1)}
	for i := 0; i < 4; i++ {
		if got := rr.Next(bs); got.URL.Host != "localhost:9002" {
			t.Fatalf("pick %d = %s, want localhost:9002", i, got.URL.Host)
		}
	}
}

func TestRoundRobinNoAliveBackends(t *testing.T) {
	rr := &RoundRobin{}
	b := mustBackend(t, "http://localhost:9001", 1)
	b.SetAlive(false)
	if got := rr.Next([]*Backend{b}); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestLeastConnectionsPicksQuietest(t *testing.T) {
	lc := LeastConnections{}
	busy := mustBackend(t, "http://localhost:9001", 1)
	busy.begin()
	busy.begin()
	idle := mustBackend(t, "http://localhost:9002", 1)
	if got := lc.Next([]*Backend{busy, idle}); got != idle {
		t.Fatalf("got %s, want idle backend", got.URL.Host)
	}
}

func TestWeightedRoundRobinRespectsWeights(t *testing.T) {
	wrr := WeightedRoundRobin{}
	heavy := mustBackend(t, "http://localhost:9001", 3)
	light := mustBackend(t, "http://localhost:9002", 1)
	bs := []*Backend{heavy, light}
	seen := map[string]int{}
	for i := 0; i < 8; i++ {
		seen[wrr.Next(bs).URL.Host]++
	}
	if seen["localhost:9001"] != 6 || seen["localhost:9002"] != 2 {
		t.Fatalf("distribution = %v, want 6:2", seen)
	}
}

func TestWeightedRoundRobinSkipsDead(t *testing.T) {
	wrr := WeightedRoundRobin{}
	dead := mustBackend(t, "http://localhost:9001", 10)
	dead.SetAlive(false)
	alive := mustBackend(t, "http://localhost:9002", 1)
	if got := wrr.Next([]*Backend{dead, alive}); got != alive {
		t.Fatalf("got %s, want alive backend", got.URL.Host)
	}
}

func TestNewBalancerRejectsUnknownStrategy(t *testing.T) {
	if _, err := NewBalancer(nil, "magic"); err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := `{
		"listen": ":9090",
		"strategy": "least-connections",
		"backends": [{"url": "http://localhost:9001", "weight": 2}],
		"health_check": {"path": "/health", "interval": "5s", "timeout": "1s"}
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listen != ":9090" || cfg.Strategy != "least-connections" {
		t.Fatalf("config = %+v", cfg)
	}
	if len(cfg.Backends) != 1 || cfg.Backends[0].Weight != 2 {
		t.Fatalf("backends = %+v", cfg.Backends)
	}
	hc, err := cfg.ToHealthConfig()
	if err != nil {
		t.Fatalf("ToHealthConfig: %v", err)
	}
	if hc.Path != "/health" || hc.Interval != 5*time.Second || hc.Timeout != time.Second {
		t.Fatalf("health config = %+v", hc)
	}
	bs, err := cfg.ToBackends()
	if err != nil {
		t.Fatalf("ToBackends: %v", err)
	}
	if bs[0].Weight != 2 {
		t.Fatalf("backend weight = %d, want 2", bs[0].Weight)
	}
}

func TestLoadConfigRejectsEmptyBackends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"listen": ":9090", "backends": []}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for empty backends")
	}
}

func TestLoadConfigBadJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

// backendServer is a tiny upstream that answers with its own name.
func backendServer(t *testing.T, name string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, name)
	}))
}

func TestProxyRoundRobinsAcrossBackends(t *testing.T) {
	s1 := backendServer(t, "one")
	defer s1.Close()
	s2 := backendServer(t, "two")
	defer s2.Close()

	bs := []*Backend{mustBackend(t, s1.URL, 1), mustBackend(t, s2.URL, 1)}
	lb, err := NewBalancer(bs, "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(lb)
	defer srv.Close()

	var got []string
	for i := 0; i < 4; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		got = append(got, string(body))
	}
	want := []string{"one", "two", "one", "two"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request %d served by %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestProxyFailsOverToAliveBackend(t *testing.T) {
	// A backend that is down before the first request: connection refused.
	dead := mustBackend(t, "http://127.0.0.1:1", 1)
	alive := backendServer(t, "alive")
	defer alive.Close()

	bs := []*Backend{dead, mustBackend(t, alive.URL, 1)}
	lb, err := NewBalancer(bs, "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(lb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "alive" {
		t.Fatalf("body = %q, want %q", body, "alive")
	}
	if dead.IsAlive() {
		t.Fatal("failed backend should be marked down")
	}
}

func TestProxyNoHealthyBackends(t *testing.T) {
	dead := mustBackend(t, "http://127.0.0.1:1", 1)
	dead.SetAlive(false)
	lb, err := NewBalancer([]*Backend{dead}, "round-robin")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(lb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestStatsEndpoint(t *testing.T) {
	s1 := backendServer(t, "one")
	defer s1.Close()
	bs := []*Backend{mustBackend(t, s1.URL, 2)}
	lb, err := NewBalancer(bs, "least-connections")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(lb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/__lb/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"strategy":"least-connections"`) {
		t.Fatalf("stats body missing strategy: %s", body)
	}
	if !strings.Contains(string(body), `"weight":2`) {
		t.Fatalf("stats body missing weight: %s", body)
	}
}

func TestHealthCheckerMarksDownAndUp(t *testing.T) {
	var mu sync.Mutex
	healthy := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ok := healthy
		mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b := mustBackend(t, srv.URL, 1)
	hc := NewHealthChecker([]*Backend{b}, HealthConfig{Path: "/", Interval: time.Hour, Timeout: time.Second})
	hc.logf = func(string, ...any) {} // silence transitions in test output

	ctx := context.Background()
	hc.CheckAll(ctx)
	if !b.IsAlive() {
		t.Fatal("backend should be alive while it answers 200")
	}

	mu.Lock()
	healthy = false
	mu.Unlock()
	hc.CheckAll(ctx)
	if b.IsAlive() {
		t.Fatal("backend should be down after 503s")
	}

	mu.Lock()
	healthy = true
	mu.Unlock()
	hc.CheckAll(ctx)
	if !b.IsAlive() {
		t.Fatal("backend should recover once it answers 200 again")
	}
}

func TestHealthCheckerStartStopsOnCancel(t *testing.T) {
	srv := backendServer(t, "one")
	defer srv.Close()
	b := mustBackend(t, srv.URL, 1)
	hc := NewHealthChecker([]*Backend{b}, HealthConfig{Interval: 10 * time.Millisecond})
	hc.logf = func(string, ...any) {}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hc.Start(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("health checker did not stop after context cancel")
	}
}
