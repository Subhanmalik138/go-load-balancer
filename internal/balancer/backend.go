// Package balancer implements the core of the load balancer: backends,
// selection strategies, health checking and the reverse proxy.
package balancer

import (
	"net/url"
	"sync"
	"sync/atomic"
)

// Backend is one upstream server behind the balancer.
type Backend struct {
	URL    *url.URL
	Weight int

	alive    atomic.Bool
	conns    atomic.Int64 // in-flight requests, for least-connections
	requests atomic.Uint64
	errors   atomic.Uint64

	mu            sync.Mutex // guards currentWeight (smooth weighted RR)
	currentWeight int
}

// NewBackend parses rawurl into a Backend with the given weight.
// A weight <= 0 is treated as 1.
func NewBackend(rawurl string, weight int) (*Backend, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if weight <= 0 {
		weight = 1
	}
	b := &Backend{URL: u, Weight: weight}
	b.alive.Store(true)
	return b, nil
}

// IsAlive reports whether the backend currently passes health checks.
func (b *Backend) IsAlive() bool { return b.alive.Load() }

// SetAlive flips the health state; it returns true if the state changed.
func (b *Backend) SetAlive(alive bool) bool {
	return b.alive.Swap(alive) != alive
}

// InFlight returns the number of requests currently being served.
func (b *Backend) InFlight() int64 { return b.conns.Load() }

// Requests returns the total number of requests sent to this backend.
func (b *Backend) Requests() uint64 { return b.requests.Load() }

// Errors returns the total number of failed requests for this backend.
func (b *Backend) Errors() uint64 { return b.errors.Load() }

// begin marks the start of a request; end must be called once it finishes.
func (b *Backend) begin() {
	b.conns.Add(1)
	b.requests.Add(1)
}

func (b *Backend) end() { b.conns.Add(-1) }

func (b *Backend) fail() { b.errors.Add(1) }

// Strategy picks the next backend from the alive subset.
type Strategy interface {
	// Next returns the backend to serve the next request, or nil when
	// no backend is alive. The caller must hold no lock on the balancer.
	Next(backends []*Backend) *Backend
	Name() string
}

// RoundRobin cycles through the alive backends in order.
type RoundRobin struct {
	counter atomic.Uint64
}

func (r *RoundRobin) Name() string { return "round-robin" }

func (r *RoundRobin) Next(backends []*Backend) *Backend {
	alive := aliveOnly(backends)
	if len(alive) == 0 {
		return nil
	}
	n := r.counter.Add(1) - 1
	return alive[n%uint64(len(alive))]
}

// LeastConnections picks the alive backend with the fewest in-flight
// requests, breaking ties by list order.
type LeastConnections struct{}

func (LeastConnections) Name() string { return "least-connections" }

func (LeastConnections) Next(backends []*Backend) *Backend {
	var best *Backend
	for _, b := range backends {
		if !b.IsAlive() {
			continue
		}
		if best == nil || b.InFlight() < best.InFlight() {
			best = b
		}
	}
	return best
}

// WeightedRoundRobin implements the smooth weighted round-robin algorithm
// used by nginx: over a full cycle each backend receives requests in exact
// proportion to its weight, without the burstiness of naive repetition.
type WeightedRoundRobin struct{}

func (WeightedRoundRobin) Name() string { return "weighted-round-robin" }

func (WeightedRoundRobin) Next(backends []*Backend) *Backend {
	var best *Backend
	total := 0
	for _, b := range backends {
		if !b.IsAlive() {
			continue
		}
		total += b.Weight
		b.mu.Lock()
		b.currentWeight += b.Weight
		if best == nil || b.currentWeight > best.currentWeight {
			best = b
		}
		b.mu.Unlock()
	}
	if best == nil {
		return nil
	}
	best.mu.Lock()
	best.currentWeight -= total
	best.mu.Unlock()
	return best
}

func aliveOnly(backends []*Backend) []*Backend {
	out := make([]*Backend, 0, len(backends))
	for _, b := range backends {
		if b.IsAlive() {
			out = append(out, b)
		}
	}
	return out
}
