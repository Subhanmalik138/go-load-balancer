// Command lb runs an HTTP reverse-proxy load balancer in front of a pool
// of backends, with pluggable selection strategies and active health
// checks. See config.example.json for the config file format.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Subhanmalik138/go-load-balancer/internal/balancer"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file (see config.example.json)")
	listen := flag.String("listen", ":8080", "address to listen on (ignored with -config)")
	strategy := flag.String("strategy", "round-robin", "selection strategy: round-robin, least-connections, weighted-round-robin")
	backendsFlag := flag.String("backends", "", "comma-separated backend URLs, e.g. http://localhost:9001,http://localhost:9002 (ignored with -config)")
	flag.Parse()

	var (
		cfg      *balancer.Config
		backends []*balancer.Backend
		err      error
	)

	if *configPath != "" {
		cfg, err = balancer.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("config: %v", err)
		}
		backends, err = cfg.ToBackends()
		if err != nil {
			log.Fatalf("config: %v", err)
		}
		*listen = cfg.Listen
		*strategy = cfg.Strategy
	} else {
		if *backendsFlag == "" {
			fmt.Fprintln(os.Stderr, "usage: lb -backends http://localhost:9001,http://localhost:9002 [-listen :8080] [-strategy round-robin]")
			fmt.Fprintln(os.Stderr, "   or: lb -config config.json")
			flag.PrintDefaults()
			os.Exit(2)
		}
		for _, raw := range strings.Split(*backendsFlag, ",") {
			b, err := balancer.NewBackend(strings.TrimSpace(raw), 1)
			if err != nil {
				log.Fatalf("backend %q: %v", raw, err)
			}
			backends = append(backends, b)
		}
	}

	lb, err := balancer.NewBalancer(backends, *strategy)
	if err != nil {
		log.Fatalf("balancer: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hc := HealthConfigOrDefault(cfg)
	checker := balancer.NewHealthChecker(backends, hc)
	go checker.Start(ctx)

	srv := &http.Server{
		Addr:         *listen,
		Handler:      lb,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("load-balancer: listening on %s, strategy %s, %d backend(s)",
		*listen, lb.StrategyName(), len(backends))
	for _, b := range backends {
		log.Printf("load-balancer: backend %s (weight %d)", b.URL, b.Weight)
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
	log.Println("load-balancer: stopped")
}

// HealthConfigOrDefault builds the health-check settings from the config
// file, or falls back to the checker's own defaults when no file was used.
func HealthConfigOrDefault(cfg *balancer.Config) balancer.HealthConfig {
	if cfg == nil {
		return balancer.HealthConfig{}
	}
	hc, err := cfg.ToHealthConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	return hc
}
