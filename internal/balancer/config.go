package balancer

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// BackendConfig is one upstream entry in the JSON config file.
type BackendConfig struct {
	URL    string `json:"url"`
	Weight int    `json:"weight,omitempty"`
}

// HealthConfigFile is the health-check section of the JSON config.
type HealthConfigFile struct {
	Path     string `json:"path,omitempty"`
	Interval string `json:"interval,omitempty"` // e.g. "10s"
	Timeout  string `json:"timeout,omitempty"`  // e.g. "2s"
}

// Config is the JSON file the load balancer reads at startup.
type Config struct {
	Listen      string           `json:"listen"`
	Strategy    string           `json:"strategy,omitempty"`
	Backends    []BackendConfig  `json:"backends"`
	HealthCheck HealthConfigFile `json:"health_check,omitempty"`
}

// LoadConfig reads and validates a config file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("config %s: at least one backend is required", path)
	}
	for i, b := range cfg.Backends {
		if b.URL == "" {
			return nil, fmt.Errorf("config %s: backend %d has no url", path, i)
		}
	}
	return &cfg, nil
}

// ToBackends converts the configured backends into live Backend values.
func (c *Config) ToBackends() ([]*Backend, error) {
	out := make([]*Backend, 0, len(c.Backends))
	for _, bc := range c.Backends {
		b, err := NewBackend(bc.URL, bc.Weight)
		if err != nil {
			return nil, fmt.Errorf("backend %q: %w", bc.URL, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// ToHealthConfig converts the health-check section into a HealthConfig.
func (c *Config) ToHealthConfig() (HealthConfig, error) {
	hc := HealthConfig{Path: c.HealthCheck.Path}
	if c.HealthCheck.Interval != "" {
		d, err := time.ParseDuration(c.HealthCheck.Interval)
		if err != nil {
			return hc, fmt.Errorf("health_check.interval: %w", err)
		}
		hc.Interval = d
	}
	if c.HealthCheck.Timeout != "" {
		d, err := time.ParseDuration(c.HealthCheck.Timeout)
		if err != nil {
			return hc, fmt.Errorf("health_check.timeout: %w", err)
		}
		hc.Timeout = d
	}
	return hc, nil
}
