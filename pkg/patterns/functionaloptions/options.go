package functionaloptions

import (
	"errors"
	"time"
)

// Server represents a configurable backend network server.
type Server struct {
	host         string
	port         int
	timeout      time.Duration
	maxConns     int
	tlsEnabled   bool
	rateLimiter  bool
}

// Option defines a functional option for configuring a Server.
type Option func(*Server) error

// WithPort sets the listening port.
func WithPort(port int) Option {
	return func(s *Server) error {
		if port <= 0 || port > 65535 {
			return errors.New("invalid port number: must be between 1 and 65535")
		}
		s.port = port
		return nil
	}
}

// WithTimeout sets the request timeout.
func WithTimeout(d time.Duration) Option {
	return func(s *Server) error {
		if d <= 0 {
			return errors.New("timeout must be positive")
		}
		s.timeout = d
		return nil
	}
}

// WithMaxConns sets the maximum concurrent connection limit.
func WithMaxConns(conns int) Option {
	return func(s *Server) error {
		if conns <= 0 {
			return errors.New("max connections must be greater than zero")
		}
		s.maxConns = conns
		return nil
	}
}

// WithTLS enables TLS encryption on the server.
func WithTLS(enabled bool) Option {
	return func(s *Server) error {
		s.tlsEnabled = enabled
		return nil
	}
}

// WithRateLimiter enables request rate limiting.
func WithRateLimiter(enabled bool) Option {
	return func(s *Server) error {
		s.rateLimiter = enabled
		return nil
	}
}

// NewServer constructs a Server instance applying defaults overridden by functional options.
func NewServer(host string, opts ...Option) (*Server, error) {
	if host == "" {
		host = "0.0.0.0"
	}

	// Default production values
	srv := &Server{
		host:        host,
		port:        8080,
		timeout:     30 * time.Second,
		maxConns:    1000,
		tlsEnabled:  false,
		rateLimiter: false,
	}

	for _, opt := range opts {
		if err := opt(srv); err != nil {
			return nil, err
		}
	}

	return srv, nil
}

// Getters for inspection
func (s *Server) Host() string          { return s.host }
func (s *Server) Port() int             { return s.port }
func (s *Server) Timeout() time.Duration { return s.timeout }
func (s *Server) MaxConns() int         { return s.maxConns }
func (s *Server) TLSEnabled() bool      { return s.tlsEnabled }
func (s *Server) RateLimiter() bool     { return s.rateLimiter }
