package functionaloptions

import (
	"testing"
	"time"
)

func TestNewServer_Defaults(t *testing.T) {
	srv, err := NewServer("localhost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if srv.Host() != "localhost" {
		t.Errorf("expected localhost, got %s", srv.Host())
	}
	if srv.Port() != 8080 {
		t.Errorf("expected default port 8080, got %d", srv.Port())
	}
	if srv.Timeout() != 30*time.Second {
		t.Errorf("expected default timeout 30s, got %v", srv.Timeout())
	}
	if srv.TLSEnabled() != false {
		t.Errorf("expected tls false by default")
	}
}

func TestNewServer_CustomOptions(t *testing.T) {
	srv, err := NewServer("api.internal",
		WithPort(9090),
		WithTimeout(10*time.Second),
		WithMaxConns(5000),
		WithTLS(true),
		WithRateLimiter(true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if srv.Port() != 9090 {
		t.Errorf("expected port 9090, got %d", srv.Port())
	}
	if srv.Timeout() != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", srv.Timeout())
	}
	if srv.MaxConns() != 5000 {
		t.Errorf("expected maxConns 5000, got %d", srv.MaxConns())
	}
	if !srv.TLSEnabled() || !srv.RateLimiter() {
		t.Errorf("expected TLS and RateLimiter to be true")
	}
}

func TestNewServer_Validation(t *testing.T) {
	_, err := NewServer("localhost", WithPort(-1))
	if err == nil {
		t.Error("expected error for invalid port, got nil")
	}

	_, err = NewServer("localhost", WithTimeout(-5*time.Second))
	if err == nil {
		t.Error("expected error for negative timeout, got nil")
	}
}
