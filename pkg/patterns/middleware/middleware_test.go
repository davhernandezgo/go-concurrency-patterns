package middleware

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestMiddleware_ChainOrder(t *testing.T) {
	var trace []string

	m1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace = append(trace, "m1_before")
			next.ServeHTTP(w, r)
			trace = append(trace, "m1_after")
		})
	}

	m2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace = append(trace, "m2_before")
			next.ServeHTTP(w, r)
			trace = append(trace, "m2_after")
		})
	}

	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace = append(trace, "handler")
		w.WriteHeader(http.StatusOK)
	})

	handler := Chain(m1, m2)(final)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	expected := []string{"m1_before", "m2_before", "handler", "m2_after", "m1_after"}
	if !reflect.DeepEqual(trace, expected) {
		t.Fatalf("expected onion execution order %v, got %v", expected, trace)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestMiddleware_Recovery(t *testing.T) {
	var caughtPanic any
	panicMiddleware := Recovery(func(err any) {
		caughtPanic = err
	})

	panickingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("database connection dropped")
	})

	handler := panicMiddleware(panickingHandler)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 Internal Server Error, got %d", rec.Code)
	}
	if caughtPanic != "database connection dropped" {
		t.Fatalf("expected caught panic string, got %v", caughtPanic)
	}
}

func TestMiddleware_RequestID(t *testing.T) {
	reqIDMiddleware := RequestID(func() string {
		return "uuid-1234-generated"
	})

	var extractedID any
	handler := reqIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		extractedID = r.Context().Value(RequestIDKey)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/id", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") != "uuid-1234-generated" {
		t.Fatalf("expected X-Request-ID header to be set, got %s", rec.Header().Get("X-Request-ID"))
	}
	if extractedID != "uuid-1234-generated" {
		t.Fatalf("expected request id in context, got %v", extractedID)
	}
}
