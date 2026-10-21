package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Middleware is an idiomatic Go decorator for http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain combines multiple middlewares into a single composable decorator.
// They execute in the order provided: middleware1 -> middleware2 -> ... -> finalHandler.
func Chain(middlewares ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// RequestLogger middleware logs request path and execution duration.
func RequestLogger(logFunc func(msg string)) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			duration := time.Since(start)
			if logFunc != nil {
				logFunc(fmt.Sprintf("%s %s completed in %v", r.Method, r.URL.Path, duration))
			}
		})
	}
}

// Recovery middleware recovers from panics, returning 500 Internal Server Error without crashing the process.
func Recovery(onPanic func(err any)) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if onPanic != nil {
						onPanic(rec)
					}
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

type contextKey string

const RequestIDKey contextKey = "request_id"

// RequestID attaches a correlation ID header and injects it into request context.
func RequestID(generator func() string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := r.Header.Get("X-Request-ID")
			if reqID == "" && generator != nil {
				reqID = generator()
			}
			w.Header().Set("X-Request-ID", reqID)
			ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
