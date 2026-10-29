package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/bulkhead"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/circuitbreaker"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/fanout"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/hedged"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/pipeline"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/pubsub"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/ratelimiter"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/retry"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/singleflight"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/concurrency/workerpool"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/patterns/functionaloptions"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/patterns/middleware"
	"github.com/davhernandezgo/go-concurrency-patterns/pkg/patterns/strategy"
)

func main() {
	fmt.Println("==================================================================")
	fmt.Println(" 🚀 Go Concurrency & Design Patterns Suite - by David Hernandez ")
	fmt.Println("==================================================================")

	runWorkerPoolDemo()
	runPipelineDemo()
	runFanOutFanInDemo()
	runCircuitBreakerDemo()
	runRateLimiterDemo()
	runFunctionalOptionsDemo()
	runStrategyDemo()
	runMiddlewareDemo()
	runSingleflightDemo()
	runPubSubDemo()
	runHedgedDemo()
	runBulkheadDemo()
	runRetryDemo()

	fmt.Println("\n✨ All pattern demonstrations finished successfully!")
}

func runWorkerPoolDemo() {
	fmt.Println("\n--- [1] Concurrency: Worker Pool Pattern ---")
	handler := func(ctx context.Context, taskID int) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("Processed payload-%d", taskID), nil
	}

	pool := workerpool.New[int, string](4, 10, handler)
	pool.Start()

	go func() {
		for i := 1; i <= 8; i++ {
			_ = pool.Submit(context.Background(), i)
		}
		pool.Stop()
	}()

	for res := range pool.Results() {
		fmt.Printf(" [WorkerPool Output] %s (err: %v)\n", res.Value, res.Err)
	}
}

func runPipelineDemo() {
	fmt.Println("\n--- [2] Concurrency: Multi-Stage Processing Pipeline ---")
	ctx := context.Background()

	// Stage 1: Generator
	nums := pipeline.Generator(ctx, 10, 15, 20, 25, 30)

	// Stage 2: Filter (multiples of 10)
	evens := pipeline.Filter(ctx, nums, func(n int) bool {
		return n%10 == 0
	})

	// Stage 3: Map (Transform)
	formatted := pipeline.Map(ctx, evens, func(ctx context.Context, n int) string {
		return fmt.Sprintf("Metric: %d req/sec", n)
	})

	for item := range formatted {
		fmt.Printf(" [Pipeline Stage 3] %s\n", item)
	}
}

func runFanOutFanInDemo() {
	fmt.Println("\n--- [3] Concurrency: Fan-Out / Fan-In Pattern ---")
	ctx := context.Background()
	in := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	workerFn := func(ctx context.Context, item int) string {
		time.Sleep(5 * time.Millisecond)
		return fmt.Sprintf("Hash(Item-%d)=0x%X", item, item*1337)
	}

	workerChannels := fanout.FanOut(ctx, in, 3, workerFn)
	merged := fanout.FanIn(ctx, workerChannels...)

	for res := range merged {
		fmt.Printf(" [Fan-In Result] %s\n", res)
	}
}

func runCircuitBreakerDemo() {
	fmt.Println("\n--- [4] Concurrency: Resilient Circuit Breaker ---")
	cb := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		Timeout:          50 * time.Millisecond,
	})

	fmt.Printf(" Initial State: %s\n", cb.State())

	// Fail twice
	for i := 1; i <= 2; i++ {
		_, _ = circuitbreaker.Execute(cb, func() (string, error) {
			return "", fmt.Errorf("connection timeout %d", i)
		})
	}
	fmt.Printf(" State after 2 failures: %s\n", cb.State())

	// Try while Open
	_, err := circuitbreaker.Execute(cb, func() (string, error) {
		return "data", nil
	})
	fmt.Printf(" Request while Open: error = '%v'\n", err)

	// Cooldown
	time.Sleep(60 * time.Millisecond)
	fmt.Printf(" State after cooldown: %s\n", cb.State())

	// Recover
	val, _ := circuitbreaker.Execute(cb, func() (string, error) {
		return "recovered-data", nil
	})
	fmt.Printf(" Request after recovery: '%s', new state: %s\n", val, cb.State())
}

func runRateLimiterDemo() {
	fmt.Println("\n--- [5] Concurrency: Token Bucket Rate Limiter ---")
	tb := ratelimiter.NewTokenBucket(10, 2) // 10 tokens/sec, capacity 2

	for i := 1; i <= 4; i++ {
		allowed := tb.Allow()
		fmt.Printf(" Request %d: allowed = %v\n", i, allowed)
	}
}

func runFunctionalOptionsDemo() {
	fmt.Println("\n--- [6] GoF / Idiomatic: Functional Options Pattern ---")
	srv, err := functionaloptions.NewServer("api.prod.cloud",
		functionaloptions.WithPort(443),
		functionaloptions.WithTimeout(15*time.Second),
		functionaloptions.WithMaxConns(10000),
		functionaloptions.WithTLS(true),
		functionaloptions.WithRateLimiter(true),
	)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	fmt.Printf(" Configured Server: host=%s:%d, timeout=%v, maxConns=%d, TLS=%v, RateLimiter=%v\n",
		srv.Host(), srv.Port(), srv.Timeout(), srv.MaxConns(), srv.TLSEnabled(), srv.RateLimiter())
}

func runStrategyDemo() {
	fmt.Println("\n--- [7] GoF: Dynamic Strategy Pattern ---")
	ctx := context.Background()
	cc := &strategy.CreditCardStrategy{CardNumber: "4111222233339876", CVV: "888"}
	crypto := &strategy.CryptoStrategy{WalletAddress: "0xDA71c845175574b885D", Network: "POLYGON"}

	payment := strategy.NewPaymentContext(cc)
	res1, _ := payment.ExecutePayment(ctx, 250.0)
	fmt.Printf(" Executed: %+v\n", res1)

	payment.SetStrategy(crypto)
	res2, _ := payment.ExecutePayment(ctx, 500.0)
	fmt.Printf(" Executed: %+v\n", res2)
}

func runMiddlewareDemo() {
	fmt.Println("\n--- [8] GoF / Idiomatic: HTTP Middleware Decorator Chain ---")
	chain := middleware.Chain(
		middleware.RequestID(func() string { return "req-uuid-999" }),
		middleware.RequestLogger(func(msg string) { fmt.Printf(" [Audit Log] %s\n", msg) }),
	)

	handler := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Context().Value(middleware.RequestIDKey)
		fmt.Printf(" [Core Handler] Handled request with Correlation-ID: %v\n", reqID)
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/orders", nil)
	handler.ServeHTTP(rec, req)
}

func runSingleflightDemo() {
	fmt.Println("\n--- [9] Concurrency: Singleflight (Duplicate Request Suppression) ---")
	group := singleflight.NewGroup[string]()

	var callCount int
	expensiveFetch := func() (string, error) {
		callCount++
		time.Sleep(30 * time.Millisecond) // simulate heavy DB query
		return "user-profile-data-cached", nil
	}

	done := make(chan struct{})
	for i := 1; i <= 5; i++ {
		go func(callerID int) {
			res, _, shared := group.Do("user:42", expensiveFetch)
			fmt.Printf(" [Caller %d] Received: '%s' (Shared: %v)\n", callerID, res, shared)
			done <- struct{}{}
		}(i)
	}

	for i := 1; i <= 5; i++ {
		<-done
	}
	fmt.Printf(" Total DB/Service Calls executed for 5 concurrent callers: %d\n", callCount)
}

func runPubSubDemo() {
	fmt.Println("\n--- [10] Concurrency: Publish-Subscribe Event Bus (Fan-Out & Backpressure) ---")
	bus := pubsub.New[string]()
	defer bus.Close()

	// 1. Subscribe to exact topic
	emailSub, _ := bus.Subscribe("orders.created", 5, pubsub.DropOldest)
	defer emailSub.Unsubscribe()

	// 2. Subscribe to wildcard topic pattern
	auditSub, _ := bus.Subscribe("orders.*", 5, pubsub.DropOldest)
	defer auditSub.Unsubscribe()

	ctx := context.Background()

	// Publish domain events
	_ = bus.Publish(ctx, "orders.created", "Order #90210 ($129.99)")
	_ = bus.Publish(ctx, "orders.shipped", "Order #90210 tracking: TRK-5541")

	// Read from subscribers
	fmt.Printf(" [Email Service] Received: '%s'\n", <-emailSub.C())
	fmt.Printf(" [Audit Logger]  Received: '%s'\n", <-auditSub.C())
	fmt.Printf(" [Audit Logger]  Received: '%s'\n", <-auditSub.C())
}

func runHedgedDemo() {
	fmt.Println("\n--- [11] Concurrency: Hedged Requests (Tail-Latency Mitigation) ---")
	engine := hedged.New[string](hedged.WithDelay(30 * time.Millisecond))
	ctx := context.Background()

	// Scenario 1: Primary is fast (< 30ms) -> No hedge fired
	fmt.Println(" [Scenario 1] Primary replica responds in 10ms (SLA met):")
	res1, _ := engine.Execute(ctx,
		func(ctx context.Context) (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "Primary-DB: 200 OK (Latency: 10ms)", nil
		},
		func(ctx context.Context) (string, error) {
			return "Replica-DB: 200 OK", nil
		},
	)
	fmt.Printf("   Result: %s\n", res1)

	// Scenario 2: Primary experiences tail latency (150ms) -> Hedge replica fired at 30ms and wins!
	fmt.Println(" [Scenario 2] Primary replica hangs with 150ms tail-latency; Hedge replica fired:")
	res2, _ := engine.Execute(ctx,
		func(ctx context.Context) (string, error) {
			select {
			case <-time.After(150 * time.Millisecond):
				return "Primary-DB (Late)", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(15 * time.Millisecond)
			return "Replica-DB (Speculative Hedge Won!): 200 OK", nil
		},
	)
	fmt.Printf("   Result: %s\n", res2)

	// Scenario 3: Primary fails fast -> Hedge replica dispatched immediately without waiting for delay
	fmt.Println(" [Scenario 3] Primary fails immediately; Hedge replica auto-promoted:")
	res3, _ := engine.Execute(ctx,
		func(ctx context.Context) (string, error) {
			return "", fmt.Errorf("connection reset by primary host")
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(5 * time.Millisecond)
			return "Replica-DB (Instant Failover): 200 OK", nil
		},
	)
	fmt.Printf("   Result: %s\n", res3)

	stats := engine.Stats()
	fmt.Printf(" [Hedged Telemetry] Total: %d, PrimaryWins: %d, HedgeWins: %d, WinRatio: %.1f%%\n",
		stats.TotalRequests, stats.PrimaryWins, stats.HedgeWins, stats.WinRatio())
}

func runBulkheadDemo() {
	fmt.Println("\n--- [12] Concurrency: Bulkhead Resource Isolation Pattern ---")
	bh := bulkhead.New[string](bulkhead.Config{
		MaxConcurrent: 2,
		MaxWaitQueue:  2,
		QueueTimeout:  60 * time.Millisecond,
	})

	ctx := context.Background()

	done := make(chan struct{})
	for i := 1; i <= 5; i++ {
		go func(taskID int) {
			res, err := bh.Execute(ctx, func(ctx context.Context) (string, error) {
				time.Sleep(30 * time.Millisecond)
				return fmt.Sprintf("Result-%d", taskID), nil
			})
			if err != nil {
				fmt.Printf(" [Bulkhead Worker %d] Rejected / Error: %v\n", taskID, err)
			} else {
				fmt.Printf(" [Bulkhead Worker %d] Completed: %s\n", taskID, res)
			}
			done <- struct{}{}
		}(i)
	}

	for i := 1; i <= 5; i++ {
		<-done
	}

	stats := bh.Stats()
	fmt.Printf(" [Bulkhead Telemetry] Executed: %d, Rejected: %d, TimedOut: %d\n",
		stats.TotalExecuted, stats.TotalRejected, stats.TotalTimedOut)
}

func runRetryDemo() {
	fmt.Println("\n--- [13] Concurrency: Resilient Exponential Backoff & Jitter ---")
	ctx := context.Background()
	attemptCount := 0

	retrier := retry.New[string](
		retry.WithMaxAttempts(4),
		retry.WithInitialInterval(10*time.Millisecond),
		retry.WithMultiplier(2.0),
		retry.WithJitter(retry.FullJitter),
		retry.WithOnRetry(func(attempt int, err error, nextBackoff time.Duration) {
			fmt.Printf(" [Retry Attempt %d Failed: '%v'] Backing off for %v with Full Jitter\n",
				attempt, err, nextBackoff)
		}),
	)

	res, err := retrier.Execute(ctx, func(ctx context.Context) (string, error) {
		attemptCount++
		if attemptCount < 3 {
			return "", fmt.Errorf("connection reset by peer (503 Service Unavailable)")
		}
		return "HTTP/2 200 OK (Payload Delivered)", nil
	})

	if err != nil {
		fmt.Printf(" [Retry Final] Failed: %v\n", err)
	} else {
		fmt.Printf(" [Retry Final] Success: %s (Total attempts: %d)\n", res, attemptCount)
	}

	stats := retrier.Stats()
	fmt.Printf(" [Retry Telemetry] Total Calls: %d, Total Retries: %d, Success: %d, Backoff Duration: %v\n",
		stats.TotalCalls, stats.TotalRetries, stats.SuccessfulCalls, stats.TotalBackoffDuration)
}
