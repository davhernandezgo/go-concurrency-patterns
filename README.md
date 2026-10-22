# Go Concurrency & Design Patterns in Action

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://golang.org)
[![Build Status](https://img.shields.io/badge/CI-Passing-2ea44f?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/davhernandezgo/go-concurrency-patterns/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![Coverage](https://img.shields.io/badge/Coverage-100%25-brightgreen?style=for-the-badge)](https://github.com/davhernandezgo/go-concurrency-patterns)

> A production-ready, benchmarked suite of **Idiomatic Design Patterns** and **Advanced Concurrency Primitives** in Go. Designed for high-throughput microservices, distributed pipelines, and resilient backend systems.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture & Concurrency Visualizations](#-architecture--concurrency-visualizations)
  - [1. Worker Pool with Backpressure](#1-worker-pool-with-backpressure)
  - [2. Multi-Stage Pipeline](#2-multi-stage-pipeline)
  - [3. Fan-Out / Fan-In](#3-fan-out--fan-in)
  - [4. Resilient Circuit Breaker](#4-resilient-circuit-breaker)
- [Catalog of Patterns](#-catalog-of-patterns)
  - [Concurrency Primitives](#concurrency-primitives)
  - [Idiomatic & GoF Design Patterns](#idiomatic--gof-design-patterns)
- [Performance & Benchmarks](#-performance--benchmarks)
- [Getting Started](#-getting-started)
- [Author & Contact](#-author--contact)

---

## 🎯 Overview

In Go, applying classic Object-Oriented patterns blindly often leads to unidiomatic and inefficient code. True Go mastery lies in combining **CSP (Communicating Sequential Processes)** concurrency models with **Go-idiomatic software composition** (interfaces, functional options, middleware decorators, and channel topologies).

This repository serves as a **battle-tested reference** showcasing:
1. **Zero-leak concurrency**: Context cancellation, bounded channel buffers, and guaranteed goroutine lifecycle control.
2. **Backpressure management**: Non-blocking `TrySubmit` and bounded worker queues to prevent out-of-memory cascades under burst traffic.
3. **Resilience engineering**: Circuit Breaker with automatic half-open health checks and Token-Bucket Rate Limiting.
4. **Clean Code & Idiomatic Patterns**: Functional Options, Onion-style HTTP Middleware Chains, and Dynamic Strategy swapping.

---

## 📐 Architecture & Concurrency Visualizations

### 1. Worker Pool with Backpressure
```mermaid
flowchart LR
    A[Incoming Requests] -->|Submit| B[(Job Queue Buffer)]
    B --> W1[Worker Goroutine 1]
    B --> W2[Worker Goroutine 2]
    B --> W3[Worker Goroutine 3]
    B --> Wn[Worker Goroutine N]
    W1 --> C[(Results Channel)]
    W2 --> C
    W3 --> C
    Wn --> C
    C --> D[Consumer / Aggregator]
```

### 2. Multi-Stage Pipeline
```mermaid
flowchart LR
    Gen[Generator Stage] -->|stream chan| Map[Transform Stage]
    Map -->|stream chan| Filter[Filter Stage]
    Filter -->|stream chan| Batch[Batch / Window Stage]
    Batch -->|stream chan| Sink[Final Consumer / DB]
```

### 3. Fan-Out / Fan-In
```mermaid
flowchart TD
    In[High-Volume Input Stream] --> Split{Fan-Out Splitter}
    Split --> P1[Worker 1]
    Split --> P2[Worker 2]
    Split --> P3[Worker 3]
    P1 --> Merge{Fan-In Multiplexer}
    P2 --> Merge
    P3 --> Merge
    Merge --> Out[Unified Output Stream]
```

### 4. Resilient Circuit Breaker
```mermaid
stateDiagram-v2
    [*] --> Closed
    Closed --> Open : Consecutive Failures >= Threshold
    Open --> HalfOpen : Cooldown Timeout Elapsed
    HalfOpen --> Closed : Consecutive Successes >= Threshold
    HalfOpen --> Open : Any Failure Occurs
```

---

## 📦 Catalog of Patterns

### Concurrency Primitives

| Pattern | Description | Key Features |
| :--- | :--- | :--- |
| **Worker Pool** (`pkg/concurrency/workerpool`) | Manages fixed workers consuming from a synchronized queue. | Dynamic types via Go Generics, graceful shutdown via `Stop()` and `StopNow()`, non-blocking `TrySubmit()`. |
| **Pipeline** (`pkg/concurrency/pipeline`) | Composable data transformations connected via channels. | `Generator`, `Map`, `Filter`, `Batch` with time-window fallback, and `Reduce`. |
| **Fan-Out / Fan-In** (`pkg/concurrency/fanout`) | Distributes workloads across goroutines and multiplexes results. | Parallel CPU/IO multiplexing with `sync.WaitGroup` and leak-free cancellation. |
| **Circuit Breaker** (`pkg/concurrency/circuitbreaker`) | Prevents cascading system failures during downstream outages. | Three-state machine (`CLOSED`, `OPEN`, `HALF_OPEN`), thread-safe state transitions, zero external dependencies. |
| **Token Bucket Limiter** (`pkg/concurrency/ratelimiter`) | Throttles traffic allowing bursts up to bucket capacity. | Float precision refill calculation, instant `Allow()`, and non-busy-waiting `Wait(ctx)`. |
| **Singleflight** (`pkg/concurrency/singleflight`) | Suppresses duplicate in-flight requests (cache stampede prevention). | Executes identical concurrent requests only once, broadcasting results across all waiting callers using `sync.WaitGroup` and mutex locks. |

### Idiomatic & GoF Design Patterns

| Pattern | Package | Real-World Application |
| :--- | :--- | :--- |
| **Functional Options** | `pkg/patterns/functionaloptions` | Flexible, backward-compatible constructor for servers, HTTP clients, and database connection pools without constructor telescoping. |
| **Middleware / Decorator** | `pkg/patterns/middleware` | Onion-style HTTP request decorators for Distributed Tracing (Correlation ID), Request Timing, and Panic Recovery. |
| **Strategy** | `pkg/patterns/strategy` | Polymorphic payment engines (Credit Card, Crypto, ACH) hot-swappable at runtime. |
| **Thread-Safe Singleton** | `pkg/patterns/singleton` | Safe single-instance initialization using `sync.Once`. |

---

## ⚡ Performance & Benchmarks

Run benchmarks locally on your machine:
```bash
make bench
# or
go test -bench=. -benchmem ./...
```

Sample benchmark output (`Apple M1 Pro`, `Go 1.22+`):
```text
BenchmarkWorkerPool-10             	    2438	    491922 ns/op	   48096 B/op	    1015 allocs/op
BenchmarkUnboundedGoroutines-10    	    4060	    298719 ns/op	   80020 B/op	    2002 allocs/op
```

> **Engineering Insight**: While spawning unconstrained goroutines is quick for small tasks, a bounded Worker Pool dramatically caps memory allocations, prevents thread thrashing, and shields downstream databases/APIs from resource exhaustion.

---

## 🚀 Getting Started

### Prerequisites
- **Go 1.22+** installed.
- Optional: `make` command-line utility.

### Running the Live Interactive Demo
```bash
# Clone the repository
git clone https://github.com/davhernandezgo/go-concurrency-patterns.git
cd go-concurrency-patterns

# Run live demonstrations of all patterns
go run ./cmd/demo/main.go
# or
make run
```

### Running Unit Tests with Race Detection
```bash
make test
# or
go test -v -race ./...
```

---

## 👨‍💻 Author & Contact

**David E. Hernandez G**
- **GitHub**: [@davhernandezgo](https://github.com/davhernandezgo)
- **LinkedIn**: [David Hernandez](https://www.linkedin.com/in/davhernandezgo/)
- **Specialization**: Backend Distributed Systems · Go (Golang) · Java 21 / Spring Boot 3
