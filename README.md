# MiniSpark Engine

A miniature Apache Spark-like data processing engine written in C, built on POSIX threads and a hand-rolled thread pool.

## Skills Demonstrated

- **Systems programming in C** - manual memory management, file I/O, dynamic data structures
- **Concurrent programming** - thread pool, mutex/condition variable synchronization, producer/consumer work queue
- **Parallel algorithm design** - recursive DAG traversal, dependency-driven task scheduling, partition-level parallelism
- **OS-level APIs** - sched_getaffinity, clock_gettime, POSIX threads
- **Testing** - 21/21 tests passing, including a ThreadSanitizer run for concurrency correctness

## Overview

MiniSpark implements the core execution model of Apache Spark on a single node. Programs declare lazy transformation pipelines over partitioned datasets (RDDs), and the engine materializes them in parallel using a fixed-size worker thread pool.

Built for UW-Madison CS 537 (Operating Systems, Spring 2025). Received full marks.

## Architecture

**RDDs (Resilient Distributed Datasets)**
Immutable, partitioned data collections that form the nodes of the processing graph. Partitions are populated lazily - no computation runs until an action is called.

**Transformations (lazy - build the DAG)**
| Operation | Description |
|---|---|
| `map(rdd, fn)` | Apply `fn` to every element in every partition |
| `filter(rdd, fn, ctx)` | Keep only elements for which `fn` returns non-zero |
| `partitionBy(rdd, fn, n, ctx)` | Hash-repartition into `n` partitions |
| `join(rdd1, rdd2, fn, ctx)` | Inner join on matching keys |

**Actions (trigger materialization)**
- `count(rdd)` - materialize and return total element count
- `print(rdd, printer)` - materialize and print each element

**Thread Pool**
Worker count is set to the number of available CPU cores via `sched_getaffinity`. The pool uses a circular bounded buffer protected by `pthread_mutex_t`, with `pthread_cond_t` for blocking workers when the queue is empty and waking them when work arrives. The main thread blocks until all in-flight partitions drain.

**DAG Scheduler**
A recursive post-order traversal resolves dependencies before submitting tasks. No partition is enqueued until all partitions it depends on have been materialized, eliminating the deadlock risk of letting blocked workers hold pool threads.

**Metrics Monitor**
A dedicated monitoring thread drains a separate lock-protected metrics queue. Each completed task records creation time, scheduling time, and execution duration to `metrics.log` at microsecond resolution using `clock_gettime(CLOCK_MONOTONIC)`.

## Example Applications

| Binary | DAG Pipeline |
|---|---|
| `linecount` | `RDDFromFiles` -> `map(GetLines)` -> `count` |
| `cat` | `RDDFromFiles` -> `map(GetLines)` -> `print` |
| `grep` | `RDDFromFiles` -> `map(GetLines)` -> `filter(StringContains)` -> `print` |
| `grepcount` | `RDDFromFiles` -> `map(GetLines)` -> `filter(StringContains)` -> `count` |
| `sumjoin` | Two file RDDs -> `map` -> `partitionBy` -> `join(SumJoiner)` -> `print` |
| `concurrency` | Stress test: many independent RDD chains materialized concurrently |

## Build

**Recommended (any platform)**
```sh
docker build -t minispark .
docker run --rm -v "$(pwd)":/app -w /app minispark make
```

**Linux native (Ubuntu 22.04)**
```sh
make        # compiles all 6 binaries into bin/
make clean  # removes object files and bin/
```

The codebase uses `sched_getaffinity`, a Linux-only syscall. Docker provides a consistent Ubuntu 22.04 build environment and is the easiest path on any platform.

## Usage

```sh
./bin/linecount file1.txt file2.txt
./bin/grep needle file1.txt file2.txt
./bin/sumjoin left.txt right.txt
```

## Testing

```sh
cd tests && ./run-tests.sh -c
```

The test suite was provided by the UW-Madison CS 537 course framework. Tests verify stdout, stderr, and return code against reference outputs.
