# miniRPC Benchmark Report

本报告使用 `benchmark/rpc_bench.cpp` 对比两条服务端路径：

```text
Reactor + ThreadPool
Coroutine + epoll + ThreadPool
```

目标不是证明某个绝对 QPS 数字，而是解释不同压力下的瓶颈来源。

## 环境

| item | value |
| --- | --- |
| date | 2026-05-31 |
| runtime | Docker container `minirpc-linux` |
| kernel | Linux 6.12.54-linuxkit |
| arch | aarch64 |
| logical CPUs | 10 |
| benchmark binary | `./build/rpc_bench` |
| payload | 16 bytes |
| status expectation | `0:N` means all requests returned `StatusCode::kOk` |

CPU 数据来自 `getrusage(RUSAGE_SELF)`，因此包含 benchmark 进程内客户端线程和服务端线程的 user/system CPU 时间，适合做同进程两路径对比，不等同于线上独立进程 CPU 使用率。

## Commands

Baseline echo:

```sh
./build/rpc_bench --server both --connections 8 --requests 1000 \
  --payload-size 16 --handler-delay-ms 0 --timeout-ms 3000 --port 19900
```

Connection pressure:

```sh
./build/rpc_bench --server both --connections 32 --requests 1600 \
  --payload-size 16 --handler-delay-ms 0 --timeout-ms 3000 --port 19920
```

Slow handler:

```sh
./build/rpc_bench --server both --connections 16 --requests 800 \
  --payload-size 16 --handler-delay-ms 5 --timeout-ms 5000 --port 19940
```

## Results

### 1. Baseline Echo

| server | conn | req | QPS | P50us | P95us | P99us | maxus | failed | cpu_usr | cpu_sys | rejected | timeout | statuses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Reactor | 8 | 1000 | 34343 | 223 | 330 | 374 | 499 | 0 | 0.025 | 0.049 | 0 | 0 | `0:1000` |
| Coroutine | 8 | 1000 | 34756 | 216 | 344 | 404 | 495 | 0 | 0.019 | 0.051 | 0 | 0 | `0:1000` |

Analysis:

Low concurrency baseline does not show a decisive throughput difference. The coroutine path has a slightly lower P50 and slightly higher P99 in this run, which is consistent with the design goal: coroutine primarily improves connection control flow, not raw echo throughput. Both paths have zero failures, rejected requests, and timeouts.

### 2. Connection Pressure

| server | conn | req | QPS | P50us | P95us | P99us | maxus | failed | cpu_usr | cpu_sys | rejected | timeout | statuses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Reactor | 32 | 1600 | 52508 | 515 | 852 | 3338 | 3832 | 0 | 0.029 | 0.077 | 0 | 0 | `0:1600` |
| Coroutine | 32 | 1600 | 49567 | 594 | 1071 | 1592 | 1985 | 0 | 0.022 | 0.064 | 0 | 0 | `0:1600` |

Analysis:

With more concurrent connections, Reactor QPS is higher in this run, but its tail latency is worse: P99 is 3338us versus Coroutine's 1592us. That suggests the coroutine path pays some scheduling/control overhead at P50/P95, while its per-connection sequential flow avoids a larger tail spike in this scenario. The absence of failures and rejected requests means the result is not caused by overload behavior.

### 3. Slow Handler

| server | conn | req | handler delay | QPS | P50us | P95us | P99us | maxus | failed | cpu_usr | cpu_sys | rejected | timeout | statuses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Reactor | 16 | 800 | 5ms | 632 | 25091 | 27106 | 27916 | 28744 | 0 | 0.048 | 0.117 | 0 | 0 | `0:800` |
| Coroutine | 16 | 800 | 5ms | 621 | 25630 | 27533 | 28163 | 28592 | 0 | 0.075 | 0.115 | 0 | 0 | `0:800` |

Analysis:

The 5ms handler dominates throughput. Both paths converge around 620-630 QPS and P99 around 28ms. This is the expected behavior for a server that dispatches business logic to `ThreadPool`: slow handler work moves the bottleneck to worker queueing instead of blocking the IO thread. Both paths still return all requests successfully, so the IO layer continues to accept/read/write while handlers sleep.

## Bottleneck Summary

| scenario | observed bottleneck | evidence |
| --- | --- | --- |
| baseline echo | protocol + scheduling overhead, no overload | QPS similar, P99 below 500us, no failures |
| connection pressure | connection/event scheduling affects tail latency | Reactor higher QPS, Coroutine lower P99 |
| slow handler | business worker capacity dominates | QPS collapses to handler-delay-limited range; both paths have similar P99 |

## Interview Narrative

This benchmark supports the project narrative:

```text
I implemented two server paths, Reactor + ThreadPool and Coroutine + epoll + ThreadPool.
The coroutine path is not presented as magic QPS optimization; it restores connection handling
to a synchronous ReadFrame -> Dispatch -> WriteFrame flow while keeping nonblocking epoll under it.
In benchmarks, baseline throughput is similar, high-connection tail latency differs by path,
and slow handlers move the bottleneck to the ThreadPool instead of blocking the IO loop.
```

## Next Improvements

- Add a dedicated backpressure benchmark with small ThreadPool queue limits.
- Export P95 in `RpcMetrics`, not only benchmark-side latency vectors.
- Add a longer run with external CPU sampling if absolute CPU claims are needed.
- Add a graceful shutdown benchmark scenario that starts shutdown while slow requests are in flight.
