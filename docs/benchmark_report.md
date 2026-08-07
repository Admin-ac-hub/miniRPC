# miniRPC Benchmark Report

本报告保留两类独立对比：

- 2026-08-07：同一 Reactor 路径的 epoll 与 io_uring backend A/B。
- 2026-05-31：Reactor 与 Coroutine 控制流路径的历史对比。

两组数据的目标和代码版本不同，不横向混算。

## 2026-08-07 Reactor Backend A/B

### Environment

| item | value |
| --- | --- |
| commit | `4eb4219-dirty` |
| runtime | Docker container `minirpc-linux` |
| kernel | Linux 6.12.54-linuxkit |
| architecture | aarch64 |
| compiler | GCC 13.3.0 |
| build type | Release |
| liburing | 2.5 for io_uring; not linked for epoll |
| server path | `RpcServer` / Reactor + ThreadPool |
| load model | same-process, closed-loop, one client thread and one in-flight request per connection |
| runs | 5 per backend and scenario |

`rpc_bench` now prints `commit`, `kernel`, `compiler`, `tcp_backend`,
`coroutine_io_backend`, and `liburing` before the run. The epoll and io_uring
binaries came from the same worktree and container.

CPU values use `getrusage(RUSAGE_SELF)`, so they include clients and server in the
same process. They are useful for detecting large A/B changes in this harness, but
are not server-only CPU measurements. No cycles/instructions claim is made without
an external `perf` run.

### Commands

The two Release builds differ only in the Reactor backend:

```sh
cmake -S . -B build-epoll-release -DCMAKE_BUILD_TYPE=Release \
  -DMINIRPC_TCP_SERVER_BACKEND=epoll
cmake -S . -B build-uring-release -DCMAKE_BUILD_TYPE=Release \
  -DMINIRPC_TCP_SERVER_BACKEND=io_uring
```

Each binary was run with `--server reactor --runs 5`. The fixed scenarios were:

```sh
./rpc_bench --server reactor --connections 8   --requests 100000 --payload-size 16    --timeout-ms 3000
./rpc_bench --server reactor --connections 64  --requests 100000 --payload-size 16    --timeout-ms 3000
./rpc_bench --server reactor --connections 16  --requests 1600   --payload-size 16    --handler-delay-ms 5 --timeout-ms 5000
./rpc_bench --server reactor --connections 128 --requests 100000 --payload-size 16    --timeout-ms 3000
./rpc_bench --server reactor --connections 512 --requests 10000  --payload-size 16    --timeout-ms 3000
./rpc_bench --server reactor --connections 16  --requests 2000   --payload-size 65536 --timeout-ms 3000
```

Ports were assigned separately for each run. Raw five-run outputs remain in the
validation container as `/tmp/minirpc-bench-*.txt`.

### Five-Run Medians

| scenario | epoll QPS | io_uring QPS | QPS delta | epoll P99us | io_uring P99us | P99 delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 conn, 100k, 16B | 47,236 | 48,772 | +3.3% | 292 | 299 | +2.4% |
| 64 conn, 100k, 16B | 97,039 | 113,453 | +16.9% | 1,325 | 1,184 | -10.6% |
| 16 conn, 1,600, 5ms handler | 640 | 638 | -0.3% | 27,384 | 27,309 | -0.3% |
| 128 conn, 100k, 16B | 99,939 | 147,682 | +47.8% | 2,169 | 1,797 | -17.2% |
| 512 conn, 10k, 16B | 68,228 | 128,028 | +87.6% | 10,203 | 76,795 | +652.7% |
| 16 conn, 2k, 64KiB | 27,531 | 21,377 | -22.4% | 1,090 | 1,376 | +26.2% |

All listed requests succeeded. Across every five-run group, failed, rejected, and
timeout counts were zero and every response status was `StatusCode::kOk`.

The 64 KiB case was repeated as three independent five-run groups because the first
result crossed the 5% regression gate:

| group | epoll QPS | io_uring QPS | io_uring delta | epoll P99us | io_uring P99us |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 27,531 | 21,377 | -22.4% | 1,090 | 1,376 |
| 2 | 25,219 | 21,384 | -15.2% | 1,451 | 1,328 |
| 3 | 27,237 | 22,091 | -18.9% | 1,168 | 1,362 |

This is a sustained io_uring throughput regression, not a single noisy run.

### Interpretation

The data does not support switching the default backend:

- io_uring improves small-message throughput at 64 and 128 connections in this
  environment.
- The slow-handler case converges because the four handler workers are the
  bottleneck.
- 64 KiB throughput regresses by 15-22% across three independent groups.
- At 512 connections, io_uring has much higher QPS but a 76.8 ms median P99. The
  harness also creates 512 client threads, so this result mixes backend behavior
  with client scheduling and must not be presented as a server capacity claim.

An attempted 512-connection, 100k-request run exceeded the 90-second validation
budget in one epoll round. The reported 512-connection scenario therefore uses
10k requests. Before changing the default, the 64 KiB path and 512-connection tail
need external-process profiling, context-switch/RSS sampling, and `perf stat`
cycles/instructions. epoll remains the default.

## Reliability Evidence

Both backends passed the same 14-test suite in Debug, Release, and ASan/UBSan
builds. Dedicated TSan runs of `tcp_server_test` also passed for both backends.
The graceful-shutdown suite includes a deterministic slow-reader regression: an
8 MiB response is held behind socket backpressure, `pending_requests` must remain
one while `Stop()` is draining, and the server may stop only after the complete
frame is accepted by the kernel send path. This passed with both backends.

TSan on the broader integration test reports an existing `TcpClient::Close()`
versus reader-thread `recv()` fd race in `src/net/tcp_client.cpp`. The same
client code is used with both server backends; this warning is not evidence of an
io_uring server race and remains a separate client task.

The io_uring Release `tcp_server_test` and TCP integration test each completed 100
repeat rounds. The Release `rpc_client_test`, which includes the 8 MiB shutdown
regression, completed 100/100 rounds with io_uring and 100/100 rounds with epoll.
An earlier epoll repeat stalled around iteration 64 after all server and socket
fds had closed; the remaining threads were in the `RpcClient` timeout-cleaner and
join path. A fresh 100-round run crossed that point and completed. This remains a
client lifecycle investigation rather than evidence of a server backend hang.

A broader repeat also exposed a hang in iteration 38 of
`coroutine_rpc_server_test`. That test uses the independent
`CoroutineRpcServer + CoroutineIoContext + epoll` path and does not instantiate the
selected `TcpServer` backend. It is retained as a separate pre-existing coroutine
stability issue rather than being attributed to io_uring.

## Historical: Reactor vs Coroutine

The following 2026-05-31 snapshot compared control-flow paths, not Reactor
backends. Both paths used epoll. It is retained for history and is not combined
with the 2026-08-07 A/B data.

| scenario | Reactor QPS / P99us | Coroutine QPS / P99us | failures |
| --- | ---: | ---: | ---: |
| 8 conn, 1,000, 16B | 34,343 / 374 | 34,756 / 404 | 0 |
| 32 conn, 1,600, 16B | 52,508 / 3,338 | 49,567 / 1,592 | 0 |
| 16 conn, 800, 5ms handler | 632 / 27,916 | 621 / 28,163 | 0 |

The historical conclusion remains limited: coroutine structure restores a
synchronous `ReadFrame -> Dispatch -> WriteFrame` flow; it is not presented as a
universal QPS optimization.

## Next Work

- Investigate the client close/reader/timeout-cleaner lifecycle race and the
  one observed epoll repeat stall independently of the server backend work.
- Profile the io_uring 64 KiB send path and 512-connection tail before discussing
  a default switch.
- Move clients and server into separate processes for server-only CPU and memory
  evidence.
- Add dedicated slow-reader and connect/disconnect benchmark modes. Correctness
  coverage for backpressure and connection churn already exists in automated tests.
