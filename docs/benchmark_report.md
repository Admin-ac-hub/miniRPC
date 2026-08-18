# miniRPC Benchmark Report

本报告验证稳定 Reactor 基线，并附带实验性协程路径作为控制流对比：

```text
Reactor + epoll + ThreadPool
Coroutine + epoll + ThreadPool
```

## 环境

| item | value |
| --- | --- |
| date | 2026-08-18 |
| worktree base | `070319d-dirty` |
| runtime | Docker container `minirpc-linux` |
| kernel | Linux 6.12.54-linuxkit |
| architecture | aarch64 |
| compiler | GCC 13.3.0 |
| build type | Release |
| load model | same-process, closed-loop, one in-flight request per connection |

`dirty` 表示数据来自本次收口修改完成、正式提交之前的工作区。CPU 数据来自
`getrusage(RUSAGE_SELF)`，包含同一进程中的客户端和服务端线程，只用于两条路径的
同环境比较。

## 固定场景

```sh
./build-release/rpc_bench --server both --connections 32 \
  --requests 100000 --runs 5 --payload-size 16 \
  --handler-delay-ms 0 --timeout-ms 3000 --port 19920
```

两条路径按轮次交替先运行。性能字段取 5 轮中位数，可靠性计数汇总全部 5 轮。

## 结果

| server | runs | conn | req/run | QPS median | P50us | P95us | P99us | maxus | user CPU s | sys CPU s | failed | rejected | timeout | statuses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Reactor | 5 | 32 | 100000 | 83887 | 371 | 609 | 720 | 1970 | 1.074 | 3.302 | 0 | 0 | 0 | `0:500000` |
| Coroutine | 5 | 32 | 100000 | 66104 | 464 | 821 | 964 | 2332 | 1.198 | 3.720 | 0 | 0 | 0 | `0:500000` |

逐轮范围：

| server | QPS range | P99 range | max latency range |
| --- | ---: | ---: | ---: |
| Reactor | 82973-88022 | 694-741us | 1084-3300us |
| Coroutine | 65785-66873 | 958-979us | 1874-3371us |

两条路径各完成 500000 个请求，没有失败、拒绝、超时或状态码异常。当前固定场景中，
Reactor 的吞吐和延迟分位数均优于 Coroutine，因此 Reactor 继续作为稳定默认实现。
协程路径的保留价值是同步风格控制流，而不是性能升级。

## 可靠性证据

同一工作区还通过：

- Linux Debug：14/14。
- Linux Release：14/14。
- ASan/UBSan：14/14。
- TSan 关键并发集合：5/5。
- Release 下 `tcp_server_test`、`rpc_client_test`、
  `coroutine_rpc_connection_test` 各连续 10 次。

自动化场景覆盖半包/粘包、协议错误、最大连接数、连接 churn、fd 复用、慢连接背压、
大响应 partial send、pending 超时清理和 graceful shutdown。

## 解释边界

- 这是同进程闭环测试，不代表跨机器容量或线上 SLA。
- 客户端线程和服务端线程共享 CPU，CPU 时间不是 server-only 数据。
- 没有独立采样 RSS、context switch 或硬件计数器，不据此做微架构归因。
- 固定场景只用于提供可复现基线，不继续扩展 benchmark 模式制造更多数字。

## 面试口径

```text
实现以 epoll + eventfd 驱动的 Reactor RPC 服务端，通过 ThreadPool 隔离业务执行；
在 32 条长连接、每轮 10 万请求的固定闭环场景中运行 5 轮，记录 QPS、延迟分位数、
CPU 时间和全部失败计数。Reactor 中位 QPS 为 83,887、P99 为 720us，100 万次路径
对比请求全部成功；实验协程路径用于控制流对比，不替代稳定 Reactor。
```
