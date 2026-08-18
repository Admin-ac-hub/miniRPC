# 性能分析与压测方法

miniRPC 的压测以稳定 Reactor 路径为基线，并用可复现的固定场景附带比较实验性协程路径：

```text
Reactor + epoll + ThreadPool
Coroutine + epoll + ThreadPool
```

当前实测数据和环境信息见 [benchmark_report.md](benchmark_report.md)。

## 负载模型

`rpc_bench` 是同进程、闭环的端到端压测：benchmark 进程同时运行客户端和服务端，客户端收到上一条响应后才在同一连接发送下一条请求。

- `--connections N` 创建 `N` 个客户端线程和 `N` 条长连接，每条连接最多有一个在途请求，因此最大在途请求数也是 `N`。
- `--requests N` 是每轮所有连接合计的请求数，不是每条连接的请求数。
- 每轮会创建新的服务端和客户端；所有连接成功 warmup 后才开始计时，连接关闭和线程退出不计入耗时。
- `--server both` 时，两条服务端路径按轮次交替先运行，降低固定执行顺序带来的偏差。
- `--runs N` 默认执行 5 轮。QPS、耗时、延迟和 CPU 时间按字段取中位数；成功、失败、拒绝、超时和状态码计数跨所有轮求和，避免偶发错误被中位数隐藏。

这套模型适合验证 Reactor 基线并比较控制流差异，但不是开放到达率测试，也不能把结果直接解释为独立服务端的线上容量或 SLA。

## 固定场景

最终报告只保留一组可复现的连接压力场景：Release 构建、32 条连接、每轮 100000
个请求、16B payload、4 个业务 worker 和 `10000` 的线程池队列上限。

```sh
./build-release/rpc_bench --server both --connections 32 --requests 100000 \
  --runs 5 --payload-size 16 --handler-delay-ms 0 --timeout-ms 3000 --port 19920
```

输出包含两部分：`raw runs` 保留每轮原始值，`summary` 输出性能字段中位数和可靠性计数总和。主要字段为：

- `qps_med`、`p50_med`、`p95_med`、`p99_med`：5 轮中位数，延迟单位为微秒。
- `usr_s_med`、`sys_s_med`：`getrusage(RUSAGE_SELF)` 得到的同进程 CPU 秒数中位数，不是 CPU 利用率。
- `fail_sum`、`rej_sum`、`tout_sum`：所有轮的失败、拒绝和超时总数。
- `statuses_sum`：所有轮的响应状态码分布；`0:500000` 表示 5 轮共 500000 个请求全部成功。

## 指标闭环

压测和运行时调试对应同一组服务端指标：

| 指标 | 含义 | 实现状态 |
| --- | --- | --- |
| `active_connections` | 当前连接数 | Reactor / Coroutine 已记录 |
| `total_requests` | 收到的请求总数 | Reactor / Coroutine 已记录 |
| `total_responses` | 整帧写成功的响应总数，包含成功、错误、超时和拒绝响应 | Reactor / Coroutine 已记录 |
| `success_requests` | 成功响应数 | 已记录 |
| `failed_requests` | 错误终态或响应写回失败数 | 已记录 |
| `timeout_requests` | 服务端返回 TIMEOUT 的请求数，属于 failed 子集 | 已记录 |
| `rejected_requests` | 服务端拒绝的请求数，属于 failed 子集 | 已记录 |
| `pending_requests` | 等待 handler 或等待响应 terminal write completion 的请求数 | 已记录 |
| `avg_latency` | 从收到请求到响应写成功/失败终态的全生命周期累计平均延迟 | 已记录 |
| `p50/p95/p99_latency` | 最近最多 10000 条上述全生命周期延迟样本的分位数 | 已记录 |
| `threadpool_queue_size` | 业务线程池当前排队长度 | 已记录 |

`RpcMetrics::ToPrometheusText()` 和服务端 `MetricsText()` 导出 Prometheus 文本格式。`total_responses` 与请求终态分类彼此独立：响应写回失败会增加 `failed_requests`，但不会增加 `total_responses`。

## 解释边界

- CPU 数据包含同一进程内的客户端线程和服务端线程，只适合同一环境中的路径对比。
- 当前表格不包含 server-only CPU、cycles/instructions、RSS 或 context switches；这些指标需要外部分进程与 `perf stat`/进程采样，缺少证据时不作相关归因。
- 分位数来自客户端端到端延迟；服务端 metrics 的分位数用于定位服务端处理阶段，两者不能混为一谈。
- 当前 benchmark CLI 不修改线程池容量。队列拒绝和慢连接背压主要由 `tcp_server_test`、`rpc_client_test` 覆盖，协程路径另有独立回归，不在结果表中制造未落地的可配置场景。

## 分析顺序

每次更新报告时按以下顺序记录：

1. 环境：commit、CPU、内核、编译器、构建类型、worker、连接数、payload 和轮数。
2. 结果：QPS、P50/P95/P99、失败总数和 CPU 时间。
3. 证据：逐轮原始输出、metrics、日志或 profiling 数据。
4. 结论：区分直接观察与原因推断；没有 profiling 证据时不把差异归因到某个函数或调度机制。

## 简历口径

```text
实现以 epoll + eventfd 驱动的 Reactor RPC 服务端，将 IO 与业务线程池隔离；
设计同进程闭环压测，在固定连接压力场景下记录 QPS、P50/P95/P99、失败、
拒绝、超时和状态码，并用实验性协程路径做控制流对比。
```
