# miniRPC

[![Linux CI](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml/badge.svg?branch=main)](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml)

miniRPC 是一个 C++17 RPC 框架项目，重点放在 Linux 非阻塞网络、二进制协议、超时错误模型、业务线程池分发，以及 reactor 和用户态协程两条服务端路径的工程化对比。

项目目标不是只给出一个 QPS 数字，而是解释 RPC 服务端在不同压力下的表现：低并发延迟、连接管理开销、慢 handler 下的业务排队、P50/P95/P99 延迟和 CPU 时间。压测方法见 [docs/performance.md](docs/performance.md)，当前实测报告见 [docs/benchmark_report.md](docs/benchmark_report.md)。

## 模块

- `include/minirpc/core`：基础类型、状态码、端点。
- `include/minirpc/protocol`：协议帧、请求响应模型、编解码。
- `include/minirpc/serialization`：序列化抽象。
- `include/minirpc/net`：网络连接和 TCP server/client 抽象。
- `include/minirpc/client`：客户端调用入口。
- `include/minirpc/server`：服务端和服务注册。
- `include/minirpc/discovery`：服务发现和负载均衡。
- `include/minirpc/runtime`：线程池等运行时组件。
- `include/minirpc/observability`：日志和指标。

路线文档见 [RPC_FRAMEWORK_ROADMAP.md](RPC_FRAMEWORK_ROADMAP.md)，架构分层见 [docs/architecture.md](docs/architecture.md)，协程设计见 [docs/coroutine.md](docs/coroutine.md)。

## 构建与测试

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

服务端依赖 Linux `epoll + eventfd`。在 macOS 开发机上不要用本地构建结果验证服务端行为，真实验证应进入项目 Linux 容器：

```sh
docker start minirpc-linux
docker exec minirpc-linux bash -c "cd /work && cmake --build build -j && ctest --test-dir build --output-on-failure"
```

## 当前实现

### Reactor + ThreadPool

- `RpcServer::Start()` 启动后台 reactor 线程。
- reactor 负责 accept、读写事件、连接生命周期和响应队列。
- 完整请求投递到 `ThreadPool` 执行业务 handler。
- worker 线程通过响应队列和 `eventfd` 唤醒 reactor 写回响应。
- `ThreadPool::Post()` 队列满会返回失败，服务端返回明确的 `kServerError` 响应并关闭连接，避免无限堆积请求。

客户端 `RpcClient` 维护一条长连接，内部 reader 线程按 `request_id` 匹配响应；同步 `Call()` 和异步 `CallAsync()` 共用同一张 pending map、请求 deadline 和超时清理线程。

### Backpressure / Benchmark / Metrics

Reactor 路径为每个连接维护独立的分段输出缓冲。当某个连接因为慢客户端或对端不读响应导致待写数据积压时，服务端不会让该连接无限制继续读入新请求：

- `high_watermark_bytes` 默认 `1MB`：待写字节数达到高水位后进入背压状态。
- `low_watermark_bytes` 默认 `256KB`：待写字节数降到低水位后退出背压状态。
- 进入背压时从该 fd 的 epoll interest 暂时移除 `EPOLLIN`，保留 `EPOLLOUT` 继续刷出响应。
- 退出背压时恢复 `EPOLLIN`。
- 状态变化会输出日志：`enter backpressure fd=<fd> write_buffer_size=<bytes>` / `leave backpressure ...`。

背压只作用于单个慢连接，不暂停 accept，也不影响其他正常连接继续收发。`max_write_buffer_bytes` 仍是硬上限，默认 `16MB`，用于防止极端情况下内存无限增长。

benchmark target 为 `rpc_bench`，会启动内置 echo server 并模拟多客户端请求：

```sh
docker start minirpc-linux
docker exec minirpc-linux bash -c "cd /work && cmake --build build -j && ./build/rpc_bench --server reactor --host 127.0.0.1 --port 19700 --connections 64 --requests 100000 --runs 5 --payload-size 128 --timeout-ms 3000"
```

也可以一次跑多个 payload，用于对比小包和大包路径：

```sh
docker exec minirpc-linux bash -c "cd /work && ./build/rpc_bench --server reactor --host 127.0.0.1 --port 19700 --connections 64 --requests 1000 --runs 5 --payload-sizes 64,1024,65536 --timeout-ms 3000"
```

`--connections N` 会创建 `N` 个客户端线程和 `N` 条长连接，每条连接同步串行调用；`--requests` 是每轮所有连接合计的请求数。输出保留逐轮原始表，并汇总 QPS、延迟和 CPU 时间的跨轮中位数；失败、拒绝、超时和状态码按全部轮次求和，避免偶发错误被中位数隐藏。

服务端 metrics 保存在内存中。`avg` 使用全生命周期累计值，`p50 / p95 / p99` 从最近最多 `10000` 条请求延迟样本计算。普通 RPC 客户端可调用内置方法查询：

```cpp
auto response = client.Call("rpc", "metrics", "", std::chrono::seconds(2));
```

返回文本包含：

```text
minirpc_active_connections 1
minirpc_requests_total 100
minirpc_responses_total 100
minirpc_failed_requests_total 0
minirpc_backpressure_connections 0
minirpc_max_write_buffer_size 0
minirpc_server_state 0
minirpc_inflight_requests 0
minirpc_shutdown_start_time_ms 0
minirpc_graceful_shutdown_timeout_total 0
minirpc_avg_latency_us 120
minirpc_p50_latency_us 90
minirpc_p95_latency_us 240
minirpc_p99_latency_us 420
```

Reactor 背压自动化测试在 `tcp_server_test` 中构造慢客户端：该客户端批量发送请求但不读取响应，触发单连接 `write_buffer` 超过高水位；测试同时创建正常客户端，验证正常请求仍能收到响应。运行方式：

```sh
docker exec minirpc-linux bash -c "cd /work && cmake --build build -j && ctest --test-dir build --output-on-failure -R 'tcp_server_test|metrics_test|rpc_client_test'"
```

验证背压生效主要看三点：测试通过；日志中出现 `enter backpressure` 和 `leave backpressure`；metrics 中 `minirpc_backpressure_connections` 在慢客户端期间升高，`minirpc_max_write_buffer_size` 超过 `high_watermark_bytes`。

### Performance Path Optimization

`TcpServer::SendFrame()` 保持原公共签名不变，内部将 `ProtocolFrame` encode 成 `std::string` 后通过 `std::move` 放入响应队列；reactor drain 响应队列时继续移动到连接输出缓冲。这样避免了原先 `const std::string` 入队、`write_buffer += data`、`data.substr(sent)` 带来的重复深拷贝。

每条连接的输出缓冲现在由 `std::deque<std::string> write_buffers`、`write_buffer_offset` 和 `write_buffer_bytes` 组成。发送完成一段就 `pop_front()`，部分发送只推进首段 offset；不会再对大 string 做 `erase(0, n)`。避免 `erase(0, n)` 的原因是它会把未发送尾部整体前移，慢客户端或 64KB+ payload 连续发送时会把 IO 热路径变成频繁线性内存搬移。

EPOLL 行为保持不变：待写字节非空时关注 `EPOLLOUT`，清空后移除 `EPOLLOUT`；背压水位使用 `write_buffer_bytes`，慢连接只暂停自己的 `EPOLLIN`，不影响其他连接。

### Client Production Features

同步调用仍使用原 API：

```cpp
auto response = client.Call("EchoService", "Echo", "hello", std::chrono::seconds(2));
```

异步调用返回 `std::future<RpcResponse>`：

```cpp
auto future = client.CallAsync("EchoService", "Echo", "hello", std::chrono::seconds(2));
auto response = future.get();
```

`Call()` 内部基于 `CallAsync().get()` 实现。每个 pending 请求记录独立 `expire_time`，后台 timeout cleaner 会在超时后移出 pending map 并返回 `kTimeout` 响应；reader 线程收到迟到响应时，如果 pending 已不存在会丢弃，如果刚好过期则返回 timeout 而不是成功响应。

简单重连通过 `RpcClientOptions` 配置：

```cpp
minirpc::RpcClientOptions options;
options.max_reconnect_attempts = 5;
options.reconnect_interval = std::chrono::milliseconds(20);
client.SetOptions(options);
```

连接断开时已有 pending 请求会失败返回；后续请求会在自身 deadline 内尝试 reconnect，不会无限阻塞。连接池先提供轻量 `RpcClientPool` 骨架，固定 N 个 `RpcClient` 并用 round-robin 分发 `Call()` / `CallAsync()`。

### Graceful Shutdown

`RpcServer` 现在有明确 shutdown 状态机：

- `ShutdownState::kRunning`：正常 accept 新连接并处理请求。
- `ShutdownState::kDraining`：`Stop(grace_period)` 已开始，停止 accept；已进入 handler 的请求继续执行；同一已有连接上的新请求返回 `kServerError` 和 `server shutting down`。
- `ShutdownState::kStopped`：TCP reactor 和线程池已停止。

`Stop(grace_period)` 的顺序是：切到 Draining，记录 `shutdown_start_time_ms`，停止 accept，等待 `inflight_requests` 在 grace period 内归零；若超时则增加 `graceful_shutdown_timeout_total`，随后关闭 TCP 连接并停止线程池。现有线程池会等待正在执行的 C++ handler 返回；框架不会强杀业务线程，因此业务 handler 不应永久阻塞。

### Benchmark Result

完整环境、命令、逐场景分析和 CPU 时间口径见 [docs/benchmark_report.md](docs/benchmark_report.md)。

### Coroutine + epoll + ThreadPool

协程路径不改变现有 `RpcServer` / `RpcClient` / `ProtocolFrame` 公共 API，而是提供独立的 `CoroutineRpcServer` 入口：

```text
ReadFrame
DecodeRequest
Dispatch
WriteFrame
```

连接处理代码保持同步风格；底层非阻塞 IO 遇到 `EAGAIN` 时挂起当前协程，后续由 epoll 事件恢复。业务 handler 仍通过 `ThreadPool` 执行，worker 完成后通过 `eventfd` 唤醒 IO 线程恢复等待响应的协程。

## 近期主线

1. 补混合快慢请求压测，验证慢 handler 对快请求尾延迟的影响；当前固定延迟场景只用于观察业务线程池排队。
2. 增加独立进程和外部 CPU profiling，补充当前同进程闭环压测的边界。
3. 继续完善 deadline：客户端 timeout 已传播为请求 deadline，服务端在分发前拒绝已过期请求；后续可补 handler 内剩余时间检查。
4. 继续完善优雅关闭：`Stop(grace_period)` 已停止 accept 新连接，并允许已接收请求在 grace period 内完成；后续可补 shutdown 压测场景。

当前不优先扩展 HTTP、TLS、复杂注册中心、syscall hook 或分布式 tracing，避免横向铺开半成品。

## 简历口径

实现 C++17 miniRPC 框架，基于 Linux epoll/eventfd 构建非阻塞网络层；设计固定头二进制协议和 Protobuf 请求体，解决 TCP 半包/粘包；实现用户态协程调度器，将 RPC 连接处理封装为同步风格的 `ReadFrame -> Dispatch -> WriteFrame` 流程；业务 handler 通过线程池执行，IO 协程通过 eventfd 跨线程恢复；支持请求超时、deadline 传播、错误码、基础背压和 metrics 导出，并在固定场景中各运行 5 轮，以中位数对比 Reactor 与协程路径的 QPS/P50/P95/P99 和 CPU 时间。
