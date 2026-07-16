# miniRPC 架构分层

## 分层

```text
Application
  -> client / server
  -> discovery
  -> serialization
  -> protocol
  -> net
  -> runtime / observability
  -> core
```

## 模块说明

- `core`：跨模块基础类型，不依赖其他业务模块。
- `protocol`：RPC 协议帧、消息类型和编解码。
- `serialization`：请求响应对象和字节流之间的转换。
- `net`：TCP 连接、客户端、服务端监听。
- `client`：面向调用方的 RPC 调用入口。
- `server`：服务注册、服务启动和请求分发。
- `discovery`：服务解析和负载均衡。
- `runtime`：线程池、任务调度等运行时能力。
- `observability`：日志、指标和后续 tracing。

## 服务端路径

miniRPC 保留两条服务端实现路径，用于对比不同控制流模型下的行为，而不是只追单一 QPS 数字。

### Reactor + ThreadPool

```text
epoll reactor
  -> accept/read/write
  -> decode ProtocolFrame
  -> ThreadPool::Post(handler)
  -> response queue
  -> eventfd wakeup
  -> write response
```

这个路径把 IO 和业务执行分离。reactor 线程只处理连接生命周期和收发；完整请求进入 `ThreadPool`，worker 生成响应后通过 `eventfd` 唤醒 reactor 写回。线程池队列满时 `Post()` 返回 `false`，调用方必须返回错误或关闭连接，不能无限排队。

### Coroutine + epoll + ThreadPool

```text
connection coroutine
  -> ReadFrame
  -> DecodeRequest
  -> Dispatch
  -> WriteFrame
```

协程路径解决的是控制流问题：传统 epoll 连接处理容易演进成状态机和回调，用户态协程把连接流程恢复成同步风格。底层 socket 仍是非阻塞的，读写遇到 `EAGAIN` 时挂起当前协程，epoll 事件到来后恢复。业务 handler 仍然通过 `ThreadPool` 执行，避免慢请求阻塞 IO 线程。

## 可观测性目标

性能分析需要形成闭环：压测时看到的 QPS、P95/P99 和失败数，必须能在服务端指标中找到对应原因。近期优先补齐：

- `active_connections`
- `total_requests`
- `success_requests`
- `failed_requests`
- `timeout_requests`
- `pending_requests`
- `rejected_requests`
- `avg_latency`
- `p95_latency`
- `p99_latency`
- `threadpool_queue_size`

压测报告结构见 [performance.md](performance.md)。
