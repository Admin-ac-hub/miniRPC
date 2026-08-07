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
TcpServer facade
  -> EpollTcpServerBackend (default)
  -> IoUringTcpServerBackend (build-time opt-in)
  -> accept/read/write
  -> decode ProtocolFrame
  -> ThreadPool::Post(handler)
  -> response queue
  -> eventfd wakeup
  -> write response
```

这个路径把 IO 和业务执行分离。`TcpServer` 是稳定 facade，CMake 的 `MINIRPC_TCP_SERVER_BACKEND` 在 epoll 与 io_uring backend 之间选择，默认值为 epoll，不提供运行时 fallback。reactor 线程只处理连接生命周期和收发；完整请求进入 `ThreadPool`，worker 生成响应后通过 `eventfd` 唤醒 reactor 写回。线程池队列满时 `Post()` 返回 `false`，调用方必须返回错误或关闭连接，不能无限排队。

`RpcServer` 的 pending request 生命周期从请求准入持续到响应整帧被内核发送接口接受。后端私有 write-completion callback 在完整发送、连接关闭、取消、队列清理或 hard stop 时恰好完成一次，graceful shutdown 据此等待真实的响应写入进度，而不是只等待 handler 返回或 response 入队。这个完成点不是应用层 ACK，不保证对端业务代码已经读取响应。

公共 `TcpServer::SendFrame()` 签名和入队语义保持不变；只有 `RpcServer` 使用内部 completion 路径。epoll 在 `send(2)` 消耗整帧后成功，io_uring 在累计 send CQE 覆盖整帧后成功。Running -> Draining 的状态切换、请求/拒绝响应的 pending 登记以及最终 drain seal 使用同一同步边界，避免 Stop 观察到零后又登记待写响应。

io_uring backend 的 SQE 提交和 CQE 消费只发生在 IO 线程。每个 accept、wake poll、recv、send 和 cancel 都使用不复用的 operation token；连接相关 completion 还必须匹配 `ConnectionId + generation`。关闭连接时先停止 rearm 并异步取消在途操作，等持有 buffer 的目标 CQE 与 cancel CQE 都回收后再关闭 fd 和触发 `OnClose`，避免 fd 复用、迟到 CQE 和 buffer 生命周期问题。

io_uring 构建要求 liburing 2.0+。启动时还要求 Linux 提供 `IORING_FEAT_NODROP`、`IORING_FEAT_FAST_POLL` 以及 accept/recv/send/poll/cancel opcode；探测失败返回 `kNetworkError`。当前验证环境为 Linux 6.12.54 和 liburing 2.5。

`TcpServer` 网络 callback 可以调用 `TcpServer::Stop()`，reactor 会在当前 callback 返回后完成清理，下一次外部 `Start()` 会先回收已退出线程。callback 不得直接析构其所属 `TcpServer` / `RpcServer`；对象销毁必须由非 reactor 线程负责并等待 `Stop()` 完成。更上层的 `RpcServer::Stop()` 会等待业务和响应写完成，因此必须从业务 handler 之外的线程调用；同样只支持前一次 `Stop()` 返回后再 `Start()`，不支持 draining 期间并发重启。

### Coroutine + epoll + ThreadPool

```text
connection coroutine
  -> ReadFrame
  -> DecodeRequest
  -> Dispatch
  -> WriteFrame
```

协程路径解决的是控制流问题：传统连接处理容易演进成状态机和回调，用户态协程把连接流程恢复成同步风格。底层 socket 仍是非阻塞的，读写遇到 `EAGAIN` 时挂起当前协程，epoll 事件到来后恢复。业务 handler 仍然通过 `ThreadPool` 执行，避免慢请求阻塞 IO 线程。Reactor 的 backend 选项不改变这条路径；协程 IO 仍固定使用 epoll。

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
