# miniRPC 架构分层

## 分层

```text
Application
  -> client / server
  -> protocol
  -> net
  -> runtime / observability
  -> core
```

## 模块说明

- `core`：跨模块基础类型，不依赖其他业务模块。
- `protocol`：RPC 协议帧、消息类型、Protobuf body 和编解码。
- `net`：TCP 连接、客户端、服务端监听。
- `client`：面向调用方的 RPC 调用入口。
- `server`：服务注册、服务启动和请求分发。
- `runtime`：线程池、任务调度等运行时能力。
- `observability`：日志和运行时指标。

## 服务端路径

miniRPC 以 Reactor 为稳定默认实现。协程服务端作为独立实验入口保留，用于对比控制流模型，不改变 `RpcServer` 的默认行为。

### Reactor + ThreadPool

```text
TcpServer facade
  -> epoll Reactor
  -> accept/read/write
  -> decode ProtocolFrame
  -> ThreadPool::Post(handler)
  -> response queue
  -> eventfd wakeup
  -> write response
```

这个路径把 IO 和业务执行分离。`TcpServer` 是稳定 facade，reactor 线程只处理连接生命周期和收发；完整请求进入 `ThreadPool`，worker 生成响应后通过 `eventfd` 唤醒 reactor 写回。线程池队列满时 `Post()` 返回 `false`，调用方必须返回错误或关闭连接，不能无限排队。

资源边界由 `TcpServerOptions` 统一配置：`max_connections` 限制同时接入的连接数，读写缓冲和跨线程响应队列均有硬上限，高低水位只暂停发生背压的连接。`max_connections = 0` 表示不限制连接数。

`RpcServer` 的 pending request 生命周期从请求准入持续到响应整帧被内核发送接口接受。后端私有 write-completion callback 在完整发送、连接关闭、取消、队列清理或 hard stop 时恰好完成一次，graceful shutdown 据此等待真实的响应写入进度，而不是只等待 handler 返回或 response 入队。这个完成点不是应用层 ACK，不保证对端业务代码已经读取响应。

公共 `TcpServer::SendFrame()` 签名和入队语义保持不变；只有 `RpcServer` 使用内部 completion 路径。epoll 在 `send(2)` 消耗整帧后成功。Running -> Draining 的状态切换、请求/拒绝响应的 pending 登记以及最终 drain seal 使用同一同步边界，避免 Stop 观察到零后又登记待写响应。

`TcpServer` 网络 callback 可以调用 `TcpServer::Stop()`，reactor 会在当前 callback 返回后完成清理，下一次外部 `Start()` 会先回收已退出线程。callback 不得直接析构其所属 `TcpServer` / `RpcServer`；对象销毁必须由非 reactor 线程负责并等待 `Stop()` 完成。更上层的 `RpcServer::Stop()` 会等待业务和响应写完成，因此必须从业务 handler 之外的线程调用；同样只支持前一次 `Stop()` 返回后再 `Start()`，不支持 draining 期间并发重启。

### 实验路径：Coroutine + epoll + ThreadPool

```text
connection coroutine
  -> ReadFrame
  -> DecodeRequest
  -> Dispatch
  -> WriteFrame
```

协程路径解决的是控制流问题：传统连接处理容易演进成状态机和回调，用户态协程把连接流程恢复成同步风格。底层 socket 仍是非阻塞的，读写遇到 `EAGAIN` 时挂起当前协程，epoll 事件到来后恢复。业务 handler 仍然通过 `ThreadPool` 执行，避免慢请求阻塞 IO 线程。

跨线程 worker completion 使用 `CoroutineHandle` 投递到 IO control queue，再由 `eventfd` 唤醒 IO 线程；worker 不直接修改 scheduler。`CoroutineRpcServer::Stop()` 先进入 Draining，在 IO 线程关闭 listener，等待已准入请求达到写完成或明确失败，再取消连接与 waiter。每次 `Start()` 创建新的 IO context，因此支持前一次 `Stop()` 返回后的重启。协程栈由 `mmap` 分配并带双侧 guard page。

这条路径仍是实验入口：当前没有连接空闲、读写超时和最大连接数配置，也不会替代稳定 Reactor 路径。当前收口范围见 [coroutine_runtime_minimal_plan.md](coroutine_runtime_minimal_plan.md)。

## 可观测性

压测时看到的 QPS、P95/P99 和失败数需要能在服务端指标中找到对应原因。当前已经记录：

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
