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

`ThreadPool::Stop()` 拒绝新任务，但继续执行所有已接收任务，队列排空后 join worker；返回时队列为空，重启不会重放旧任务。生命周期操作应由 worker 之外的线程串行调用。`RpcServer::Stop(grace)` 超时后先关闭连接、主动失败客户端 pending，再等待线程池排空；grace 限制的是连接和响应排空的等待时间，不是整个 `Stop()` 的耗时上限。阻塞不返回的 handler 仍会阻止停机完成。

资源边界由 `TcpServerOptions` 统一配置：`max_connections` 限制同时接入的连接数，读写缓冲和跨线程响应队列均有硬上限，高低水位只暂停发生背压的连接。`max_connections = 0` 表示不限制连接数。

`max_frame_body_size` 默认是 2 MiB 减 20 字节，`max_read_buffer_bytes` 默认是 2 MiB。`Start()` 在监听前校验 body 上限不超过协议的 16 MiB，且读缓冲能容纳整个最大帧；非法配置返回 `kServerError`。配置应在 `Start()` 前或外部线程调用 `Stop()` 返回后设置，重启会应用新的解码上限。运行中或 reactor 尚未 join 时调用 `SetOptions()` 会被忽略并记录 warn。读循环按剩余缓冲预算限制 `recv` 长度，每轮先解码完整帧；多个合法粘包不会因累计流量超过预算而被关闭。

空闲连接由 `idle_timeout_ms` 控制：连接超过该时长没有成功读写（含 accept、收到字节、写出字节，背压中的连接照常计时）即被服务端关闭并立即释放连接配额；`0` 表示禁用（默认，保持既有行为）。实现是最小堆 + 惰性删除：每个连接在堆中至多一个 pending 定时器项，`epoll_wait` 超时取 `min(1000ms, 最近到期)`，项到期时若连接已关闭或 `generation`/`activity_seq` 不匹配则直接丢弃或按最新活跃时间重推，不会误关活跃连接。

`RpcServer` 的响应 body 与默认接收预算对齐，上限为 2 MiB − 20 字节（包括 Protobuf 开销）。序列化后超限时改回 `kSerializeError` 应用级响应，保留请求 ID、清空 payload，连接继续可用；正常写出该错误响应后计入 `total_responses` 和 `failed_requests`。

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

## 客户端接收边界

`TcpClientOptions::max_read_buffer_bytes` 默认是 2 MiB，通过 `TcpClient(options)` 在构造时设置，连接期间不变。有效 body 上限是 `min(16 MiB, max_read_buffer_bytes - 20)`；小于固定头的配置在 `Connect()` 返回 `kNetworkError`，不会创建 socket。

客户端同样按剩余预算读取并及时解码完整帧。帧头声明超限或缓冲已满仍需更多数据时，关闭连接并只触发一次 `OnClose(kProtocolError, message)`。`RpcClient` 沿用现有断线语义，把该连接的所有 pending 请求完成为 `kNetworkError`，无需等到调用 deadline。默认 `RpcClient` 因而最多接收 2 MiB 整帧；更大的接收预算目前只在低层 `TcpClient` 配置，`RpcClient` / `RpcServer` 尚未提供 TCP 选项透传。

## 可观测性

压测时看到的 QPS、P95/P99 和失败数需要能在服务端指标中找到对应原因。当前已经记录：

- `active_connections`
- `total_requests`
- `success_requests`
- `failed_requests`
- `timeout_requests`
- `pending_requests`
- `rejected_requests`
- `protocol_error_total`
- `avg_latency`
- `p95_latency`
- `p99_latency`
- `threadpool_queue_size`

服务端帧头解码失败（非法 magic/version/type/codec 或 body 声明超限）在断连前累计 `protocol_error_total`，通过快照及 Prometheus 的 `minirpc_protocol_error_total` 导出。这些帧尚未进入 RPC 请求处理，不计入 `failed_requests` 或 `rejected_requests`；正常断连不增加该计数。客户端暂未接入此指标。

当失败发生在 magic/version 校验之后（msg_type / codec / body_size 超限），`TryDecode` 会把已解析的 `request_id` 填进 `frame`；服务端据此回一个 `kProtocolError` 错误响应并设 `close_after_send`，**响应写出后**才关闭连接——客户端因此能拿到明确的失败原因，而不是无声断连。magic/version 不符时头字段不可信，不构造响应，直接关闭。

延迟采样使用 10000 个共享原子槽，写入不获取 mutex、不分配内存；样本计数、累计耗时和写入游标分散到 8 个固定原子分片，查询时合并。分片只使用线程本地整数索引，不保存对象指针，内存占用不随线程或请求数增长。分位数查询复制已发布的槽并在本地排序，不阻塞采样线程。单一采样线程会保留最后 10000 个样本；多个分片的游标独立、可以覆盖同一槽，因此多线程分位数使用近似的近期样本，不能解释为严格的全局最近 10000 次记录。样本总数与累计平均值覆盖全部记录，写入停止后可精确核对；并发快照不保证各字段来自同一时刻。

压测报告结构见 [performance.md](performance.md)。
