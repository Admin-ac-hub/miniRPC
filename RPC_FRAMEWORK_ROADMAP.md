# miniRPC 项目范围与收口路线

## 1. 项目定位

miniRPC 是一个面向 Linux 的 C++17 RPC 框架，核心目标是完整实现并验证一条
`epoll Reactor + ThreadPool` RPC 链路，而不是堆叠大量未接入的分布式组件。

稳定主路径：

```text
RpcClient
  -> request_id / deadline / pending map
  -> TCP long connection
  -> 20-byte ProtocolFrame
  -> epoll Reactor
  -> ThreadPool handler
  -> eventfd response wakeup
  -> RpcResponse
```

用户态协程服务端保留为独立实验路径，用于展示同步风格的非阻塞 IO 控制流，
不替代 `RpcServer`，也不是项目完整性的前提。

## 2. 已完成范围

### 协议与 RPC 语义

- 固定 20 字节网络序协议头和有上限的可变 body。
- 半包、粘包、非法 magic/version/type/codec 和超长 body 处理。
- Protobuf RPC body，支持二进制 payload 和请求 deadline。
- `request_id` 请求响应匹配和统一 `StatusCode` 错误终态。
- 服务名、方法名注册与分发，缺失服务或方法返回明确响应。

### Reactor 服务端

- Linux `epoll + eventfd` 单 IO 线程。
- accept/read/write 与业务 handler 隔离，handler 始终进入 `ThreadPool`。
- worker 通过跨线程队列提交响应，由 `eventfd` 唤醒 Reactor。
- `ConnectionId + generation` 防止 fd 复用后的迟到发送或关闭误操作。
- partial send、写队列和连接关闭时的 write completion 终态。
- Running -> Draining -> Stopped 优雅停机，支持 grace period 和硬停机。

### 资源边界与背压

- 最大连接数；超限连接在 accept 后立即关闭，已有连接释放后恢复容量。
- 单连接读缓冲和写缓冲硬上限。
- 写缓冲高低水位背压，只暂停对应慢连接的读取。
- 跨线程响应队列和业务线程池队列有界，队列满时明确拒绝。
- 协议 body 大小上限，避免由帧长度触发无界内存增长。

### 客户端

- TCP 长连接复用。
- 同步 `Call()` 与返回 `std::future` 的 `CallAsync()`。
- 单连接多并发请求和 `request_id -> promise` 匹配。
- 基于 `steady_clock` 的本地超时清理和线上 deadline 传递。
- 连接关闭主动失败全部 pending request。
- 有界重连次数和固定 endpoint 的多连接 `RpcClientPool`。

### 可观测性与验证

- 请求、响应、成功、失败、超时、拒绝和 pending 指标。
- active connection、背压连接、最大写缓冲和线程池队列指标。
- 平均延迟及最近 10000 个样本的 P50/P95/P99。
- 服务状态、停机开始时间和 grace period 超时指标。
- Linux Debug/Release 测试、benchmark，以及独立 sanitizer 验证流程。

## 3. 核心不变量

后续维护必须保持：

1. Reactor 线程只负责连接生命周期、协议收发和跨线程队列消费。
2. 业务 handler 不在 IO 线程执行。
3. 线程池和响应队列满时必须产生明确终态，不能无限堆积。
4. 跨线程发送和关闭必须校验 `ConnectionId + generation`。
5. 客户端 pending 请求必须由响应、超时或连接关闭完成，不能无限等待。
6. 服务端 pending 请求持续到响应整帧写成功或明确失败。
7. `Stop(grace_period)` 先停止准入，再等待已接收请求，超时后硬停机。
8. 非法协议、慢连接和大响应不能拖垮其他正常连接。

## 4. 协程实验路径

仓库保留：

```text
Coroutine
  -> Scheduler / TimerQueue
  -> CoroutineIoContext
  -> epoll + eventfd
  -> CoroutineRpcConnection
  -> CoroutineRpcServer
```

它与 Reactor 共享协议、业务线程池和指标语义，但使用独立服务端入口。当前已经覆盖
稳定 handle、幂等调度、结束回收、Stop 取消、重启、guard page、ABI 状态保存和
sanitizer fiber hooks。

这条路径继续标记为 experimental。当前不增加连接空闲/读写超时配置、多 IO 线程、
work stealing、syscall hook 或协程客户端。具体边界见
[docs/coroutine_runtime_minimal_plan.md](docs/coroutine_runtime_minimal_plan.md)。

## 5. 明确不包含

- io_uring 后端。
- 服务发现、负载均衡和动态注册中心。
- 独立的通用 Serializer 插件层。
- 心跳、熔断、限流、认证、TLS、HTTP gateway 和分布式 tracing。
- Protobuf IDL 到客户端 stub/服务端 skeleton 的代码生成。
- 多 Reactor、跨机器容量或生产 SLA 承诺。

这些能力不是“假装已完成的占位组件”。出现明确业务需求前，不在仓库中保留未接入
实现；需要扩展时再围绕公共 API、失败语义和集成测试单独设计。

## 6. 验证基线

所有 Linux 行为在仓库指定容器中验证：

```sh
docker start minirpc-linux
docker exec minirpc-linux bash -c \
  "cd /work && cmake --build build -j && ctest --test-dir build --output-on-failure"
```

关键回归场景包括：

- 半包、粘包、协议错误和超长 body。
- 并发请求匹配、deadline、断线、重连和 pending 清理。
- 最大连接数拒绝、连接 churn 和 fd generation 复用。
- 慢连接背压、大响应 partial send 和写缓冲硬上限。
- 线程池拒绝、优雅停机、在途响应写完成和 hard stop。
- Reactor 重启、并发 Stop 和 callback 内停止。

性能数据只使用固定、可复现的同进程闭环场景，不解释为跨机器容量。方法和结果分别见
[docs/performance.md](docs/performance.md) 与
[docs/benchmark_report.md](docs/benchmark_report.md)。

## 7. 收口标准

项目完成以以下条件为准：

- Reactor 主链路的 Debug/Release 全量测试稳定通过。
- ASan/UBSan 全量测试和关键并发路径的 TSan 测试通过。
- 固定 benchmark 记录环境、参数、延迟分位数、失败数和状态码分布。
- README、架构、协议和实现边界一致，不声明未接入组件。
- 协程始终作为可选实验路径，删除它不影响 `RpcServer` / `RpcClient` 主链路。

达到这些条件后优先准备设计说明和面试表达，不继续通过增加模块数量制造表面复杂度。
