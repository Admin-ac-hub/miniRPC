# miniRPC 协程设计

miniRPC 的协程能力放在 `runtime` 层，目标是把非阻塞 IO 封装成同步风格的控制流。现有 `RpcServer` / `RpcClient` / 协议帧 API 不因为引入协程而改变。

## 目标

当前服务端模型是 reactor 线程负责 accept/read/write，完整请求投递到 `ThreadPool` 执行业务 handler。这个模型简单可靠，但连接处理逻辑会自然演进成事件状态机。协程层要解决的是：

- IO 等待时挂起当前执行流，而不是阻塞 IO 线程。
- epoll 事件到来后恢复等待该 fd 的协程。
- 让连接处理代码可以逐步写成 `ReadFrame -> Dispatch -> WriteFrame` 的顺序流程。

协程不替代协议与 body codec、服务注册层，也不让业务 handler 直接跑在 reactor 线程里。耗时业务仍然必须经过 `ThreadPool` 或后续明确的业务调度器。

## 分阶段接入

### 1. 用户态上下文切换

先实现独立的 `Coroutine`：

```text
main context
  -> Coroutine::Resume()
  -> coroutine stack
  -> Coroutine::Yield()
  -> main context
```

这一阶段只验证栈切换、`Resume/Yield` 顺序、完成状态和析构安全，不接入 epoll。

### 2. 协程调度器

在 `runtime` 层增加 ready queue：

```text
Scheduler::Spawn(fn)
Scheduler::Run()
Scheduler::YieldCurrent()
Scheduler::SuspendCurrent()
```

调度器拥有协程对象，只把可运行协程放进 ready queue。`YieldCurrent()` 表示当前协程主动让出并重新排队；`SuspendCurrent()` 表示当前协程等待外部事件，不自动回到 ready queue。这样 Timer 和 epoll 后续都可以通过 `Scheduler::Schedule(coroutine)` 恢复挂起协程。

### 3. TimerQueue

增加按 deadline 排序的 timer queue，让协程支持睡眠和后续 RPC deadline：

```text
TimerQueue::SleepFor(delay)
  -> 记录当前 coroutine 的 deadline
  -> Scheduler::SuspendCurrent()

TimerQueue::DrainExpired(now)
  -> 把到期 coroutine 交回 Scheduler::Schedule()
```

`TimerQueue` 不自己开线程，后续会由 event loop 在每轮 epoll 前计算 `TimeUntilNext()`，并在 epoll 返回后调用 `DrainExpired()`。这样 timer 超时恢复和 fd 可读写恢复都走同一个 ready queue。

### 4. FdEvent 与 epoll

非阻塞读写遇到 `EAGAIN` 时：

```text
CoroutineIoContext::Read / WriteAll
  -> syscall returns EAGAIN
  -> 记录 fd 等待的 coroutine
  -> 注册 EPOLLIN / EPOLLOUT
  -> Scheduler::SuspendCurrent()
```

epoll 返回后：

```text
CoroutineIoContext::Run
  -> epoll_wait
  -> 找到等待该 fd 的 coroutine
  -> Scheduler::Schedule(coroutine)
  -> Resume 后继续读写
```

第一版已经使用显式 API：`CoroutineIoContext::Read()` 和 `CoroutineIoContext::WriteAll()`。暂不 hook `read/write/connect/sleep`，避免系统调用替换影响现有代码路径。

### 5. 协程化协议帧读写

在字节级协程 IO 之上增加 `CoroutineFrameChannel`：

```text
CoroutineFrameChannel::ReadFrame(fd)
  -> CoroutineIoContext::Read()
  -> RpcCodec::TryDecode()
  -> 处理半包 / 粘包 / 协议错误

CoroutineFrameChannel::WriteFrame(fd, frame)
  -> RpcCodec::Encode()
  -> CoroutineIoContext::WriteAll()
```

这一层只处理 `ProtocolFrame`，不关心 `RpcRequest` / `RpcResponse` 的业务语义。它是后续连接协程循环的直接 building block。

### 6. 单连接 RPC 协程循环

在协议帧读写之上增加 `CoroutineRpcConnection`：

```text
CoroutineRpcConnection::Serve(fd)
  -> CoroutineFrameChannel::ReadFrame()
  -> DecodeRequestBody()
  -> ServiceRegistry::Find()
  -> ThreadPool::Post(handler)
  -> Scheduler::SuspendCurrent()
  -> worker finishes and CoroutineIoContext::Schedule(coroutine)
  -> EncodeResponseBody()
  -> CoroutineFrameChannel::WriteFrame()
```

这一层已经跑通单连接 RPC 闭环，并支持可选 `ThreadPool` 分发。跨线程恢复不直接修改 scheduler ready queue，而是写入 `CoroutineIoContext` 的 control queue 并通过 `eventfd` 唤醒 epoll，IO 线程醒来后再统一调度协程。未传入 `ThreadPool` 时保留同步执行模式，主要用于小范围单元测试。

### 7. 协程版服务端入口

`CoroutineRpcServer` 提供可选服务端入口，不改变现有 `RpcServer` API：

```text
CoroutineRpcServer::Start(endpoint)
  -> setup nonblocking listen fd
  -> spawn accept coroutine
  -> CoroutineIoContext::Run() on IO thread

accept coroutine
  -> WaitReadable(listen_fd)
  -> accept4(SOCK_NONBLOCK)
  -> spawn CoroutineRpcConnection::Serve(client_fd)
```

每个连接协程使用同步风格 `ReadFrame -> Dispatch -> WriteFrame`，但业务 handler 仍通过 `ThreadPool` 执行。现有 reactor + callback 路径保留为稳定基线。

## 当前实现边界

当前已经落地：

- `Coroutine::Resume()`
- `Coroutine::Yield()`
- `Coroutine::Finished()`
- `Scheduler::Spawn()`
- `Scheduler::Run()`
- `Scheduler::YieldCurrent()`
- `Scheduler::SuspendCurrent()`
- `TimerQueue::SleepFor()`
- `TimerQueue::DrainExpired()`
- `CoroutineIoContext::Run()`
- `CoroutineIoContext::Schedule()`
- `CoroutineIoContext::Post()`
- `CoroutineIoContext::Read()`
- `CoroutineIoContext::WriteAll()`
- `CoroutineFrameChannel::ReadFrame()`
- `CoroutineFrameChannel::WriteFrame()`
- `CoroutineRpcConnection::Serve()`
- `CoroutineRpcServer::Start()`
- `CoroutineRpcServer::Stop()`
- Stop 对 fd waiter 和 timer waiter 的主动取消
- `CoroutineRpcServer` 的 Draining 准入封口和 Start -> Stop -> Start
- x86-64 System V ABI 和 aarch64 ABI 下的基础通用寄存器保存与恢复

当前活动范围和验收条件见
[coroutine_runtime_minimal_plan.md](coroutine_runtime_minimal_plan.md)。协程服务端保持实验路径，
不作为默认实现，也不继续扩展为通用协程框架。
