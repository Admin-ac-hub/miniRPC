# Coroutine Runtime 最小完成计划

> 状态：已于 2026-08-18 按本计划完成收口。Coroutine 路径继续保持 experimental。

## 1. 定位

本计划面向实习项目收口，不把 miniRPC 扩展成通用协程框架。目标是保留一条
可演示、可复测、能解释设计取舍的实验性协程服务端：

```text
Reactor（默认）：epoll + eventfd + ThreadPool
Coroutine（实验）：epoll + eventfd + Coroutine + ThreadPool
```

协程路径用于展示如何把非阻塞连接状态机写成顺序控制流，不以替换 Reactor、
追求更高 QPS 或达到生产级资源治理为目标。`RpcServer`、`RpcClient`、协议类型和
`Status` 等现有公共 API 不变。

## 2. 当前基线

以下工作已经完成，本计划不重复设计：

- 协程使用稳定 handle，重复调度幂等，结束后可以回收。
- fd/timer waiter 可以在 Stop 时取消，跨线程完成通过 `eventfd` 回到 IO 线程。
- fd 注销和关闭收口到 IO 线程。
- `CoroutineRpcServer` 支持 Running -> Draining -> Stopped、并发 Stop 和重启。
- handler 始终进入 `ThreadPool`，队列满和大响应失败有明确终态。
- 协程栈使用 guard page，并保存 x86-64/aarch64 所需的浮点控制状态。
- 已接入 ASan fiber hooks，并完成 Debug、Release、ASan/UBSan、选定 TSan 测试。
- 慢 handler、慢写、fd 复用、Stop 竞态和 1000 次连接 churn 已有回归覆盖。

这意味着当前主要风险已经从“运行时生命周期是否正确”转为“如何控制项目边界并
提供可复现证据”。

## 3. 剩余工作

### Phase 1：冻结正确性基线

不再增加调度器抽象，只保留现有回归集合。后续改动必须通过：

```sh
docker exec minirpc-linux bash -c \
  "cd /work && cmake --build build -j && ctest --test-dir build --output-on-failure"
```

收口前再确认以下场景仍有自动化覆盖：

- Start -> Stop -> Start 和并发 Stop。
- 空闲连接、慢 handler、慢写和 peer close 下 Stop 能返回。
- 1000 次连接 churn 后连接、协程和 waiter 回到基线。
- worker completion 与 suspend 竞争不会重复恢复协程。
- 大响应写回失败和线程池拒绝不会遗留 pending request。

若测试稳定通过，不为“代码看起来更完整”继续重构运行时。

### Phase 2：只跑一组固定对比压测

使用现有 `rpc_bench`，在 Linux Release 构建中固定一组连接压力场景，各路径运行
5 轮：

```sh
./build-release/rpc_bench --server both --connections 32 \
  --requests 100000 --runs 5 --payload-size 16 \
  --handler-delay-ms 0 --timeout-ms 3000 --port 19920
```

报告只记录当前工具已经提供的数据：QPS、P50/P95/P99、失败数、拒绝数、超时数、
状态码分布和进程级 CPU 时间。不为了本计划新增 profiler、RSS 采样器或 benchmark
模式。

结论必须说明：

- 同进程闭环结果只能比较当前两条实现，不能代表线上容量。
- Coroutine 的主要收益是顺序化连接控制流，不保证吞吐或尾延迟更优。
- Reactor 继续作为默认实现，无论单次压测哪条路径更快。

### Phase 3：文档收口

同步 `README.md`、`docs/coroutine.md`、`docs/architecture.md` 和 benchmark 文档，明确：

- Coroutine 路径是可选且实验性的，Reactor 是稳定默认路径。
- 只有一个 IO 线程，每个连接内请求顺序处理，业务 handler 使用线程池。
- 当前没有连接空闲超时、通用读写 deadline 和最大连接数配置。
- 当前不承诺多 IO 线程扩展、协程客户端或透明 syscall hook。
- 面试和简历只陈述实际测试数据，不把协程描述为必然的性能优化。

## 4. 明确不做

- 不实现 io_uring。
- 不实现通用 exactly-once wait operation 框架。
- 不增加独立的读超时、写超时或 `CoroutineRpcServerOptions`。
- 不增加最大连接数配置、额外 coroutine/timer 指标或 stack cache。
- 不做多 IO 线程、work stealing、syscall hook 或协程客户端。
- 不引入新的第三方依赖。
- 不继续做性能优化，除非固定压测暴露可复现的正确性问题。

连接空闲超时也不作为当前必做项。只有在实际演示或压测中确认它阻碍验收时，才增加
一个简单的 connection idle timeout；不顺带扩展成通用 deadline 系统。

## 5. 完成定义

满足以下条件即可结束 coroutine runtime 专项，不再追加功能：

- Linux Debug 和 Release 全量测试通过。
- ASan/UBSan 全量测试及选定 TSan 并发测试通过。
- 重启、慢 handler、慢写、Stop 竞态和 1000 次连接 churn 回归保持通过。
- 固定 Reactor/Coroutine 压测完成 5 轮，无请求丢失或无限等待，并记录结果。
- 文档写清单一 IO 线程、每连接顺序处理、实验状态及缺少的资源限制。

达到上述条件后，`CoroutineRpcServer` 仍保持 experimental。这表示项目已完成一条
边界清晰的协程实验路径，而不是宣称完成生产级通用协程运行时。
