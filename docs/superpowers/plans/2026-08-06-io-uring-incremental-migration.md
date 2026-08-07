# io_uring Incremental Migration Plan

**Goal:** 在不修改 `RpcServer`、`RpcClient`、`ProtocolFrame`、`RpcRequest`、
`RpcResponse` 和 `Status` 公共契约的前提下，为服务端增加 io_uring IO 后端；迁移期间完整保留
epoll 后端，默认仍使用 epoll，按阶段验证后再单独决定是否切换默认值。

**Strategy:** 先把现有 `TcpServer` 拆成稳定 facade 和 epoll backend，确认行为完全不变；再以
独立文件增加 io_uring backend，通过 CMake 构建期开关做 A/B。普通 Reactor 路径稳定后，才单独
迁移 `CoroutineIoContext`。不在同一个提交中同时修改两条 IO 路径。

**Tech Stack:** C++17、Linux socket、epoll、eventfd、liburing、CMake 3.16、ctest。

---

## Execution Status (2026-08-07)

Phase 0-7 的 Reactor backend、构建选择、共享测试和 sanitizer 验证已经落地；Phase 8 已完成同环境
epoll/io_uring 五轮 A/B 并触发性能回退 gate，因此默认值保持 epoll。以下项目有意保持未勾选：

- benchmark 尚无专用慢读和 connect/disconnect churn 模式；正确性由自动化测试覆盖。
- server-only CPU、context switches、RSS 和 cycles/instructions 需要外部分进程与 `perf`，当前同进程
  harness 不据此作结论。
- 小 accepted-socket `SO_SNDBUF` 和 cancel/completion 故障注入仍可进一步提高确定性；现有大包、
  背压、hard limit、close-after-send、RST、callback close、fd churn 和 sanitizer 测试覆盖正常系统路径。
- Reactor `tcp_server_test` 与 TCP integration 已完成 io_uring Release 100 轮；全仓重复验证另外发现
  独立 epoll 协程路径的 `coroutine_rpc_server_test` 在第 38 轮挂起，作为单独稳定性任务保留。
- Phase 9-10 是后续独立决策；`CoroutineIoContext` 继续使用 epoll。

收尾验证还修复了 graceful shutdown 的响应写入竞态：原实现会在 response 仅入队时减少
`pending_requests`，随后 `Stop()` 可能取消尚未发送完的响应。现在两个 backend 都通过私有整帧写完成
callback 结束请求，连接关闭、取消、队列清理和 hard stop 也会以失败状态完成 callback。新增的确定性
回归用 `SO_RCVBUF=4096` 的慢读连接阻塞 8 MiB 响应，验证 pending 在整帧进入内核 send 路径前保持为
1。包含该回归的 Release `rpc_client_test` 已在 io_uring 和 epoll 下分别完成 100/100 轮。

第一次 epoll 重复运行曾在约第 64 轮、全部 server/socket fd 已关闭后停在 `RpcClient` timeout-cleaner
与 join 链；重新运行完成 100/100。它与既有 `TcpClient::Close()`/reader TSan 告警一起保留为客户端
生命周期问题，不归因于服务端 backend。`RpcServer::Stop()` 从业务 handler 内调用以及 draining 期间
并发 `Start()` 当前也不属于支持的生命周期用法，需另立任务设计串行化语义。

抽取后验证发现旧 epoll 会在解码前按一次 `recv` drain 的聚合大小误杀完全合法的粘包 burst。当前两个
backend 的 `max_read_buffer_bytes` 都限制“尚不能组成完整 frame 的残留数据”，完整 frame 会先解码并
移出 buffer。这是显式接受的独立 bug fix，不再把它描述成纯机械迁移的一部分。

---

## 1. Hard Constraints

- [x] epoll 源码和测试在整个迁移过程中保留；本计划不包含删除 epoll。
- [x] 完整 uring backend 通过生命周期测试后才开放 `MINIRPC_TCP_SERVER_BACKEND`；其默认值为
      `epoll`，可选值仅为 `epoll`、`io_uring`。
- [x] 后端选择发生在 CMake 配置期，不增加 `RpcServer` 或 `TcpServer` 公共参数。
- [x] 选择 `io_uring` 后，如果内核、seccomp 或 opcode 不满足要求，`Start()` 返回明确的
      `StatusCode::kNetworkError`；禁止静默回退 epoll。
- [x] 只有 IO 线程可以获取 SQE、submit 和消费 CQE。业务 worker 继续只写 response/close queue，
      再通过 eventfd 唤醒 IO 线程。
- [x] 每个异步操作使用唯一 operation token；CQE 身份必须校验
      `operation token + ConnectionId + generation`，不能只校验 fd。
- [x] handler 始终通过 `ThreadPool`，且 `ThreadPool::Post()` 失败路径保持完整。
- [x] 线协议、枚举取值、20 字节协议头和 codec 均不修改。
- [x] 所有构建与测试只在 `minirpc-linux` Docker 容器执行，不在 macOS 验证 Linux IO 行为。
- [x] 引入 `liburing` 前需得到用户确认；缺包时停止并询问，不自行重建容器。

---

## 2. Target Architecture

```text
RpcServer
  -> TcpServer facade                         public API unchanged
       -> EpollTcpServerBackend               retained and initially default
       -> IoUringTcpServerBackend             opt-in during migration

worker thread
  -> response_queue / close_queue
  -> eventfd
  -> selected IO backend thread

io_uring backend
  SQ: one accept + one wake poll + <= one recv/connection + <= one send/connection
  CQ: operation token -> conn_id/generation validation -> state transition
```

构建目录建议固定分开，避免污染现有 Linux build：

```sh
# 保留当前默认 epoll build
cmake -S . -B build-epoll \
  -DCMAKE_BUILD_TYPE=Release \
  -DMINIRPC_TCP_SERVER_BACKEND=epoll

# io_uring opt-in build
cmake -S . -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DMINIRPC_TCP_SERVER_BACKEND=io_uring
```

两个 build 必须来自同一 commit，并分别运行全量 ctest。运行时自动 fallback 不属于本计划。

---

## 3. Planned File Layout

新增文件：

- `include/minirpc/net/tcp_server_backend.h`
  - 私有后端接口和 factory 声明。
  - 不包含 `<liburing.h>`，不增加公共使用方式。
- `src/net/epoll_tcp_server_backend.cpp`
  - 从当前 `src/net/tcp_server.cpp` 原样迁移的 epoll 实现。
- `src/net/io_uring_tcp_server_backend.cpp`
  - io_uring completion state machine。
- `cmake/FindLiburing.cmake`
  - 定义 `Liburing::Liburing` imported target。

修改文件：

- `include/minirpc/net/tcp_server.h`
  - 保留全部 public 方法签名，将 private reactor 状态替换为 PImpl。
- `src/net/tcp_server.cpp`
  - 只保留 facade 委托和构建期 backend factory 选择。
- `CMakeLists.txt`
  - 增加并校验 `MINIRPC_TCP_SERVER_BACKEND`；仅 uring build 查找并链接 liburing。
- `.github/workflows/linux-ci.yml`
  - 安装 `liburing-dev`，增加 epoll/io_uring 构建矩阵。
- `tests/net/tcp_server_test.cpp`
  - 增加连接取消、关闭竞态和背压行为测试；同一测试套件在两个 build 下运行。
- `tests/net/tcp_echo_integration_test.cpp`
  - 增加连接 churn、partial send 和顺序回归。
- `benchmark/rpc_bench.cpp`
  - 输出实际构建的 TCP backend，避免报告标签混淆。
- `README.md`、`docs/architecture.md`、`docs/performance.md`、
  `docs/benchmark_report.md`、`RPC_FRAMEWORK_ROADMAP.md`
  - 在对应阶段同步状态和基准。

历史设计记录 `docs/superpowers/specs/*` 和既有计划不回写；它们描述的是当时已完成的 epoll 设计。

---

## 4. Phase 0: Environment and Baseline Gate

**Purpose:** 在任何代码改动前确认 Linux 环境，并保存可以和 uring 对比的 epoll 基线。

- [x] 启动既有容器；如果 Docker daemon、容器或 `/work` 挂载不可用，停止并询问用户。

```sh
docker start minirpc-linux
docker exec minirpc-linux bash -c \
  "cd /work && uname -r && cmake --build build -j && ctest --test-dir build --output-on-failure"
```

- [x] 检查 io_uring 和 liburing 环境。`pkg-config` 缺失或找不到 liburing 时只记录结果，不在本阶段安装。

```sh
docker exec minirpc-linux bash -c \
  "uname -r; cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || true; pkg-config --modversion liburing"
```

- [x] 记录 commit、kernel、architecture、compiler、CMake build type、liburing 版本。
- [x] 用当前 epoll 实现运行 `docs/performance.md` 的三组固定 Release benchmark，保存原始逐轮输出。
- [x] 记录工作树状态；本任务之外的现有改动必须保留并避开，不得回退。

**Gate:** 原有全量测试必须通过。基线失败时先定位现有问题，不能把失败带入后端抽取。

---

## 5. Phase 1: Extract the Existing epoll Backend

**Purpose:** 只改变代码组织，不改变任何网络行为。完成后默认 build 仍是 epoll。

### Task 1.1: Add the backend facade

- [x] 在 `tcp_server_backend.h` 定义只供实现使用的 `TcpServerBackend` 接口：
  `SetOnOpen`、`SetOnFrame`、`SetOnClose`、`SetOnBackpressure`、`SetOptions`、`Start`、
  `Stop`、`StopAccepting`、`running`、`SendFrame`、`CloseConnection`。
- [x] 声明 `MakeEpollTcpServerBackend()` 和 `MakeIoUringTcpServerBackend()` factory。
- [x] 将 `TcpServer` private state 改成 `struct Impl` + `std::unique_ptr<Impl>`。
- [x] `src/net/tcp_server.cpp` 只负责把所有 public 方法委托给 backend。
- [x] public 方法签名、返回值以及 callback 线程语义保持不变。

### Task 1.2: Move epoll code without redesign

- [x] 将当前 listener、epoll、eventfd、连接表、buffer、response queue 和 close queue 逻辑移动到
      `epoll_tcp_server_backend.cpp`。
- [ ] 只做类型名和成员归属所必需的机械修改；本任务不顺手重构 read/write/backpressure。
- [x] 保留 `generation == 0` 对跨线程 `SendFrame`/`CloseConnection` 的既有通配行为。
- [x] 保留 stale response 入队成功、由 IO 线程 drain 时丢弃的既有语义。
- [x] 保留 `OnOpen`、`OnFrame`、`OnBackpressure`、`OnClose` 均由 reactor thread 触发。

### Task 1.3: Verify the extraction

- [x] Docker Debug/Release 全量 build + ctest。
- [x] 重跑大包、512 小包顺序、慢客户端背压、graceful shutdown。
- [ ] 对比 Phase 0 benchmark，确认抽取本身没有明显性能回退。

**Suggested commit:** `net: extract epoll tcp server backend`

**Rollback:** 该提交可以整体回退；尚未引入 liburing，也未改变默认后端。

---

## 6. Phase 2: Add Experimental liburing Build Support

**Purpose:** 让尚未完成的 uring backend 可以被内部测试单独编译和探测，但不把它暴露成可替换
`TcpServer` 的正式配置。每个中间提交仍保持默认 epoll 全量测试可用。

- [x] 得到用户对 `liburing` 新依赖的明确确认。
- [x] 新增 `FindLiburing.cmake`，使用 `find_path` + `find_library` 创建
      `Liburing::Liburing`，避免额外依赖 pkg-config。
- [ ] 迁移期间先增加实验开关：

```cmake
option(MINIRPC_BUILD_IO_URING_EXPERIMENTAL "Build the experimental io_uring backend" OFF)
```

- [x] 最终 epoll 开关下不查找、不包含、不链接 liburing。
- [x] 由于 `src/*.cpp` 使用 `GLOB_RECURSE`，epoll 配置时从 `MINIRPC_SOURCES` 明确排除
      `io_uring_tcp_server_backend.cpp`；不能仅依赖未引用 symbol 避免编译。
- [ ] 实验开关开启时编译 uring backend 和阶段性专用测试，并 private link
      `Liburing::Liburing`；`TcpServer` facade 仍选择 epoll。
- [ ] 阶段性测试直接通过内部 factory 构造 uring backend；未完成的 backend 不参与原有全量
      `TcpServer` 测试，避免提供一个表面可选但行为残缺的正式配置。
- [x] 确保 `liburing.h` 只出现在 uring `.cpp`，不泄漏到 public header。
- [x] 增加 `IoUringTcpServerBackend` 的 queue init/probe/queue exit 和启动探测；尚未实现的
      网络操作不被 facade 调用。

启动时 probe 以下能力：

- `IORING_OP_ACCEPT`
- `IORING_OP_RECV`
- `IORING_OP_SEND`
- `IORING_OP_POLL_ADD`
- `IORING_OP_ASYNC_CANCEL`
- `IORING_FEAT_NODROP`
- `IORING_FEAT_FAST_POLL`

`io_uring_queue_init_params()` 和 liburing API 返回负 errno，错误文本必须使用 `strerror(-rc)`，不能读取
无关的全局 `errno`。

**Suggested commit:** `build: add experimental liburing backend support`

**Gate:** epoll build 在未安装 liburing 的环境中仍应独立 configure/build/test。

---

## 7. Phase 3: Implement Ring Ownership, Accept, and Wakeup

**Purpose:** 建立最小可运行 CQ loop，只处理 listener 和 worker wakeup。

### Operation ownership

- [x] 每个 SQE 使用单调递增且不复用的 `uint64_t operation_token` 作为 `user_data`。
- [x] token 映射到稳定的 Operation 对象；Operation 至少包含：

```cpp
enum class OpKind { kAccept, kWakePoll, kRecv, kSend, kCancel };

struct Operation {
    uint64_t token = 0;
    OpKind kind = OpKind::kAccept;
    ConnectionId conn_id = 0;
    uint64_t generation = 0;
    uint64_t target_token = 0;
};
```

- [x] SQE 暂时取不到时先 submit 已准备项、消费 CQE、再重试；不能丢操作或无限忙循环。
- [x] 所有 SQ/CQ 操作只发生在 IO thread。

### Accept

- [x] 第一版只使用 one-shot `IORING_OP_ACCEPT`，flags 为
      `SOCK_NONBLOCK | SOCK_CLOEXEC`。
- [x] accept CQE 成功后分配新的 `ConnectionId` 和 generation，触发 `OnOpen`，再提交下一个 accept。
- [x] `cqe->res < 0` 按 `-errno` 分类；瞬时错误可重提，资源耗尽不得形成 tight loop。
- [x] `StopAccepting()` 后若 cancel 竞态仍返回新 fd，直接关闭该 fd，不触发 `OnOpen`。

### Cross-thread wakeup

- [x] 保留现有 eventfd 和 response/close mutex queues。
- [x] 用 one-shot `IORING_OP_POLL_ADD(POLLIN)` 监听 eventfd；CQE 后 drain eventfd、drain queues、再 rearm。
- [x] 不使用 `io_uring_register_eventfd`，它用于 CQ 通知，方向与当前 worker 唤醒需求相反。
- [x] 多次 worker wake 合并时也必须完整 drain 两个队列。

### Tests

- [x] Start/Stop 幂等。
- [x] 端口占用返回 `kNetworkError`。
- [x] 并发连接 accept，`OnOpen` 次数准确。
- [x] eventfd 连续/合并唤醒不会丢 response/close command。
- [x] StopAccepting 与 accept completion 竞态不产生新活跃连接。

**Suggested commit:** `net: add io_uring accept and reactor wakeup`

---

## 8. Phase 4: Implement Receive and Frame Decode

**Purpose:** 打通 client -> server 单向收包，保持当前 codec 和 buffer 限制。

- [x] 每个连接最多一个 recv in flight。
- [x] recv Operation 拥有或引用生命周期稳定的固定 chunk；buffer 在目标 CQE 回收前不得移动或释放。
- [x] CQE 处理顺序固定为：token 查 Operation -> conn_id 查连接 -> generation 校验 -> 当前 recv token 校验。
- [x] `res > 0`：append 到 `read_buffer`，循环 `TryDecode`，只对 `NeedMoreData` 的残留执行 `max_read_buffer_bytes` 限制。
- [x] `res == 0`：peer EOF，进入连接关闭流程。
- [x] `res == -EAGAIN` 或 `-EINTR`：重新提交 recv；其它负值进入关闭流程。
- [x] 每个完整 frame 仍同步触发 `OnFrame`；callback 返回后重新查找连接，不能继续使用可能失效的引用。
- [x] 正常状态下处理完成后 rearm recv；closing 或 backpressure 状态不 rearm。

### Tests

- [x] 半包、粘包、非法 magic、超长 body。
- [x] 1 MB frame 和超过单个 recv chunk 的 frame。
- [x] 512 个小 frame 顺序不变。
- [x] peer EOF、RST 和 callback 请求关闭。
- [x] worker 响应迟到且 generation 已过期时不访问新连接。

**Suggested commit:** `net: receive and decode frames with io_uring`

---

## 9. Phase 5: Implement Ordered Send and Backpressure

**Purpose:** 打通完整 RPC 回包，并保持发送顺序、内存上限和慢连接隔离。

- [x] 每个连接最多一个 send in flight；多个 response 继续进入 FIFO deque。
- [x] send Operation 持有稳定发送数据和 offset，使用 `MSG_NOSIGNAL`。
- [x] `res > 0` 可能是部分发送：推进 offset，再提交剩余部分。
- [x] 非空 send 返回 `res == 0` 视为 IO error。
- [x] `-EAGAIN`/`-EINTR` 可重提；`-EPIPE`/`-ECONNRESET` 等进入关闭流程。
- [x] `write_buffer_bytes` 包含 queued bytes 和已经提交但 CQE 尚未确认的 bytes。
- [x] 每个 serialized response 携带内部 terminal completion；部分发送继续移动同一个 callback，整帧
      成功或 stale generation、peer close、hard limit、cancel/Stop 等失败路径恰好完成一次。
- [x] 超过 `max_write_buffer_bytes` 时关闭该连接，不影响其它连接。
- [x] 达到 high watermark 后进入 backpressure，不再提交新 recv。
- [x] 第一版不因 soft backpressure cancel recv：允许已经在途的 recv 至多再完成一个 chunk，处理后不
      rearm。这样把 cancel 限定在 hard close/stop 路径，并用测试固定最多一个 chunk 的超量边界。
- [x] 降到 low watermark 后退出 backpressure 并重新提交 recv。
- [x] `close_after_send` 等待该连接所有 queued/in-flight writes 清空后关闭。
- [x] `OnBackpressure` enter/leave 配对，连接关闭时最多补一次 leave。

### Tests

- [ ] 小 `SO_SNDBUF` 强制 partial send，验证 frame 顺序和内容。
- [x] 大 payload burst 顺序。
- [x] 慢客户端触发 high watermark，正常客户端仍能及时收发。
- [x] drain 到 low watermark 后恢复读取。
- [x] hard max、response queue full、close_after_send。
- [x] 8 MiB 响应配合 4 KiB client receive buffer 强制慢读背压；handler 返回后 pending 仍为 1，
      `Stop()` 保持 Draining，客户端读出完整 frame 后才返回。

**Suggested commit:** `net: add ordered io_uring sends and backpressure`

---

## 10. Phase 6: Cancellation and Deferred Destruction

**Purpose:** 正确处理 io_uring 最危险的关闭、fd 复用和迟到 CQE。

- [x] `CloseConnection` 先将连接标记为 hard-closing，停止提交新操作。
- [x] 对 accept/recv/send/wake 使用按 `user_data` 的 `IORING_OP_ASYNC_CANCEL`。
- [x] cancel CQE 和目标 operation CQE 分别处理：
  - cancel 成功不代表可以提前释放目标 buffer；
  - `-ENOENT` 表示 cancel 与正常 completion 竞态；
  - 目标操作通常仍会收到自己的 `-ECANCELED` CQE。
- [x] Connection 从可查找 active state 转为 closing/tombstone state，但在所有 in-flight CQE 回收前不析构。
- [x] 最终关闭时才 close fd、erase fd/id maps、释放 buffer，并且 `OnClose` 恰好触发一次。
- [x] CQE 永远不能使用裸 fd 关联连接；fd 被快速复用后，旧 generation 的 CQE 必须被忽略并正常回收。

### Stop order

1. `StopAccepting()` 唤醒 IO thread 并取消 accept。
2. `RpcServer` 在 grace period 内等待 pending request 到响应整帧写完成或 terminal write failure；
   handler 返回和 response queue 入队都不算完成。
3. `TcpServer::Stop()` 标记 hard stop，停止所有 rearm。
4. cancel accept/recv/send/wake，继续 submit/reap，直到所有持有用户 buffer 的 operation 完成。
5. 逐连接 finalize close，由 IO thread 调用 `io_uring_queue_exit()` 并释放 eventfd。
6. IO thread 返回后，外部 `Stop()` / 下一次 `Start()` 回收 joinable thread。

### Tests

- [x] idle accept/recv 在途时 Stop 能及时返回，包括活跃连接数超过 ring depth 的场景。
- [x] send 在途时 Stop 不 UAF、不死锁。
- [ ] cancel 与正常 recv/send completion 各种顺序。
- [x] 快速 connect/close churn 触发 fd 复用，`OnClose` 仍 exactly once。
- [x] worker 完成响应时原连接已经关闭。
- [x] Phase 1 先用 epoll 测试固定同一对象 `Start -> Stop -> Start -> Stop` 的现有行为；uring backend
      必须保持相同行为。

### Activate the build selector

只有上述关闭和重启测试全部通过后，才把实验开关替换为正式 cache string：

```cmake
set(MINIRPC_TCP_SERVER_BACKEND "epoll" CACHE STRING "TCP server IO backend")
set_property(CACHE MINIRPC_TCP_SERVER_BACKEND PROPERTY STRINGS epoll io_uring)
```

- [x] 对未知值在 configure 阶段 `FATAL_ERROR`。
- [x] `epoll` 配置不查找 liburing，并从 source list 排除 uring backend。
- [x] `io_uring` 配置编译 uring backend，同时保留 epoll backend 源码备用，由 facade 选择 uring factory。
- [x] 删除实验开关，避免两个配置入口表达相同含义。
- [x] 此时才让同一套 `TcpServer` 和 RPC 集成测试在两个 backend build 下全量运行。

**Suggested commit:** `net: make io_uring connection shutdown race-safe`

**Gate:** 未通过 ASan/UBSan 和重复生命周期测试前，uring backend 不可用于 benchmark 结论或默认值讨论。

---

## 11. Phase 7: Full Verification and CI Matrix

- [x] `.github/workflows/linux-ci.yml` 安装 `liburing-dev`。
- [x] matrix 至少包含：
  - `MINIRPC_TCP_SERVER_BACKEND=epoll`
  - `MINIRPC_TCP_SERVER_BACKEND=io_uring`
- [x] 两个 job 都构建 examples/tests/benchmarks 并运行全量 ctest。
- [x] uring job 的 queue/opcode probe 失败应明确失败，不能 skip 后显示绿色。
- [x] Docker Debug 和 Release 全量测试。
- [x] ASan/UBSan build；TSan 单独记录已知工具/内核限制和真实告警。
- [x] 包含 8 MiB graceful shutdown 回归的 Release `rpc_client_test` 在 epoll/io_uring 下各完成
      100/100 轮。
- [ ] lifecycle/churn 测试至少执行：

```sh
ctest --test-dir build-uring --output-on-failure --repeat until-fail:100
```

- [ ] 确认没有编译警告、CQ overflow、悬挂 operation、重复 callback 或 pending request 无限等待。

上面两项保留未勾选：全仓 repeat 已分别暴露独立协程路径挂起和一次客户端 lifecycle 停滞；服务端
backend 的定向重复、sanitizer 和 completion 回归已通过，但不能据此声称整个仓库不存在挂起。

**Suggested commit:** `ci: test epoll and io_uring backends`

---

## 12. Phase 8: Benchmark Without Switching the Default

**Purpose:** 在保留 epoll 默认值的情况下形成可信的同环境 A/B 证据。

- [x] `rpc_bench` 输出 `tcp_backend=epoll|io_uring`、commit、kernel、compiler、liburing version。
- [x] 同一容器、同一 commit、相同 CPU 配置分别运行 `build-epoll` 和 `build-uring`。
- [x] 保留现有固定场景：8 connections、64 connections、5ms slow handler。
- [ ] 在当前一连接一客户端线程模型下新增 128/512 connections、64 KiB payload、慢读客户端和
      connect/disconnect churn；在 benchmark driver 改为少量线程管理多连接之前，不使用 2000
      connections 宣称服务端容量。
- [ ] 除 QPS 和 P50/P95/P99 外，记录 server CPU、context switches、cycles/instructions、RSS、失败数、
      cancel/error/CQ overflow。
- [x] 当前 benchmark 是同进程闭环模型，不能据此单独声称 server CPU 优势；需要外部分进程或 perf 证据。
- [x] 所有可靠性计数必须符合预期，非 overload 场景 failure/timeout/reject 为 0。
- [x] 连续多组 5 轮中位数出现超过 5% 的持续 QPS/P99/CPU 回退时，不讨论切换默认值，先 profile。
- [x] 更新 `docs/benchmark_report.md`，旧数据保留为历史，不混用不同环境数字。

**Suggested commit:** `bench: compare epoll and io_uring tcp backends`

默认值仍为 epoll。切换默认值是独立决策，需要用户确认和单独提交。

---

## 13. Phase 9: Migrate Coroutine Readiness Separately

`CoroutineIoContext` 是第二套独立 epoll，不能和 TcpServer backend 同时改。普通 Reactor uring 路径通过所有 gate 后，
再开始本阶段。

- [ ] 增加独立构建选项：

```cmake
set(MINIRPC_COROUTINE_IO_BACKEND "epoll" CACHE STRING "Coroutine IO backend")
set_property(CACHE MINIRPC_COROUTINE_IO_BACKEND PROPERTY STRINGS epoll io_uring)
```

- [ ] 默认仍为 epoll，保留当前 `CoroutineIoContext` 行为作为回退路径。
- [ ] 先抽取 readiness poller，不改变 `Spawn`、`Schedule`、`Run`、`Stop`、`WaitReadable`、
      `WaitWritable`、`Read`、`WriteAll` 公共签名。
- [ ] epoll poller 只是机械迁移，先跑现有 `coroutine_io_context_test`。
- [ ] io_uring poller 使用 one-shot `IORING_OP_POLL_ADD`；每个 poll token 包含 fd、wait kind 和 waiter generation。
- [ ] waiter 改变时 cancel 旧 poll；旧 CQE 只能恢复匹配 generation 的 coroutine。
- [ ] TimerQueue 继续和 IO 共用 event loop，使用有界 timeout wait 或 timeout SQE；timer 提前/取消必须有测试。
- [ ] 外部 worker 恢复协程继续通过 pending-ready queue + eventfd，不直接跨线程修改 scheduler ready queue。
- [ ] `AcceptLoop` 第一版继续 `WaitReadable + accept4`，不同时引入 direct async accept。

### Required regressions

- [ ] delayed read suspend/resume。
- [ ] write backpressure suspend/resume。
- [ ] timer 与 IO 同 loop。
- [ ] 同 fd 同时存在 read/write waiter。
- [ ] Stop/cancel 后 waiter 全部清理，不重复 resume。
- [ ] graceful shutdown 和线程池完成后跨线程 Schedule。

**Suggested commits:**

- `runtime: extract coroutine epoll poller`
- `runtime: add io_uring coroutine poller`
- `ci: test coroutine epoll and io_uring backends`

这一阶段完成后项目可以做到“不依赖 epoll 的构建”，但 `Read/WriteAll` 仍可能是 syscall + readiness wait。

---

## 14. Phase 10: Optional Completion-based Coroutine IO

本阶段是 io_uring 深化，不是替换 epoll 的必要条件。只有 Phase 9 稳定并有 profiling 证据时执行。

- [ ] `Read` 直接提交 `IORING_OP_RECV`，挂起当前协程，CQE 写入 result 后恢复。
- [ ] `WriteAll` 直接提交串行 `IORING_OP_SEND`，处理 partial completion。
- [ ] 协程栈上的 caller buffer 在挂起期间保持有效，但 Stop/close 必须先完成或取消 operation，再销毁协程。
- [ ] 为 fd 建立 epoch/registration 生命周期，避免 close 后 fd 复用导致旧 CQE 恢复新连接。
- [ ] `CoroutineRpcServer` 关闭 fd 前通知 IO context cancel/forget，不能从其它线程直接 close 后遗留 operation。
- [ ] 业务 handler 仍通过 `ThreadPool`，不得因为 completion model 回到 IO thread 执行慢 handler。

multishot accept/recv、provided buffer ring、fixed files、zero-copy send 和 `SQPOLL` 仍不在此阶段默认启用；每项应有
独立 benchmark、内核门槛和生命周期设计。

---

## 15. Final Acceptance Criteria

在讨论把任一路径默认切到 io_uring 前，必须同时满足：

1. `RpcServer`、`RpcClient`、`TcpServer`、协议和 `Status` 公共 API 无变更。
2. epoll 和 io_uring 分别可独立 configure/build/test，默认仍可随时切回 epoll build。
3. 两套 backend 的现有行为测试和新增竞态测试全部通过。
4. 非故意 overload 场景没有失败、超时、拒绝、CQ overflow、重复 callback 或连接泄漏。
5. Stop/StopAccepting/graceful shutdown 在 accept/recv/send/worker 全部可能在途时可终止。
6. ASan/UBSan 和重复 churn/lifecycle 测试无错误。
7. benchmark 记录完整环境和原始数据，不以单轮结果或不同环境数据得出结论。
8. 文档明确当前默认 backend、备用 backend、最低 kernel/liburing 以及容器限制。
9. 切换默认值经过用户确认，并作为独立、可回退的提交完成。

---

## 16. Explicitly Out of Scope

- 删除 epoll backend。
- 修改 wire protocol 或公共 RPC 消息类型。
- io_uring 化 `TcpClient`；它当前不使用 epoll，应另立任务评估。
- 第一版启用 multishot、registered/fixed buffers、fixed files、zero-copy 或 `SQPOLL`。
- 因 io_uring 不可用而运行时静默 fallback。
- 在 macOS 上模拟或验证 Linux io_uring 行为。
