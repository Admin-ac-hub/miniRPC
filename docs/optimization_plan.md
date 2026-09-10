# miniRPC 下一步优化计划

> 按优先级排。每项都遵守 `AGENTS.md`：不引入新依赖、先补能复现的失败用例再改实现、
> 涉及 epoll 的测试在 docker 容器 `minirpc-linux` 里跑、路径镜像、CMake 显式 `add_test`。
>
> ⚠️ **需要先和你确认的公共 API 变更**：P0 要给 `TcpServerOptions` 加字段。
> 按 `AGENTS.md` §1，公共 API 调整需先确认再动手。默认值为「禁用」可保证向后兼容。

---

## P0 — 连接空闲超时（先做这个）

### 1. 问题

目前一条连接只要**不触发读缓冲上限、对端也不关闭**，就可以永久存在：

| 场景 | 现有防线 | 结果 |
| --- | --- | --- |
| 发半个包（< 2MB）后装死 | `max_read_buffer_bytes` 不触发 | ❌ **连接永久挂着** |
| 连上后什么都不发 | 无 | ❌ 永久占 fd + 连接计数 |
| 对端进程被 `kill -9`（无 FIN） | 无（TCP keepalive 未开） | ❌ 半开连接永久残留 |

后果：fd、内存、`active_connections` 计数、`max_connections` 配额**都被慢速/半开连接慢慢吃光**。
README「当前限制」已承认：「稳定 Reactor 路径尚未提供连接空闲超时」。

### 2. 目标

- 可配置的**每连接空闲超时**：一段时间无成功读写 → 主动关闭。
- 默认**关闭**（`0` = 禁用），保证现有 14 个测试行为完全不变。
- 与 `max_connections` 配合：超时回收后**立即释放连接配额**。

### 3. 方案选型

| 方案 | 复杂度 | 精度 | 结论 |
| --- | --- | --- | --- |
| A. 每轮 `epoll_wait` 后 O(n) 全扫 | 低 | ≤1000ms | 简单但 1 万连接每秒扫 1 万次，且精度受限于 1s |
| B. **最小堆 + 惰性删除 + 动态 epoll 超时** | 中 | 毫秒 | ✅ **推荐** |
| C. 复用 `runtime/timer_queue.h` | 中 | 毫秒 | TimerQueue 强绑 `CoroutineHandle`，泛化要动 runtime 层，**先不动** |
| D. 时间轮 | 高 | 毫秒 | 连接数不大时用不上 |

**选 B**，理由：与 `TimerQueue` 内部实现同构（最小堆 + 惰性删除），
且能复用现有 `epoll_wait` 的 timeout 参数做到精确唤醒，**不引入新线程、不引入定时器 fd**。

### 4. 详细设计

#### 4.1 数据结构

```cpp
// 新增（或放进 epoll backend 的内部结构）
struct IdleTimer {
    std::chrono::steady_clock::time_point deadline;   // 单调钟，不受系统时间调整影响
    ConnectionId conn_id;
    uint64_t generation;      // 与连接 generation 对齐，防 id 复用误关
    uint64_t activity_seq;    // 活性版本号：Touch 后旧定时器自动失效
};

std::priority_queue<IdleTimer, std::vector<IdleTimer>, LaterDeadline> idle_timers_;
```

三个字段的作用：
- `generation`：连接被关、id 被复用后，**旧定时器不能关掉新连接**（与 `ConnectionId + generation` 同源）。
- `activity_seq`：解决「连接又活跃了，但堆里还留着旧的 deadline」——**惰性删除**，
  pop 出来时若 `seq` 不匹配就直接丢弃，不做堆内删除（`priority_queue` 不支持）。
- 时间源用 **`steady_clock`**：墙上钟被 NTP 调整会导致误杀或永不超时。

#### 4.2 新增选项（⚠️ 公共 API 变更，需确认）

```cpp
struct TcpServerOptions {
    // ... 现有 7 项不变 ...
    std::chrono::milliseconds idle_timeout_ms{0};   // 新增；0 = 禁用（保持现有行为）
};
```

`RpcServer` 暂不暴露，只在 `TcpServer` 层提供；需要时再往上透传。

#### 4.3 改动点

| # | 位置 | 改动 |
| --- | --- | --- |
| 1 | `Connection` 结构 | 加 `last_activity`（time_point）+ `activity_seq`（uint64_t） |
| 2 | 新增 `TouchConnection(Connection&)` | 更新 `last_activity`、`++activity_seq`、push 新定时器 |
| 3 | `AcceptConnections()` | accept 成功后 `Touch` |
| 4 | `HandleClientRead()` | 成功 `recv > 0` 后 `Touch` |
| 5 | `SendToConnection()` / `FlushWriteBuffer()` | 成功写出字节后 `Touch` |
| 6 | `RunEventLoop()` | `epoll_wait` 超时改为 `NextIdleTimeoutMs()`（`min(1000ms, 最近到期)`，无定时器则 1000ms） |
| 7 | 新增 `DrainIdleTimers()` | 每轮 epoll_wait 返回后调用：pop 所有到期项，校验 `generation`/`activity_seq`，真到期才 `CloseById` |
| 8 | `CloseById()` | 无需主动清堆（惰性：pop 时 id 不存在就跳过） |
| 9 | `CleanupAfterJoin()` | `idle_timers_` 清空 |

**关键：`DrainIdleTimers` 必须在 `DrainResponses()` 之后**，否则可能关掉一个刚被写入响应的连接。

#### 4.4 一个需要决策的细节

**背压中的连接（`in_backpressure == true`）要不要计时？**

- 方案 A：照常计时 → 慢客户端会被超时踢掉，但也**顺带解决了"慢客户端永久占着连接"**的问题。
- 方案 B：暂停计时 → 更"仁慈"，但慢连接可以无限期存在。

**建议选 A**（照常计时），因为背压中我们**没有读它**，它确实在浪费资源；
且写缓冲还在排空时 `FlushWriteBuffer` 会 `Touch`，只有**真的卡死不动**才会超时。

### 5. 测试用例（先写失败用例再改实现）

新文件：`tests/net/tcp_idle_timeout_test.cpp`，CMake 里 `add_executable` + `add_test`。

| 用例 | 断言 |
| --- | --- |
| `IdleConnectionClosed` | 服务端 `idle_timeout_ms=200`，客户端连上后不发数据 → 约 200ms 内 `recv` 返回 0 |
| `ActiveConnectionSurvives` | 客户端每 50ms 发一个合法请求，持续 1s（> 2× 超时）→ 连接存活、全部成功 |
| **HalfPacketConnectionClosed** | 客户端只发 500 字节不完整帧后装死 → 被超时关闭（**补上现有漏洞**） |
| `DisabledByDefault` | `idle_timeout_ms=0`（默认）→ 静置 1s 连接仍存活（**向后兼容回归**） |
| `CapacityReleasedAfterIdleClose` | `max_connections=2`，两条连接空闲超时被关 → 第三条连接可以成功建立 |
| `GenerationMismatchIgnored` | 连接关闭、id 复用后，旧定时器不会误关新连接 |

### 6. 执行顺序（每步可编译可测）

```
1️⃣ 加选项 + Connection 字段 + Touch()，默认 0 禁用  →  行为零变化，全量 ctest 必须仍 14/14
2️⃣ 加定时器堆 + DrainIdleTimers + 动态 epoll 超时
3️⃣ 补 6 个测试用例（先跑出失败，再改实现）
4️⃣ docker：Debug + Release + ASan/UBSan 全量 + 新测试连跑 10 次
5️⃣ 文档同步：docs/architecture.md、README「当前限制」删掉这条
```

### 7. 风险与回归点

- **默认禁用** → 现有 14 个测试行为完全不变，风险最低。
- `epoll_wait` 超时从固定 1000ms 变动态 → 需确认 `accepting_` 状态检查（`:347`）仍能被及时响应（取 `min(1000, ...)` 保证最坏仍 1s）。
- `OnCloseFn` 签名 `void(ConnectionId, uint64_t)` **不带关闭原因**，
  无法区分「对端关闭」和「空闲超时」。**不改签名**（公共 API），仅用 `Logger` 打日志区分。
  若确实需要 reason → 属 API 变更，单独提。

### 8. 验收标准

- [x] 新增测试全绿（文档 6 用例，`GenerationMismatchIgnored` 拆为 stale-timer 续期与残留定时器两个场景，共 7 个用例函数）
- [x] 全量 ctest Debug + Release 各通过（15/15）
- [x] ASan/UBSan 通过（15/15）
- [x] Release 下新测试连续 10 次无 flaky（`steady_clock` 精度 + 200ms 阈值要留足余量）
- [x] README / architecture.md 同步更新

> 实现备注：Touch 不再每次都 push 新堆项（文档 4.1 原设计在高流量 +
> 长 idle_timeout 下堆会随 Touch 次数无限膨胀），改为每连接至多一个 pending 项，
> 到期时若 `activity_seq` 过期则按最新 `last_activity` 重推。堆大小上界为连接数，
> 方案 B 的最小堆 / 惰性删除 / 动态 epoll 超时三特征不变。

---

## P1 — 配置一致性：16MB vs 2MB

**问题**：协议层 `kDefaultMaxFrameBodySize = 16MB`，连接层 `max_read_buffer_bytes = 2MB`。
默认配置下**实际能收的最大帧约 2MB**，与协议宣称的 16MB 不符（详见对话分析）。

**方案**（三选一，倾向第 2 个）：
1. 把 `max_read_buffer_bytes` 默认改成 ≥16MB（代价：慢速攻击窗口变大）
2. **让 `RpcCodec` 的 `max_body_size` 可配置，并校验 `max_read_buffer_bytes >= max_body_size + 20`**，
   不一致时 `Start()` 返回明确错误（推荐，把隐式耦合变成显式校验）
3. 文档写清两者关系

**工作量**：小。**风险**：改 `TcpServerOptions`，属 API 变更。

---

## P2 — 客户端读缓冲上限（补齐不对称）

**问题**：`src/net/tcp_client.cpp:128` 的 `std::string buffer` **无任何大小限制**，
只有 codec 的 16MB 声明校验。服务端若声明 15MB 并慢速发送，客户端内存无限涨。

**方案**：给 `TcpClient` 加 `max_read_buffer_bytes` 选项（默认 2MB，与服务端对称），
`kNeedMoreData` 时超限 → `CloseReason::kProtocolError` + 断连。

**工作量**：小。

---

## P3 — 单 Reactor 多核扩展

**问题**：一个 epoll 线程 = 一个核，连接数/吞吐上万后是瓶颈。

**方案**：`SO_REUSEPORT` + 每核一个 `EpollTcpServerBackend` 实例，
连接 accept 后**绑定固定 Reactor**（不跨线程迁移）。
`TcpServerBackend` 抽象已经留好缝，改动主要在 `TcpServer` facade 的多实例管理。

**前置**：先确认当前瓶颈（84k QPS 下 Reactor 是否真的饱和）——**先测量再优化**。

**工作量**：大。**风险**：连接生命周期、跨线程发送语义要重审。

---

## P4 — `codec` 的 `buffer.erase(0, n)` 是 O(n)

**问题**：每解一帧都要把剩余数据整体前移，大 buffer + 高频下是可观的拷贝。

**方案**：`read_buffer` 换成 **ring buffer**，或维护 `read_index_`、延迟 compact
（当 `read_index_ > buffer.size()/2` 时才真正 compact）。

**工作量**：中。**风险**：要同步改 `TcpClient` 侧的解码调用。

---

## P5 — `ServiceRegistry` 每次查找都加互斥锁

**问题**：`Find()` 走 `std::mutex`，**每个请求一次**，4 个 worker 并发就有争用。
注册远少于查找（配置期一次，运行期几乎为零）。

**方案**：`std::shared_mutex` 读写锁，或 copy-on-write 快照（注册时重建 `shared_ptr<const Map>`）。

**工作量**：小。**收益**：多核下请求路径少一把锁。

---

## P6 — deadline 改用相对超时

**问题**：`deadline_unix_ms` 用**墙上钟**，跨机器依赖 NTP 同步；时钟回拨会导致语义错误。

**方案**：body 里改传 `timeout_ms`（相对值），服务端收到时用自己的 `steady_clock` 起算。
协议字段调整属**破坏性改动**，需同步 `protocol/frame.h`、`protocol/codec.cpp`、
`docs/protocol.md`、proto 定义、相关测试，并按 `AGENTS.md` §6 先确认。

**替代（低风险）**：保留 `deadline_unix_ms`，**同时**加一个 `timeout_ms` 字段（字段号 5），
服务端优先用相对值 —— 利用 protobuf 的前向兼容，老客户端不受影响。

**工作量**：中。**风险**：协议变更，需确认。

---

## P7 — 客户端 epoll 化

**问题**：`TcpClient` 是阻塞 socket + **一连接一线程**，连接数 = 线程数。

**方案**：客户端也上 epoll + 状态机（或复用协程 IO 上下文），支撑上万连接。

**工作量**：大。**现状可接受**：定位是服务间少量长连接 + `RpcClientPool`。

---

## P8 — 线程池按 service 分组

**问题**：4 个 worker 共享一个队列，**一个慢 handler 会饿死所有快 handler**
（50ms 的 handler 会把上限压到 80 QPS）。

**方案**：按 service 分组线程池 + 队列优先级，或至少提供「慢任务隔离池」。

**工作量**：中。

---

## P9 — IDL 代码生成

**问题**：服务/方法靠**字符串手写注册**，拼错只在运行期才发现。

**方案**：`protoc` 插件从 `.proto` 的 `service` 定义生成 stub / skeleton。

**工作量**：大。**注意**：roadmap §5 明确「不包含代码生成」，做之前先确认范围。

---

## 建议的执行节奏

```
本周：P0 空闲超时（P0 是全项目唯一"资源泄漏"级别的洞，最值得做）
      + P1 配置一致性校验（小改动，顺手）
      + P2 客户端读缓冲上限（小改动，补齐对称性）

之后：先做 P5（半天，收益明确）→ P4（需要 benchmark 证明有收益才做）
      → P6 → P8 → P3/P7（大改，需要压测数据支撑）
```

**原则（写进 `RPC_FRAMEWORK_ROADMAP.md` §7 的精神）**：
> 不通过增加模块数量制造表面复杂度。每一项都要有**可测量的收益**或**明确的正确性修复**，
> 否则宁可不做。

---

## 动手前 checklist

- [x] 确认 `TcpServerOptions` 加字段（公共 API 变更）：默认 `0` 禁用，向后兼容
- [x] 确认背压中连接是否照常计时：照常（方案 A）
- [x] 确认 `OnCloseFn` 是否要加 reason：暂不加，`Logger` 打日志区分
- [x] `docker start minirpc-linux` 可用
- [x] 新测试文件已加进 `CMakeLists.txt` 的 `MINIRPC_TEST_TARGETS`
