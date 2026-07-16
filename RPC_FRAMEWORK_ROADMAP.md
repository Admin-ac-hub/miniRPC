# miniRPC 框架路线文档

## 1. 项目定位

目标是实现一个小型但不粗糙的 C++ RPC 框架。它不追求一开始就覆盖工业级 RPC 的所有功能，而是优先建立清晰的网络通信、协议编解码、服务注册、远程调用、超时控制、并发处理和测试体系。

最终希望达到的效果：

```cpp
auto response = client.Call("UserService", "GetUser", request, 3000);
```

客户端可以像调用本地函数一样调用远程服务，服务端可以注册服务和方法，框架负责网络通信、请求响应匹配、序列化、错误处理和超时管理。

## 2. 核心能力范围

第一阶段必须具备：

- TCP 长连接通信
- 自定义 RPC 协议
- 解决 TCP 粘包和半包问题
- Request / Response 数据结构
- 服务注册与方法分发
- 同步 RPC 调用
- 请求 ID 匹配
- 超时控制
- 统一错误码
- 多线程服务端
- 基础测试和示例

第二阶段增强能力：

- 异步调用
- 连接复用
- 自动重连
- 心跳检测
- 简单服务发现
- 负载均衡
- 用户态协程调度器
- JSON / Protobuf 序列化抽象
- 指标统计和压测

第三阶段可选能力：

- 限流
- 熔断
- TLS
- 压缩
- 认证鉴权
- tracing
- 更完整的注册中心集成，例如 etcd 或 ZooKeeper

## 3. 推荐整体架构

建议模块划分如下：

```text
RpcClient
  -> ServiceResolver
  -> LoadBalancer
  -> ConnectionPool
  -> RpcCodec
  -> TcpConnection

RpcServer
  -> Acceptor
  -> TcpConnection
  -> RpcCodec
  -> RpcDispatcher
  -> ThreadPool
  -> ServiceHandler
```

核心职责：

- `RpcClient`：对外提供同步和异步调用接口。
- `RpcServer`：对外提供服务注册、启动和停止接口。
- `RpcCodec`：负责协议帧的编码和解码。
- `RpcDispatcher`：根据 `service_name` 和 `method_name` 找到对应 handler。
- `Serializer`：负责请求和响应对象的序列化。
- `ConnectionPool`：复用客户端连接。
- `ServiceResolver`：根据服务名找到服务节点。
- `LoadBalancer`：从多个节点中选择一个目标节点。
- `ThreadPool`：服务端业务线程池。

## 4. 协议设计

RPC 协议必须明确、可校验、可扩展。建议采用固定头 + 可变包体。

协议帧格式：

```text
| magic | version | msg_type | codec | request_id | body_size | body |
```

字段含义：

- `magic`：协议魔数，用于快速识别非法数据。
- `version`：协议版本。
- `msg_type`：消息类型，例如 request、response、heartbeat。
- `codec`：序列化格式，例如 json、protobuf、raw。
- `request_id`：请求唯一 ID，用于匹配响应。
- `body_size`：包体长度。
- `body`：序列化后的实际内容。

建议消息类型：

```cpp
enum class MessageType {
    REQUEST = 1,
    RESPONSE = 2,
    HEARTBEAT_REQUEST = 3,
    HEARTBEAT_RESPONSE = 4
};
```

协议层必须处理：

- 非法 magic
- 不支持的 version
- 不支持的 msg_type
- 不支持的 codec
- body 长度异常
- body 超过最大限制
- 半包
- 粘包
- 连接断开时的残留数据

## 5. 请求与响应模型

请求结构建议：

```cpp
struct RpcRequest {
    uint64_t request_id;
    std::string service_name;
    std::string method_name;
    std::string payload;
};
```

响应结构建议：

```cpp
struct RpcResponse {
    uint64_t request_id;
    int32_t status_code;
    std::string error_message;
    std::string payload;
};
```

统一状态码建议：

```cpp
enum class RpcStatus {
    OK = 0,
    TIMEOUT = 1,
    NETWORK_ERROR = 2,
    SERVICE_NOT_FOUND = 3,
    METHOD_NOT_FOUND = 4,
    SERIALIZE_ERROR = 5,
    DESERIALIZE_ERROR = 6,
    PROTOCOL_ERROR = 7,
    SERVER_ERROR = 8
};
```

## 6. 服务注册与分发

服务端维护一个注册表：

```text
service_name -> method_name -> handler
```

建议接口：

```cpp
using RpcHandler = std::function<RpcResponse(const RpcRequest&)>;

class ServiceRegistry {
public:
    void Register(
        const std::string& service_name,
        const std::string& method_name,
        RpcHandler handler
    );

    RpcHandler Find(
        const std::string& service_name,
        const std::string& method_name
    ) const;
};
```

分发流程：

```text
收到请求
  -> 解码协议帧
  -> 反序列化 RpcRequest
  -> 查找 service_name
  -> 查找 method_name
  -> 执行 handler
  -> 生成 RpcResponse
  -> 编码协议帧
  -> 发送响应
```

如果服务或方法不存在，不能让连接直接异常退出，而是返回明确错误响应。

## 7. 客户端调用模型

同步调用接口：

```cpp
RpcResponse Call(
    const std::string& service_name,
    const std::string& method_name,
    const std::string& payload,
    int timeout_ms
);
```

调用流程：

```text
生成 request_id
  -> 构造 RpcRequest
  -> 序列化请求
  -> 编码协议帧
  -> 发送到服务端
  -> 等待响应
  -> 根据 request_id 匹配响应
  -> 超时则返回 TIMEOUT
```

后续异步调用接口：

```cpp
std::future<RpcResponse> AsyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const std::string& payload,
    int timeout_ms
);
```

异步调用需要维护：

```text
request_id -> promise / callback / deadline
```

响应回来后，根据 `request_id` 唤醒对应调用方。超时请求要被主动清理，避免 pending map 无限增长。

## 8. 服务端并发模型

推荐演进路线：

1. 单线程版本先跑通完整 RPC 闭环。
2. 加入 IO 事件循环，支持多个连接。
3. 加入线程池，业务 handler 在线程池执行。
4. IO 线程只负责收发数据，不执行耗时业务。

推荐流程：

```text
IO 线程收到完整请求
  -> 投递到业务线程池
  -> worker 执行 handler
  -> 生成响应
  -> 将响应投递回对应连接
  -> IO 线程发送响应
```

需要注意：

- handler 执行不能阻塞 IO 线程。
- 同一个连接的发送队列要保证线程安全。
- 连接关闭后，未发送响应不能继续访问已释放连接。
- 服务端要限制最大连接数和最大请求包大小。

协程演进路线：

1. 先在 `runtime` 层实现独立的用户态 `Coroutine`，只验证 `Resume/Yield` 和栈切换。
2. 再增加 `Scheduler`、`TimerQueue` 和 fd 等待队列。
3. 最后把非阻塞 socket 的 `EAGAIN` 挂起和 epoll 事件恢复串起来，形成显式的 `CoRead/CoWrite`。
4. 协程网络路径成熟前，现有 reactor + `ThreadPool` 路径保持可用。

协程设计细节见 `docs/coroutine.md`。

## 9. 序列化层

框架不应该和某一种序列化格式强绑定。

建议抽象：

```cpp
class Serializer {
public:
    virtual ~Serializer() = default;
    virtual std::string SerializeRequest(const RpcRequest& request) = 0;
    virtual RpcRequest DeserializeRequest(const std::string& data) = 0;
    virtual std::string SerializeResponse(const RpcResponse& response) = 0;
    virtual RpcResponse DeserializeResponse(const std::string& data) = 0;
};
```

推荐顺序：

1. 先实现 JSON，方便调试。
2. 再实现 Protobuf，更接近真实 RPC 框架。

JSON 适合前期开发，Protobuf 适合性能、类型约束和跨语言扩展。

## 10. 连接管理

客户端应支持长连接复用，而不是每次调用都重新建连。

建议实现：

- 连接复用
- 自动重连
- 连接状态机
- 空闲连接关闭
- 心跳 ping / pong
- 失败节点临时摘除

心跳可以作为协议层的一种消息：

```text
HEARTBEAT_REQUEST
HEARTBEAT_RESPONSE
```

连接状态建议：

```cpp
enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    CLOSING
};
```

## 11. 服务发现与负载均衡

先实现本地配置版服务发现：

```text
UserService = 127.0.0.1:9001,127.0.0.1:9002
OrderService = 127.0.0.1:9011
```

客户端调用流程：

```text
service_name
  -> ServiceResolver
  -> endpoints
  -> LoadBalancer
  -> selected endpoint
  -> ConnectionPool
  -> RpcClient.Call
```

负载均衡策略：

- round-robin
- random
- least-active，可选

第一版建议只做 round-robin，简单、可测、行为稳定。

## 12. 可观测性

至少要有基础日志和指标。

日志建议记录：

- 服务启动和停止
- 客户端连接和断开
- 请求开始和结束
- 请求耗时
- 错误码
- 协议异常
- 超时请求

指标建议统计：

- 当前连接数
- 总请求数
- 成功请求数
- 失败请求数
- 超时请求数
- pending 请求数
- 平均延迟
- P95 / P99 延迟，可选

可观测性的目标是支持性能分析闭环，而不是只把数字打印出来。压测时至少要能回答：

- 低并发下 reactor 路径和协程路径的额外调度成本是否可见。
- 高并发下瓶颈来自连接管理、协议编解码、业务线程池还是写回队列。
- handler 变慢时 IO 线程是否仍能 accept/read/write。
- 线程池队列满时拒绝了多少请求，是否出现 pending 请求无限增长。

推荐把压测报告固定为 Reactor + ThreadPool 与 Coroutine + epoll + ThreadPool 两条路径对比，记录 QPS、P50/P95/P99、CPU、失败数和关键服务端指标。报告模板见 `docs/performance.md`。

## 12.1 背压、deadline 与优雅关闭

这三项比横向扩展 HTTP、TLS 或复杂注册中心更贴近 RPC 框架主线。

背压：

- `ThreadPool::Post()` 队列满必须被调用方处理。
- 第一版可以返回 `kServerError` 或关闭连接，后续可追加 `kOverloaded` 状态码。
- metrics 记录 `rejected_requests`，压测报告区分 accepted QPS 和 offered QPS。

deadline：

- 客户端 timeout 已传播为请求 protobuf body 中的 `deadline_unix_ms`。
- 服务端收到已过期请求直接返回 `kTimeout`，不进入业务 handler。
- 后续可在 handler 执行前暴露剩余时间，避免已无意义的业务继续排队。

优雅关闭：

- `Stop(grace_period)` 后停止 accept 新连接。
- 已收到请求允许在 grace period 内完成。
- grace period 超时后关闭连接。
- pending 请求通过连接关闭或客户端超时明确失败，避免无限等待。

暂不优先投入：

- HTTP server
- TLS
- etcd / ZooKeeper 注册中心
- 复杂 XML 配置
- syscall hook
- 分布式 tracing

当前主线保持为：RPC 协议 + epoll + 协程 + ThreadPool + timeout + 测试 + 压测分析。

## 13. 测试策略

单元测试：

- 协议头编码和解码
- 粘包处理
- 半包处理
- 非法 magic
- 超大包限制
- request_id 生成
- 服务注册
- 服务查找
- 错误码转换

集成测试：

- 启动 EchoServer
- 客户端调用 `EchoService.Echo`
- 校验响应 payload
- 调用不存在的服务
- 调用不存在的方法
- 模拟服务端延迟触发超时
- 断开连接后重连

压测：

- 并发连接数
- 并发请求数
- QPS
- 平均延迟
- P99 延迟
- 大包请求
- 长连接稳定性

## 14. 推荐项目结构

```text
miniRPC/
  include/
    rpc/
      rpc_client.h
      rpc_server.h
      rpc_controller.h
      rpc_channel.h
      rpc_codec.h
      rpc_protocol.h
      rpc_dispatcher.h
      serializer.h
      service_registry.h
      service_resolver.h
      load_balancer.h
      connection_pool.h
      thread_pool.h

  src/
    rpc_client.cpp
    rpc_server.cpp
    rpc_codec.cpp
    rpc_protocol.cpp
    rpc_dispatcher.cpp
    serializer_json.cpp
    serializer_protobuf.cpp
    service_registry.cpp
    service_resolver.cpp
    load_balancer.cpp
    connection_pool.cpp
    thread_pool.cpp

  examples/
    echo_server.cpp
    echo_client.cpp
    user_service_server.cpp
    user_service_client.cpp

  tests/
    codec_test.cpp
    protocol_test.cpp
    registry_test.cpp
    rpc_call_test.cpp
    timeout_test.cpp
    reconnect_test.cpp

  benchmark/
    rpc_bench.cpp

  docs/
    protocol.md
    architecture.md
```

## 15. 里程碑规划

### Milestone 1：最小 RPC 闭环

目标：完成 `EchoService.Echo()` 同步调用。

必须完成：

- TCP 服务端监听
- TCP 客户端连接
- 长度头协议
- 粘包和半包处理
- RpcRequest / RpcResponse
- 服务注册
- 服务分发
- 客户端同步调用
- Echo 示例

验收标准：

- 启动服务端后，客户端可以调用 Echo 方法。
- 请求内容和响应内容一致。
- 多次连续调用不会粘包错乱。

### Milestone 2：协议严谨化

目标：让协议具备版本、类型、序列化方式和错误处理。

必须完成：

- magic
- version
- msg_type
- codec
- request_id
- body_size
- 最大包大小限制
- 协议错误返回

验收标准：

- 非法包不会导致服务端崩溃。
- 半包和粘包测试通过。
- 服务端能识别 request、response、heartbeat 类型。

### Milestone 3：超时与错误码

目标：客户端不会永久阻塞，错误能被明确表达。

必须完成：

- request_id 生成器
- pending request 管理
- 同步调用超时
- RpcStatus
- 服务不存在错误
- 方法不存在错误
- 网络错误
- 协议错误

验收标准：

- 服务端不响应时，客户端能按超时时间返回。
- 调用不存在服务时返回 `SERVICE_NOT_FOUND`。
- 调用不存在方法时返回 `METHOD_NOT_FOUND`。

### Milestone 4：多线程服务端

目标：服务端支持并发处理多个请求。

必须完成：

- ThreadPool
- IO 线程和 worker 线程分离
- 业务 handler 在线程池执行
- 响应安全发送
- 最大连接数限制

验收标准：

- 多客户端并发调用成功。
- 慢请求不会阻塞所有连接。
- 服务端可以稳定处理持续请求。

### Milestone 5：异步调用

目标：客户端支持 future 或 callback 风格调用。

必须完成：

- `AsyncCall`
- `request_id -> promise`
- 响应唤醒
- 超时清理
- 异步调用测试

验收标准：

- 同时发起多个异步请求，响应能正确匹配。
- 某个请求超时不会影响其他请求。

### Milestone 6：连接复用与心跳

目标：提高客户端连接稳定性。

必须完成：

- 长连接复用
- 连接状态管理
- 心跳请求
- 心跳响应
- 自动重连
- 空闲连接关闭

验收标准：

- 多次调用复用同一连接。
- 服务端重启后客户端能重连。
- 心跳失败后连接会被关闭或重建。

### Milestone 7：服务发现与负载均衡

目标：支持一个服务对应多个节点。

必须完成：

- 本地配置文件服务发现
- endpoint 列表解析
- round-robin 负载均衡
- endpoint 连接池

验收标准：

- 同一个服务可以配置多个服务端地址。
- 客户端请求能轮询分配到不同节点。
- 某个节点失败时不会永久阻塞整体调用。

### Milestone 8：测试、指标与压测

目标：让框架具有可信度。

必须完成：

- 单元测试
- 集成测试
- 异常测试
- 简单 benchmark
- QPS、P50/P95/P99 和失败数输出
- active/pending/rejected 请求指标
- ThreadPool 队列长度指标
- Reactor 与 Coroutine 路径对比报告

验收标准：

- 核心模块测试通过。
- Echo 压测可以输出 QPS、平均延迟、P99 和失败数。
- 慢 handler 场景可以证明 IO 线程没有被业务阻塞。
- 线程池队列满时能返回明确错误并统计 rejected 请求。
- 长时间运行没有明显内存增长或 pending 请求泄漏。

### Milestone 9：用户态协程运行时

目标：让 runtime 层具备用户态协程切换和调度能力，为后续 epoll 协程化 IO 做准备。

必须完成：

- `Coroutine`
- x86-64 上下文切换汇编
- `Scheduler`
- `TimerQueue`
- `CoRead/CoWrite`
- socketpair 或本地 TCP 协程 IO 测试

验收标准：

- 多个协程可以按预期 `Resume/Yield`。
- `CoRead/CoWrite` 遇到 `EAGAIN` 不阻塞线程。
- epoll 事件可以恢复等待中的协程。

### Milestone 10：协程化连接处理

目标：服务端连接读写可以用同步风格代码运行在异步 IO 之上。

必须完成：

- connection coroutine loop
- RPC frame 读取和写回协程化
- 连接关闭时恢复或失败等待中的协程
- 与现有 `ThreadPool` handler 分发保持兼容
- 集成测试和压测对比

验收标准：

- Echo RPC 可以走协程连接路径。
- 慢 handler 不阻塞 IO 线程。
- 连接关闭、协议错误、超时请求都能明确失败。

## 16. 推荐开发顺序

最稳妥的开发顺序：

1. 实现 TCP 通信。
2. 实现长度头协议。
3. 实现协议帧编码和解码。
4. 实现 `RpcRequest` 和 `RpcResponse`。
5. 实现服务注册和分发。
6. 实现客户端同步调用。
7. 跑通 Echo 示例。
8. 加入 request_id 和超时。
9. 加入统一错误码。
10. 加入线程池服务端。
11. 抽象序列化层。
12. 加入 JSON 序列化。
13. 加入异步调用。
14. 加入连接复用。
15. 加入心跳和自动重连。
16. 加入本地配置服务发现。
17. 加入 round-robin 负载均衡。
18. 加入日志、指标和压测。

## 17. 第一周建议计划

第 1 天：

- 建立项目结构。
- 写出协议帧结构。
- 实现基础 TCP server / client。

第 2 天：

- 实现长度头编解码。
- 完成粘包和半包处理。
- 写 codec 单元测试。

第 3 天：

- 实现 RpcRequest / RpcResponse。
- 实现 JSON 序列化。
- 实现服务注册表。

第 4 天：

- 实现 RpcServer。
- 实现 RpcDispatcher。
- 跑通 EchoService。

第 5 天：

- 实现 RpcClient。
- 实现同步调用。
- 加入 request_id。

第 6 天：

- 加入超时。
- 加入错误码。
- 补齐服务不存在、方法不存在测试。

第 7 天：

- 整理 examples。
- 写集成测试。
- 写 README。
- 做一次简单压测。

## 18. 关键工程原则

- 先跑通最小闭环，再扩展复杂能力。
- 协议层必须严谨，不能依赖临时字符串拼接。
- 网络层必须处理粘包和半包。
- 客户端必须有超时，不能无限阻塞。
- 错误必须通过统一模型返回。
- 服务端 IO 线程不要执行耗时业务。
- 每个里程碑都要有可运行示例和测试。
- 优先保证行为正确，再考虑性能优化。
- 不要一开始就引入复杂注册中心，先用本地配置。
- 不要一开始就追求所有调用方式，先同步，后异步。

## 19. 最小可验收目标

第一个可验收版本应该支持：

```text
EchoService.Echo(string message) -> string message
```

并具备：

- 自定义协议
- 粘包和半包处理
- 服务注册
- 同步调用
- request_id
- 超时
- 错误码
- 基础测试

只要这个版本足够稳定，后面的异步调用、心跳、连接池、服务发现和负载均衡都可以自然扩展出来。
