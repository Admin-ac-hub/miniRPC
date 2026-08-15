# miniRPC

[![Linux CI](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml/badge.svg?branch=main)](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml)

miniRPC 是一个面向 Linux 的 C++17 RPC 框架。项目实现了固定头二进制协议、Protobuf 请求体、同步/异步客户端、线程池业务分发、连接级背压、优雅停机与运行时指标，并为 Reactor 服务端提供可独立构建和测试的 epoll、io_uring 两种网络后端。

默认后端仍为 epoll。io_uring Reactor 后端已经完成 accept、recv、send、eventfd 唤醒、异步取消和连接关闭生命周期；现有压测中它在部分小包高并发场景更快，但在 64 KiB 响应和 512 连接尾延迟上仍有明显回退，因此没有被包装成无条件的性能升级。

## 功能边界

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| Reactor RPC server | 已完成 | epoll 默认，io_uring 构建期可选 |
| 同步/异步 RPC client | 已完成 | 长连接、请求匹配、deadline、超时清理和重连 |
| 协议与序列化 | 已完成 | 20 字节固定头，raw 与 Protobuf body codec |
| 业务调度 | 已完成 | IO 线程与 handler 线程池隔离，队列满时明确拒绝 |
| 流量保护 | 已完成 | 单连接高/低水位背压和写缓冲硬上限 |
| 优雅停机 | 已完成 | 等待已接收请求及完整响应写入，支持 grace period |
| 可观测性 | 已完成 | 连接、请求、失败、背压、延迟分位数和停机指标 |
| CoroutineRpcServer | 实验路径 | 用户态协程 + epoll，不受 Reactor 后端选项影响 |
| io_uring client / coroutine IO | 未实现 | 当前 io_uring 仅用于 Reactor TCP server |

## 架构

```text
RpcClient
   |  request_id + deadline
   v
+---------------------- RpcServer -----------------------+
| TcpServer facade                                      |
|   +-> EpollTcpServerBackend (default)                  |
|   `-> IoUringTcpServerBackend (build-time selectable)  |
|          |                                             |
|          v                                             |
|   frame decode -> ThreadPool::Post(handler)            |
|                         |                              |
|                         `-> response queue -> eventfd  |
+--------------------------------------------------------+
```

Reactor 线程只处理连接生命周期、协议帧收发和队列唤醒，业务 handler 始终在线程池中执行。io_uring 的 SQE 提交和 CQE 消费都限制在 IO 线程内；每个操作使用独立 token，连接相关完成事件还会校验 `ConnectionId + generation`，避免 fd 复用、迟到 CQE 和 buffer 生命周期竞态。

## 环境要求

- Linux
- CMake 3.16+
- 支持 C++17 的 GCC 或 Clang
- Protocol Buffers compiler 和开发库
- io_uring 后端额外需要 Linux 5.7+、liburing 2.0+

Ubuntu/Debian 可安装基础依赖：

```sh
sudo apt-get update
sudo apt-get install -y build-essential cmake protobuf-compiler libprotobuf-dev
```

启用 io_uring 时再安装：

```sh
sudo apt-get install -y liburing-dev
```

## 快速开始

默认构建使用 epoll：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

启动示例服务端，再在另一个终端运行客户端：

```sh
./build/echo_server
./build/echo_client
```

io_uring 使用独立构建目录：

```sh
cmake -S . -B build-uring \
  -DCMAKE_BUILD_TYPE=Release \
  -DMINIRPC_TCP_SERVER_BACKEND=io_uring
cmake --build build-uring --parallel
ctest --test-dir build-uring --output-on-failure
```

`MINIRPC_TCP_SERVER_BACKEND` 只接受 `epoll` 和 `io_uring`。缺少 liburing 时 CMake 会直接失败；内核、seccomp、feature 或必需 opcode 不满足要求时，`Start()` 返回 `kNetworkError`，不会静默回退到 epoll。

## 使用示例

注册并启动服务：

```cpp
#include "minirpc/server/rpc_server.h"

minirpc::RpcServer server;
server.RegisterService(
    "EchoService", "Echo",
    [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.payload = request.payload;
        return response;
    });

auto status = server.Start({"127.0.0.1", 9000});
if (!status.ok()) {
    return 1;
}
```

同步调用：

```cpp
#include <chrono>
#include "minirpc/client/rpc_client.h"

minirpc::RpcClient client({"127.0.0.1", 9000});
auto response = client.Call(
    "EchoService", "Echo", "hello", std::chrono::seconds(2));
```

异步调用返回 `std::future<RpcResponse>`：

```cpp
auto future = client.CallAsync(
    "EchoService", "Echo", "hello", std::chrono::seconds(2));
auto response = future.get();
```

## 协议与可靠性

外层协议头固定为 20 字节，所有整数使用大端序：

```text
| magic:4 | version:2 | type:1 | codec:1 | request_id:8 | body_size:4 |
```

- 解码器处理半包、粘包、非法 magic/version 和超长 body。
- 客户端 pending 请求会被响应、超时或连接关闭主动完成，不会无限等待。
- `ThreadPool::Post()` 队列满时服务端返回明确错误，避免任务无限堆积。
- 每个慢连接单独触发背压，不暂停 accept，也不阻塞其他连接。
- `Stop(grace_period)` 等待 handler 完成及响应整帧被发送接口接受；超时后执行硬停机。
- io_uring 关闭连接时先停止 rearm、取消在途操作，回收相关 CQE 后再释放 fd。

协议字段与 Protobuf body 定义见 [docs/protocol.md](docs/protocol.md)，完整并发和生命周期设计见 [docs/architecture.md](docs/architecture.md)。

## 性能结果

以下数据来自同一 Linux 6.12.54、GCC 13.3、liburing 2.5 环境中的 Release 构建，每个场景取 5 轮中位数：

| 场景 | epoll QPS | io_uring QPS | QPS 变化 | epoll P99 | io_uring P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 连接，16 B | 97,039 | 113,453 | +16.9% | 1.33 ms | 1.18 ms |
| 128 连接，16 B | 99,939 | 147,682 | +47.8% | 2.17 ms | 1.80 ms |
| 512 连接，16 B | 68,228 | 128,028 | +87.6% | 10.20 ms | 76.80 ms |
| 16 连接，64 KiB | 27,531 | 21,377 | -22.4% | 1.09 ms | 1.38 ms |

这些结果来自客户端与服务端同进程的闭环测试，不代表跨机器容量上限。512 连接场景同时创建了 512 个客户端线程，尾延迟包含客户端调度影响；64 KiB 回退在三组独立测试中持续出现。完整环境、命令、失败计数和分析见 [docs/benchmark_report.md](docs/benchmark_report.md)，复现方法见 [docs/performance.md](docs/performance.md)。

## 测试

GitHub Actions 对 epoll 和 io_uring 分别执行 Release configure、build 和完整 `ctest`。测试覆盖：

- 协议编解码、半包和粘包
- TCP server/client 与 RPC 集成
- 多连接收发、连接 churn 和 fd 复用
- 慢客户端背压与大响应 partial send
- deadline、超时、断线和重连
- graceful shutdown 与在途响应写完成
- coroutine runtime 和独立协程服务端路径

## 项目结构

```text
include/minirpc/   公共头文件
src/               框架实现
proto/             Protobuf body 定义
examples/          echo server/client
tests/             单元与集成测试
benchmark/         rpc_bench 压测程序
docs/              架构、协议与性能文档
```

## 文档

- [架构设计](docs/architecture.md)
- [协议格式](docs/protocol.md)
- [协程路径](docs/coroutine.md)
- [压测方法](docs/performance.md)
- [压测报告](docs/benchmark_report.md)
- [迭代路线](RPC_FRAMEWORK_ROADMAP.md)

## 当前限制

- io_uring 只覆盖 Reactor TCP server，客户端和协程路径仍使用原有实现。
- epoll 仍是默认后端；切换默认值前需要完成大包发送路径和高连接尾延迟 profiling。
- benchmark 目前是同进程闭环模型，尚未隔离服务端 CPU、内存和网络开销。
- 不包含 TLS、HTTP gateway、分布式 tracing 或生产级注册中心。
