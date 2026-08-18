# miniRPC

[![Linux CI](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml/badge.svg?branch=main)](https://github.com/Admin-ac-hub/miniRPC/actions/workflows/linux-ci.yml)

miniRPC 是一个面向 Linux 的 C++17 RPC 框架。稳定主路径采用 `epoll + eventfd + ThreadPool`，实现固定头二进制协议、Protobuf 请求体、同步/异步客户端、资源边界、连接级背压、优雅停机与运行时指标。仓库同时保留一条用户态协程服务端作为可选实验，不参与默认 API。

## 功能边界

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| Reactor RPC server | 已完成 | epoll + eventfd，业务 handler 在线程池执行 |
| 同步/异步 RPC client | 已完成 | 长连接、请求匹配、deadline、超时清理和重连 |
| 协议与编码 | 已完成 | 20 字节固定头，RPC body 使用 Protobuf，帧 body 支持二进制数据 |
| 业务调度 | 已完成 | IO 线程与 handler 线程池隔离，队列满时明确拒绝 |
| 资源与流量保护 | 已完成 | 最大连接数、读写缓冲上限、响应队列上限和连接级背压 |
| 优雅停机 | 已完成 | 等待已接收请求及完整响应写入，支持 grace period |
| 可观测性 | 已完成 | 连接、请求、失败、背压、延迟分位数和停机指标 |
| CoroutineRpcServer | 实验路径 | 用户态协程 + epoll，用同步风格组织异步 IO |

## 架构

```text
RpcClient
   |  request_id + deadline
   v
+---------------------- RpcServer -----------------------+
| TcpServer -> epoll Reactor                            |
|          |                                            |
|          v                                            |
|   frame decode -> ThreadPool::Post(handler)            |
|                         |                              |
|                         `-> response queue -> eventfd  |
+--------------------------------------------------------+
```

Reactor 线程只处理连接生命周期、协议帧收发和队列唤醒，业务 handler 始终在线程池中执行。跨线程发送和关闭操作通过队列提交，并使用 `eventfd` 唤醒 Reactor；连接操作校验 `ConnectionId + generation`，避免 fd 复用导致迟到操作落到错误连接。

## 环境要求

- Linux
- CMake 3.16+
- 支持 C++17 的 GCC 或 Clang
- Protocol Buffers compiler 和开发库

Ubuntu/Debian 可安装基础依赖：

```sh
sudo apt-get update
sudo apt-get install -y build-essential cmake protobuf-compiler libprotobuf-dev
```

## 快速开始

构建并运行测试：

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
- `TcpServerOptions::max_connections` 限制同时接入的连接数，`0` 表示不限制。
- 每个慢连接单独触发背压，不暂停 accept，也不阻塞其他连接。
- `Stop(grace_period)` 等待 handler 完成及响应整帧被发送接口接受；超时后执行硬停机。

协议字段与 Protobuf body 定义见 [docs/protocol.md](docs/protocol.md)，完整并发和生命周期设计见 [docs/architecture.md](docs/architecture.md)。

## 性能结果

当前压测以 Reactor 主路径为基线，并附带 Coroutine 实验路径的控制流对比。在固定 32 连接、每轮 100000 请求的 5 轮测试中，Reactor 中位 QPS 为 83887、P99 为 720us，两条路径合计 100 万请求全部成功。结果来自客户端与服务端同进程的闭环测试，不代表跨机器容量上限。完整环境、命令和分析见 [docs/benchmark_report.md](docs/benchmark_report.md)，复现方法见 [docs/performance.md](docs/performance.md)。

## 测试

GitHub Actions 在 Linux 上执行 Debug/Release 构建和完整 `ctest`。测试覆盖：

- 协议编解码、半包和粘包
- TCP server/client 与 RPC 集成
- 多连接收发、连接 churn 和 fd 复用
- 最大连接数拒绝与容量释放
- 慢客户端背压与大响应 partial send
- deadline、超时、断线和重连
- graceful shutdown 与在途响应写完成
- 可选的 coroutine runtime 和独立协程服务端路径

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
- [项目范围与收口状态](RPC_FRAMEWORK_ROADMAP.md)

## 当前限制

- benchmark 目前是同进程闭环模型，尚未隔离服务端 CPU、内存和网络开销。
- 稳定 Reactor 路径尚未提供连接空闲超时。
- CoroutineRpcServer 尚未提供连接空闲、读写超时和最大连接数配置，因此继续作为实验路径。
- 不包含服务发现、TLS、HTTP gateway 或分布式 tracing。
