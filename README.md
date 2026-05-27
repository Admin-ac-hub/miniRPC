# miniRPC

miniRPC 是一个用于学习和实践 C++ RPC 框架设计的小型项目。

当前仓库已经按模块分层搭好基础骨架：

- `include/minirpc/core`：基础类型、状态码、端点。
- `include/minirpc/protocol`：协议帧、请求响应模型、编解码。
- `include/minirpc/serialization`：序列化抽象。
- `include/minirpc/net`：网络连接和 TCP server/client 抽象。
- `include/minirpc/client`：客户端调用入口。
- `include/minirpc/server`：服务端和服务注册。
- `include/minirpc/discovery`：服务发现和负载均衡。
- `include/minirpc/runtime`：线程池等运行时组件。
- `include/minirpc/observability`：日志和指标。

路线文档见 [RPC_FRAMEWORK_ROADMAP.md](RPC_FRAMEWORK_ROADMAP.md)。

## 构建

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

## 当前最小实现

服务端最小实现使用 Linux `epoll + eventfd + nonblocking socket` 单 Reactor：

- `RpcServer::Start()` 启动后台 reactor 线程。
- reactor 负责 accept、读写事件、连接生命周期和响应队列。
- 完整请求投递到 `ThreadPool` 执行业务 handler。
- worker 线程通过响应队列和 `eventfd` 唤醒 reactor 写回响应。

客户端 `RpcClient` 维护一条长连接，内部 reader 线程按 `request_id` 匹配响应；公开接口仍以同步 `Call()` 为主。

macOS 可以编译协议、客户端和基础测试；真实 epoll 服务端需要 Linux 环境运行。
