# miniRPC — Claude 协作指南

本文件给 Claude Code 读，本仓库工作时必须遵守。背景与设计见 `RPC_FRAMEWORK_ROADMAP.md`、`docs/architecture.md`、`docs/protocol.md`，本文件不复述。

## 1. 项目定位

C++ RPC 框架，作为实习简历项目使用。所有改动按生产代码标准评估：API 稳定、错误处理完整、协议严谨、可压测可观测。不要以「学习项目」「demo 够用」为借口降低质量。

不要超出当前已落地能力擅自加功能；不要静默改公共 API（`RpcServer` / `RpcClient` / `ProtocolFrame` / `RpcRequest` / `RpcResponse` / `Status`）。这些是用户在 examples / 简历中已声明的契约，调整必须先和用户确认。

## 2. 平台

服务端 reactor 用 Linux `epoll + eventfd`，包在 `#ifdef __linux__` 内。macOS 只能编译并跑协议 / 客户端 / 单元测试。**任何需要在 Linux 验证的步骤直接进 docker，不要先在 macOS 上凑合**。

容器名 `minirpc-linux`，仓库挂在 `/work`，build 目录已用 Linux 工具链配过。标准流程：

```sh
docker start minirpc-linux
docker exec minirpc-linux bash -c "cd /work && cmake --build build -j && ctest --test-dir build --output-on-failure"
```

容器不在 / 挂载路径不同 / build 目录被 macOS 污染时，先问用户，不要自行 `docker run` 新容器或 `rm -rf build` 重配。

跨平台代码引入新 syscall / header 前确认 macOS + Linux 两端都能编译，否则用 `#ifdef` 隔离。

## 3. 构建测试

```sh
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

CMake 选项默认全开：`MINIRPC_BUILD_EXAMPLES` / `_TESTS` / `_BENCHMARKS`。

- `src/*.cpp` 由 `GLOB_RECURSE` 自动收进 `minirpc` 静态库；新增 test / example / benchmark 必须在 `CMakeLists.txt` 显式 `add_executable` + `target_link_libraries(... minirpc::minirpc)` + `add_test`。
- 仅 C++17。`-Wall -Wextra -Wpedantic` 已开，新代码不得引入新警告。
- 不要随手引入新依赖（spdlog / fmt / protobuf / gtest 等）。确需引入先问用户。

## 4. 目录与分层

`include/minirpc/<module>/*.h` ↔ `src/<module>/*.cpp` 镜像，新文件必须遵守。

依赖方向（上层依赖下层，**禁止反向**）：

```
client / server / discovery
  → serialization → protocol → net
  → runtime / observability → core
```

- `core` 不 include 其它模块。
- `client` 与 `server` 不互相 include。
- 需要打破层级 → 停下问用户，不要自行重构。

## 5. 编码约定

- 命名空间统一 `minirpc`，不嵌套子 namespace。
- 文件 `snake_case`；类型 / 方法 `PascalCase`；成员尾下划线 `member_`；常量 / 枚举值 `kPascalCase`（`kProtocolMagic`、`MessageType::kRequest`）。
- 错误用 `Status` / `StatusCode` 传递。**不要用异常做控制流**，析构 / 回调必须 `noexcept` 安全。
- 所有权清晰：拥有用 `unique_ptr` 或值，共享才用 `shared_ptr`。核心类已 delete 拷贝，新增类沿用。
- `#pragma once` + 完整路径 include `#include "minirpc/<module>/<file>.h"`，不写相对路径。
- 注释稀疏，只写「为什么」非显然之处，不写功能描述。

## 6. 协议不变量

固定 20 字节头：`magic(4) | version(2) | msg_type(1) | codec(1) | request_id(8) | body_size(4)`，整型大端序。`magic = 0x4d525043`，`version = 1`，`body_size` 上限 `kDefaultMaxFrameBodySize`。

- `MessageType` / `CodecType` 枚举值是协议契约，**禁止改既有取值**，只能追加。
- 协议字段调整属破坏性改动，必须同步：`protocol/frame.h`、`protocol/codec.cpp`、`docs/protocol.md`、相关 codec 测试，并先得到用户确认。
- 非法包 / 超长 body / 半包 / 粘包必须返回明确错误，不允许崩溃或断言。

## 7. 运行时硬规则

- IO 线程（reactor）只做收发；业务 handler 必须走 `ThreadPool`。
- 连接生命周期用 `ConnectionId + generation` 配对，发送 / 关闭必须传 generation，避免悬挂操作。
- `ThreadPool::Post` 队列满返回 `false`，调用方必须处理（背压或返回错误响应）。
- 客户端任何 pending 请求必须能被超时 / 连接关闭主动失败，禁止无限等待。

## 8. 测试

- 路径镜像：`tests/<module>/<thing>_test.cpp` ↔ `include/minirpc/<module>/`。
- 当前测试是手写 `assert`，沿用。引入 gtest / catch2 需先问用户。
- 修 bug 必须先补能复现的失败用例再改实现。
- 涉及 epoll 的集成测试在 docker 里跑（见 §2）。
