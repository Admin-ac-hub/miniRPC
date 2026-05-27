# miniRPC 架构分层

## 分层

```text
Application
  -> client / server
  -> discovery
  -> serialization
  -> protocol
  -> net
  -> runtime / observability
  -> core
```

## 模块说明

- `core`：跨模块基础类型，不依赖其他业务模块。
- `protocol`：RPC 协议帧、消息类型和编解码。
- `serialization`：请求响应对象和字节流之间的转换。
- `net`：TCP 连接、客户端、服务端监听。
- `client`：面向调用方的 RPC 调用入口。
- `server`：服务注册、服务启动和请求分发。
- `discovery`：服务解析和负载均衡。
- `runtime`：线程池、任务调度等运行时能力。
- `observability`：日志、指标和后续 tracing。

