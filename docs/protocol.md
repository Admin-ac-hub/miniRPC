# miniRPC 协议

当前协议帧采用固定头加可变包体。

```text
| magic:4 | version:2 | msg_type:1 | codec:1 | request_id:8 | body_size:4 | body:N |
```

字段说明：

- `magic`：固定为 `0x4d525043`，对应 `MRPC`。
- `version`：当前为 `1`。
- `msg_type`：请求、响应，以及为后续兼容保留的心跳枚举值；当前 RPC 主链路只处理请求和响应。
- `codec`：包体编码标识。RPC 主链路当前只支持 `kProtobuf`；`kRaw` 和 `kJson` 保留给低层帧使用或后续兼容。
- `request_id`：请求响应匹配 ID。
- `body_size`：包体长度。
- `body`：序列化后的请求或响应内容。

编解码实现位于 `include/minirpc/protocol` 和 `src/protocol`。

## 接收大小限制

`kDefaultMaxFrameBodySize` 和独立 `RpcCodec` 的默认 body 上限仍为 16 MiB。TCP 接收策略可以设置更小的上限，不改变协议字段或枚举值：

| 接收端 | 默认读缓冲 | 有效 body 上限 |
| --- | --- | --- |
| `TcpServer` / `RpcServer` | 2 MiB | `max_frame_body_size`，默认 2 MiB − 20 字节 |
| `TcpClient` / `RpcClient` | 2 MiB | `min(16 MiB, max_read_buffer_bytes − 20)` |
| 实验性协程 frame channel | 无独立读缓冲预算 | 默认 16 MiB |

body 包含完整的序列化内容，因此 RPC payload 还需为 Protobuf 元数据留出空间。超过接收端上限的长度声明在收齐 20 字节头时直接拒绝，不等待 body 到齐；合法整帧恰好等于读缓冲预算时允许通过，连续多个合法帧也不受累计流量限制。

低层 TCP 可显式接收 16 MiB body：

```cpp
minirpc::TcpServerOptions server_options;
server_options.max_frame_body_size = minirpc::kDefaultMaxFrameBodySize;
server_options.max_read_buffer_bytes =
    minirpc::kProtocolHeaderSize + server_options.max_frame_body_size;
minirpc::TcpServer server;
server.SetOptions(server_options);  // 在 Start() 前设置。

minirpc::TcpClientOptions client_options;
client_options.max_read_buffer_bytes =
    minirpc::kProtocolHeaderSize + minirpc::kDefaultMaxFrameBodySize;
minirpc::TcpClient client(client_options);
```

`TcpServer::Start()` 要求 `max_frame_body_size <= 16 MiB` 且
`max_read_buffer_bytes >= 20 + max_frame_body_size`，不一致时返回 `kServerError`。
`SetOptions()` 仅在启动前或外部 `Stop()` 完成后生效；运行中或 reactor 尚未 join 时忽略修改并记录 warn。
`TcpClient::Connect()` 要求读缓冲至少容纳 20 字节固定头，否则返回 `kNetworkError`；
`0` 不表示禁用限制。发送方需要遵守接收方预算；这些选项不限制本端发送大小。
`RpcClient` / `RpcServer` 目前使用上述默认值，尚未暴露 TCP 选项透传。

`RpcServer` 的响应也按默认接收预算限制：序列化后的整个 body 不得超过
2 MiB − 20 字节，恰好达到上限仍可发送。这个大小包括 Protobuf 字段开销、
`payload` 和 `error_message`，不只是业务 payload。超限时保留 `request_id`，
改回 `kSerializeError`、`error_message = "response body too large"` 和空 payload；
连接可继续处理后续请求，默认客户端不会因该响应超限而断连。
独立 `RpcCodec`、低层 TCP 发送接口和实验性协程服务端的上限不受此策略影响。

## Protobuf RPC Body

当 `codec = 2`（kProtobuf）时，body 使用 Protocol Buffers (proto3) 序列化。

请求 body（`PbRequestBody`）：

```text
string service_name = 1;
string method_name  = 2;
bytes  payload      = 3;
int64  deadline_unix_ms = 4;
```

`deadline_unix_ms` 为 Unix epoch 毫秒时间戳。值为 `0` 表示未设置 deadline；服务端收到已过期请求时直接返回 `kTimeout`，不会进入业务 handler。该字段只追加在 protobuf body 中，不改变外层 20 字节协议头。

响应 body（`PbResponseBody`）：

```text
int32  status_code   = 1;
string error_message = 2;
bytes  payload       = 3;
```

proto 定义文件：`proto/minirpc_body.proto`

客户端使用 Protobuf 编码。服务端收到非 `kProtobuf` RPC 请求时返回 `kNotImplemented`，响应 body 仍使用 Protobuf；低层 `TcpServer` 可以继续收发其它合法 frame codec。
