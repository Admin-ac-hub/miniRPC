# miniRPC 协议

当前协议帧采用固定头加可变包体。

```text
| magic:4 | version:2 | msg_type:1 | codec:1 | request_id:8 | body_size:4 | body:N |
```

字段说明：

- `magic`：固定为 `0x4d525043`，对应 `MRPC`。
- `version`：当前为 `1`。
- `msg_type`：请求、响应或心跳。
- `codec`：包体序列化格式。
- `request_id`：请求响应匹配 ID。
- `body_size`：包体长度。
- `body`：序列化后的请求或响应内容。

编解码实现位于 `include/minirpc/protocol` 和 `src/protocol`。

## RPC Body

协议帧的 `body` 由 `body_codec` 编码。

请求 body：

```text
| service_name_len:4 | service_name |
| method_name_len:4  | method_name  |
| payload_len:4      | payload      |
```

响应 body：

```text
| status_code:4      |
| error_len:4        | error_message |
| payload_len:4      | payload       |
```

所有长度和整数都使用大端序。`payload` 按 `std::string` 原样传输，允许二进制内容。

## Protobuf Body Format

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

客户端默认使用 protobuf 编码，服务端根据帧头中的 `codec` 字段自动解码并返回 protobuf 响应。
