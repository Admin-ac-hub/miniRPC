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
