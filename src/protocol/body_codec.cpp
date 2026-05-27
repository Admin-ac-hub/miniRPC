#include "minirpc/protocol/body_codec.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>

namespace minirpc {
namespace {

// RPC 帧头只描述消息种类和 body 长度；本文件负责 body 内部的二进制布局。
// 这里约定所有整数均使用网络字节序（大端序），避免客户端与服务端运行在
// 不同 CPU 字节序上时产生解释差异。
void AppendUint32(std::string* output, uint32_t value) {
    // 按从高位到低位的顺序逐字节追加，编码后的长度固定为 4 字节。
    output->push_back(static_cast<char>((value >> 24) & 0xff));
    output->push_back(static_cast<char>((value >> 16) & 0xff));
    output->push_back(static_cast<char>((value >> 8) & 0xff));
    output->push_back(static_cast<char>(value & 0xff));
}

void AppendInt32(std::string* output, int32_t value) {
    // status_code 是有符号值，但线上只关心其 32 位二进制表示。
    // 转为 uint32_t 后复用统一的大端整数编码过程。
    AppendUint32(output, static_cast<uint32_t>(value));
}
//*offset才是数值
uint32_t ReadUint32(const std::string& input, std::size_t* offset) {
    // offset 是解码游标。读取字段前必须确认剩余字节足够，
    // 否则对损坏或被截断的数据包进行访问会越界。
    if (input.size() - *offset < sizeof(uint32_t)) {
        throw BodyCodecError("truncated uint32 field");
    }
    const auto* data = reinterpret_cast<const unsigned char*>(input.data() + *offset);
    *offset += sizeof(uint32_t);
    // 编码端以大端写入，因此解码端通过移位重建原始整数。
    // 使用 unsigned char 避免 char 的符号扩展影响高位字节。
    return (static_cast<uint32_t>(data[0]) << 24) |
           (static_cast<uint32_t>(data[1]) << 16) |
           (static_cast<uint32_t>(data[2]) << 8) |
           static_cast<uint32_t>(data[3]);
}

int32_t ReadInt32(const std::string& input, std::size_t* offset) {
    // 与 AppendInt32 对称：先还原 32 位内容，再解释为有符号状态码。
    return static_cast<int32_t>(ReadUint32(input, offset));
}



void AppendString(std::string* output, const std::string& value) {
    // 字符串采用 length-prefixed 编码：
    //   | length: uint32 | raw bytes: length |
    // std::string 可保存 '\0'，因此 payload 并不局限于文本，也可承载二进制数据。
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
        throw BodyCodecError("string field too large");
    }
    AppendUint32(output, static_cast<uint32_t>(value.size()));
    output->append(value);
}

std::string ReadString(const std::string& input, std::size_t* offset) {
    // 先取得声明长度，再检查 body 中是否真的具有这些字节。
    // 不能依赖结尾 '\0'，因为线上格式是长度限定的原始字节串。
    const uint32_t size = ReadUint32(input, offset);
    if (input.size() - *offset < size) {
        throw BodyCodecError("truncated string field");
    }
    std::string value(input.data() + *offset, size);
    *offset += size;
    return value;
}

void RequireFullyConsumed(const std::string& input, std::size_t offset) {
    // 一个合法 body 必须恰好包含协议规定的字段。
    // 拒绝尾随字节可以尽早发现双方协议版本不一致或非法数据注入。
    if (offset != input.size()) {
        throw BodyCodecError("unexpected trailing bytes");
    }
}

}  // namespace

BodyCodecError::BodyCodecError(std::string message) : message_(std::move(message)) {}

const char* BodyCodecError::what() const noexcept {
    return message_.c_str();
}


// 请求 body 的线格式：
//   | service_name_len:4 | service_name:N |
//   | method_name_len:4  | method_name:M  |
//   | payload_len:4      | payload:P      |
// request_id 不重复写入 body，它已经存放在外层 ProtocolFrame 头部。
std::string EncodeRequestBody(const RpcRequest& request) {
    std::string body;
    AppendString(&body, request.service_name);
    AppendString(&body, request.method_name);
    AppendString(&body, request.payload);
    return body;
}

RpcRequest DecodeRequestBody(uint64_t request_id, const std::string& body) {
    // 按编码顺序逐项消费字段；任何截断或多余字节都会转化为 BodyCodecError。
    std::size_t offset = 0;
    RpcRequest request;
    // request_id 来自帧头，使解码后的业务请求仍能与对应响应关联。
    request.request_id = request_id;
    request.service_name = ReadString(body, &offset);
    request.method_name = ReadString(body, &offset);
    request.payload = ReadString(body, &offset);
    RequireFullyConsumed(body, offset);
    return request;
}

// 响应 body 的线格式：
//   | status_code:4       |
//   | error_message_len:4 | error_message:E |
//   | payload_len:4       | payload:P       |
// 成功响应通常 error_message 为空；失败响应可在该字段携带错误原因。
std::string EncodeResponseBody(const RpcResponse& response) {
    std::string body;
    AppendInt32(&body, response.status_code);
    AppendString(&body, response.error_message);
    AppendString(&body, response.payload);
    return body;
}

RpcResponse DecodeResponseBody(uint64_t request_id, const std::string& body) {
    // 与 EncodeResponseBody 保持严格相同的字段顺序，避免字段错位解释。
    std::size_t offset = 0;
    RpcResponse response;
    // 响应匹配 ID 同样取自外层帧头，而不是由 body 重复携带。
    response.request_id = request_id;
    response.status_code = ReadInt32(body, &offset);
    response.error_message = ReadString(body, &offset);
    response.payload = ReadString(body, &offset);
    RequireFullyConsumed(body, offset);
    return response;
}

}  // namespace minirpc
