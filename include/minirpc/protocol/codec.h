#pragma once

#include <cstddef>
#include <string>

#include "minirpc/protocol/frame.h"

namespace minirpc {

enum class DecodeResult {
    kNeedMoreData,
    kSuccess,
    kProtocolError
};

class RpcCodec {
public:
    explicit RpcCodec(std::size_t max_body_size = kDefaultMaxFrameBodySize);

    std::string Encode(const ProtocolFrame& frame) const;

    // 解出一帧。
    //
    // frame 的输出语义：
    //   kSuccess       → 全部字段有效，含 body。
    //   kProtocolError → 若失败发生在 magic/version 校验之后（msg_type / codec / body_size），
    //                    frame 的 version / message_type / codec_type / request_id 会被填充，
    //                    body 为空；magic 或 version 不符时不填充。
    //   kNeedMoreData  → 头已解析但 body 未收全时，填充与上面相同的头字段；头不完整时不填充。
    //
    // 这样调用方在关闭连接前，可以回一个 request_id 正确的错误响应。
    DecodeResult TryDecode(std::string& buffer, ProtocolFrame* frame, std::string* error) const;

private:
    std::size_t max_body_size_;
};

}  // namespace minirpc

