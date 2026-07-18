#include <cassert>
#include <cstring>
#include <string>

#include "minirpc/core/status.h"
#include "minirpc/protocol/body_codec.h"

int main() {
    using namespace minirpc;

    // request encode/decode
    RpcRequest req;
    req.request_id = 42;
    req.service_name = "UserService";
    req.method_name = "GetUser";
    req.payload.assign("binary\0data", 11);
    req.deadline_unix_ms = 1234567890;

    auto body = EncodeRequestBody(req);
    assert(!body.empty());

    auto decoded = DecodeRequestBody(req.request_id, body);
    assert(decoded.request_id == 42);
    assert(decoded.service_name == "UserService");
    assert(decoded.method_name == "GetUser");
    assert(decoded.payload.size() == 11);
    assert(std::memcmp(decoded.payload.data(), "binary\0data", 11) == 0);
    assert(decoded.deadline_unix_ms == 1234567890);

    // response encode/decode
    RpcResponse resp;
    resp.request_id = 99;
    resp.status_code = static_cast<int32_t>(StatusCode::kOk);
    resp.error_message = "";
    resp.payload = "result";

    auto resp_body = EncodeResponseBody(resp);
    assert(!resp_body.empty());

    auto decoded_resp = DecodeResponseBody(resp.request_id, resp_body);
    assert(decoded_resp.request_id == 99);
    assert(decoded_resp.status_code == static_cast<int32_t>(StatusCode::kOk));
    assert(decoded_resp.error_message.empty());
    assert(decoded_resp.payload == "result");

    return 0;
}
