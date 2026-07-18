#include "minirpc/protocol/protobuf_body_codec.h"

#include <utility>

#include "minirpc/protocol/body_codec.h"
#include "minirpc_body.pb.h"

namespace minirpc {

std::string ProtobufEncodeRequestBody(const RpcRequest& request) {
    PbRequestBody pb;
    pb.set_service_name(request.service_name);
    pb.set_method_name(request.method_name);
    pb.set_payload(request.payload);
    pb.set_deadline_unix_ms(request.deadline_unix_ms);

    std::string output;
    if (!pb.SerializeToString(&output)) {
        throw BodyCodecError("protobuf: failed to serialize request body");
    }
    return output;
}

RpcRequest ProtobufDecodeRequestBody(uint64_t request_id, const std::string& body) {
    PbRequestBody pb;
    if (!pb.ParseFromString(body)) {
        throw BodyCodecError("protobuf: failed to parse request body");
    }

    RpcRequest request;
    request.request_id   = request_id;
    request.service_name = std::move(*pb.mutable_service_name());
    request.method_name  = std::move(*pb.mutable_method_name());
    request.payload      = std::move(*pb.mutable_payload());
    request.deadline_unix_ms = pb.deadline_unix_ms();
    return request;
}

std::string ProtobufEncodeResponseBody(const RpcResponse& response) {
    PbResponseBody pb;
    pb.set_status_code(response.status_code);
    pb.set_error_message(response.error_message);
    pb.set_payload(response.payload);

    std::string output;
    if (!pb.SerializeToString(&output)) {
        throw BodyCodecError("protobuf: failed to serialize response body");
    }
    return output;
}

RpcResponse ProtobufDecodeResponseBody(uint64_t request_id, const std::string& body) {
    PbResponseBody pb;
    if (!pb.ParseFromString(body)) {
        throw BodyCodecError("protobuf: failed to parse response body");
    }

    RpcResponse response;
    response.request_id    = request_id;
    response.status_code   = pb.status_code();
    response.error_message = std::move(*pb.mutable_error_message());
    response.payload       = std::move(*pb.mutable_payload());
    return response;
}

}  // namespace minirpc
