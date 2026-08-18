#include <cassert>
#include <chrono>
#include <string>
#include <thread>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/protocol/body_codec.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/server/rpc_server.h"
#include "minirpc/server/service_registry.h"

int main() {
    minirpc::RpcCodec codec;
    minirpc::ProtocolFrame frame;
    frame.request_id = 42;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.codec_type = minirpc::CodecType::kRaw;
    frame.body = "hello";

    std::string buffer = codec.Encode(frame);
    minirpc::ProtocolFrame decoded;
    std::string error;
    auto result = codec.TryDecode(buffer, &decoded, &error);
    assert(result == minirpc::DecodeResult::kSuccess);
    assert(buffer.empty());
    assert(decoded.request_id == 42);
    assert(decoded.body == "hello");

    std::string sticky = codec.Encode(frame) + codec.Encode(frame);
    assert(codec.TryDecode(sticky, &decoded, &error) == minirpc::DecodeResult::kSuccess);
    assert(codec.TryDecode(sticky, &decoded, &error) == minirpc::DecodeResult::kSuccess);
    assert(sticky.empty());

    std::string partial = codec.Encode(frame);
    partial.resize(minirpc::kProtocolHeaderSize - 1);
    assert(codec.TryDecode(partial, &decoded, &error) == minirpc::DecodeResult::kNeedMoreData);

    std::string invalid = codec.Encode(frame);
    invalid[0] = '\0';
    assert(codec.TryDecode(invalid, &decoded, &error) == minirpc::DecodeResult::kProtocolError);

    minirpc::RpcRequest request;
    request.request_id = 7;
    request.service_name = "EchoService";
    request.method_name = "Echo";
    request.payload.assign("abc\0def", 7);
    auto request_body = minirpc::EncodeRequestBody(request);
    auto decoded_request = minirpc::DecodeRequestBody(request.request_id, request_body);
    assert(decoded_request.request_id == request.request_id);
    assert(decoded_request.service_name == request.service_name);
    assert(decoded_request.method_name == request.method_name);
    assert(decoded_request.payload == request.payload);

    minirpc::RpcResponse response;
    response.request_id = 8;
    response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
    response.payload.assign("xyz\0bin", 7);
    auto response_body = minirpc::EncodeResponseBody(response);
    auto decoded_response = minirpc::DecodeResponseBody(response.request_id, response_body);
    assert(decoded_response.request_id == response.request_id);
    assert(decoded_response.status_code == response.status_code);
    assert(decoded_response.payload == response.payload);

    minirpc::ServiceRegistry registry;
    registry.Register("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.payload = request.payload;
        return response;
    });
    assert(registry.HasService("EchoService"));
    assert(static_cast<bool>(registry.Find("EchoService", "Echo")));
    assert(!registry.HasService("MissingService"));
    assert(!static_cast<bool>(registry.Find("EchoService", "Missing")));

    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
        response.payload = request.payload;
        return response;
    });
    server.RegisterService("SlowService", "Sleep", [](const minirpc::RpcRequest& request) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
        response.payload = "done";
        return response;
    });

    auto status = server.Start({"127.0.0.1", 19000});
    assert(status.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    minirpc::RpcClient client({"127.0.0.1", 19000});
    auto echo1 = client.Call("EchoService", "Echo", "hello", std::chrono::seconds(2));
    assert(echo1.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk));
    assert(echo1.payload == "hello");

    auto echo2 = client.Call("EchoService", "Echo", "again", std::chrono::seconds(2));
    assert(echo2.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk));
    assert(echo2.payload == "again");

    std::vector<std::thread> callers;
    for (int i = 0; i < 8; ++i) {
        callers.emplace_back([&client, i] {
            auto concurrent = client.Call(
                "EchoService", "Echo", "msg-" + std::to_string(i), std::chrono::seconds(2));
            assert(concurrent.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk));
            assert(concurrent.payload == "msg-" + std::to_string(i));
        });
    }
    for (auto& caller : callers) {
        caller.join();
    }

    auto missing_service = client.Call("MissingService", "Echo", "", std::chrono::seconds(2));
    assert(missing_service.status_code == static_cast<int32_t>(minirpc::StatusCode::kServiceNotFound));

    auto missing_method = client.Call("EchoService", "Missing", "", std::chrono::seconds(2));
    assert(missing_method.status_code == static_cast<int32_t>(minirpc::StatusCode::kMethodNotFound));

    auto timeout = client.Call("SlowService", "Sleep", "", std::chrono::milliseconds(20));
    assert(timeout.status_code == static_cast<int32_t>(minirpc::StatusCode::kTimeout));

    client.Close();
    server.Stop();

    return 0;
}
