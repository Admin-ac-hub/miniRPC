#include <cassert>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/core/status.h"
#include "minirpc/net/socket_utils.h"
#include "minirpc/protocol/body_codec.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/runtime/thread_pool.h"
#include "minirpc/server/coroutine_rpc_connection.h"
#include "minirpc/server/service_registry.h"

namespace {

using namespace std::chrono_literals;

class SocketPair {
public:
    SocketPair() {
        const int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_);
        assert(rc == 0);
        std::string error;
        assert(minirpc::SetNonBlocking(fds_[0], &error));
    }

    ~SocketPair() {
        if (fds_[0] != -1) {
            ::close(fds_[0]);
        }
        if (fds_[1] != -1) {
            ::close(fds_[1]);
        }
    }

    int server_fd() const noexcept { return fds_[0]; }
    int client_fd() const noexcept { return fds_[1]; }

    void CloseClient() noexcept {
        if (fds_[1] != -1) {
            ::close(fds_[1]);
            fds_[1] = -1;
        }
    }

private:
    int fds_[2]{-1, -1};
};

minirpc::ProtocolFrame MakeRequestFrame(uint64_t request_id,
                                        const std::string& service,
                                        const std::string& method,
                                        const std::string& payload) {
    minirpc::RpcRequest request;
    request.request_id = request_id;
    request.service_name = service;
    request.method_name = method;
    request.payload = payload;

    minirpc::ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.codec_type = minirpc::CodecType::kProtobuf;
    frame.body = minirpc::EncodeRequestBody(request);
    return frame;
}

std::vector<minirpc::RpcResponse> ClientExchange(int fd, const std::vector<minirpc::ProtocolFrame>& frames) {
    minirpc::RpcCodec codec;
    std::string output;
    for (const auto& frame : frames) {
        output += codec.Encode(frame);
    }
    std::size_t written = 0;
    while (written < output.size()) {
        const ssize_t n = ::write(fd, output.data() + written, output.size() - written);
        assert(n > 0);
        written += static_cast<std::size_t>(n);
    }

    std::vector<minirpc::RpcResponse> responses;
    std::string input;
    char buffer[256];
    while (responses.size() < frames.size()) {
        const ssize_t n = ::read(fd, buffer, sizeof(buffer));
        assert(n > 0);
        input.append(buffer, static_cast<std::size_t>(n));
        while (true) {
            minirpc::ProtocolFrame frame;
            std::string error;
            const minirpc::DecodeResult result = codec.TryDecode(input, &frame, &error);
            if (result == minirpc::DecodeResult::kNeedMoreData) {
                break;
            }
            assert(result == minirpc::DecodeResult::kSuccess);
            assert(frame.message_type == minirpc::MessageType::kResponse);
            responses.push_back(minirpc::DecodeResponseBody(frame.request_id, frame.body));
        }
    }
    return responses;
}

void RegisterEcho(minirpc::ServiceRegistry* registry) {
    registry->Register("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
        response.payload = request.payload;
        return response;
    });
}

void TestCoroutineRpcConnectionEchoAndErrors() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::ServiceRegistry registry;
    RegisterEcho(&registry);
    minirpc::CoroutineRpcConnection connection(&io, &registry);
    minirpc::Status server_status;

    io.Spawn([&] {
        server_status = connection.Serve(sockets.server_fd());
    });

    std::vector<minirpc::RpcResponse> responses;
    std::thread client([&] {
        responses = ClientExchange(sockets.client_fd(), {
            MakeRequestFrame(1, "EchoService", "Echo", "hello"),
            MakeRequestFrame(2, "EchoService", "Missing", ""),
            MakeRequestFrame(3, "MissingService", "Echo", ""),
        });
        sockets.CloseClient();
    });

    io.Run();
    client.join();

    assert(server_status.ok());
    assert(responses.size() == 3);
    assert(responses[0].request_id == 1);
    assert(responses[0].status_code == static_cast<int32_t>(minirpc::StatusCode::kOk));
    assert(responses[0].payload == "hello");
    assert(responses[1].request_id == 2);
    assert(responses[1].status_code == static_cast<int32_t>(minirpc::StatusCode::kMethodNotFound));
    assert(responses[2].request_id == 3);
    assert(responses[2].status_code == static_cast<int32_t>(minirpc::StatusCode::kServiceNotFound));
}

void TestCoroutineRpcConnectionRejectsNonRequestFrame() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::ServiceRegistry registry;
    minirpc::CoroutineRpcConnection connection(&io, &registry);
    minirpc::Status server_status;

    io.Spawn([&] {
        server_status = connection.Serve(sockets.server_fd());
    });

    std::thread client([&] {
        minirpc::RpcCodec codec;
        minirpc::ProtocolFrame frame;
        frame.request_id = 9;
        frame.message_type = minirpc::MessageType::kResponse;
        const std::string output = codec.Encode(frame);
        const ssize_t n = ::write(sockets.client_fd(), output.data(), output.size());
        assert(n == static_cast<ssize_t>(output.size()));
        sockets.CloseClient();
    });

    io.Run();
    client.join();

    assert(!server_status.ok());
    assert(server_status.code() == minirpc::StatusCode::kProtocolError);
}

void TestCoroutineRpcConnectionDispatchesHandlerToThreadPool() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::ServiceRegistry registry;
    minirpc::ThreadPool thread_pool(1, 8);
    thread_pool.Start();

    std::vector<int> events;
    std::mutex events_mutex;
    auto push_event = [&](int event) {
        std::lock_guard<std::mutex> lock(events_mutex);
        events.push_back(event);
    };

    registry.Register("SlowService", "Work", [&](const minirpc::RpcRequest& request) {
        push_event(2);
        std::this_thread::sleep_for(50ms);
        push_event(4);
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
        response.payload = "done";
        return response;
    });

    minirpc::CoroutineRpcConnection connection(&io, &registry, &thread_pool);
    minirpc::Status server_status;

    io.Spawn([&] {
        server_status = connection.Serve(sockets.server_fd());
    });
    io.Spawn([&] {
        assert(io.timers().SleepFor(10ms));
        push_event(3);
    });

    std::vector<minirpc::RpcResponse> responses;
    std::thread client([&] {
        responses = ClientExchange(sockets.client_fd(), {
            MakeRequestFrame(11, "SlowService", "Work", ""),
        });
        sockets.CloseClient();
    });

    io.Run();
    client.join();
    thread_pool.Stop();

    assert(server_status.ok());
    assert(responses.size() == 1);
    assert(responses[0].status_code == static_cast<int32_t>(minirpc::StatusCode::kOk));
    assert(responses[0].payload == "done");

    std::lock_guard<std::mutex> lock(events_mutex);
    auto timer_it = std::find(events.begin(), events.end(), 3);
    auto done_it = std::find(events.begin(), events.end(), 4);
    assert(std::find(events.begin(), events.end(), 2) != events.end());
    assert(timer_it != events.end());
    assert(done_it != events.end());
    assert(timer_it < done_it);
}

}  // namespace

int main() {
    TestCoroutineRpcConnectionEchoAndErrors();
    TestCoroutineRpcConnectionRejectsNonRequestFrame();
    TestCoroutineRpcConnectionDispatchesHandlerToThreadPool();
    return 0;
}
