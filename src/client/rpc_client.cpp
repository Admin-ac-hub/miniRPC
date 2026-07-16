#include "minirpc/client/rpc_client.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <thread>
#include <utility>
#include <vector>

#include "minirpc/protocol/body_codec.h"

namespace minirpc {
namespace {

int64_t DeadlineUnixMs(std::chrono::milliseconds timeout) {
    const auto now = std::chrono::system_clock::now();
    const auto deadline = now + timeout;
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               deadline.time_since_epoch())
        .count();
}

}  // namespace

RpcClient::RpcClient(Endpoint endpoint)
    : endpoint_(std::move(endpoint)),
      next_request_id_(1),
      stopping_(false) {
    tcp_client_.SetOnFrame([this](ProtocolFrame frame) { OnFrame(std::move(frame)); });
    tcp_client_.SetOnClose([this](TcpClient::CloseReason r, const std::string& msg) { OnClose(r, msg); });
    timeout_thread_ = std::thread([this] { TimeoutCleanerLoop(); });
}

RpcClient::~RpcClient() {
    stopping_.store(true, std::memory_order_release);
    pending_cv_.notify_all();
    Close();
    if (timeout_thread_.joinable()) {
        timeout_thread_.join();
    }
}

Status RpcClient::Connect() { return tcp_client_.Connect(endpoint_); }

void RpcClient::Close() { tcp_client_.Close(); }

void RpcClient::SetOptions(const RpcClientOptions& options) {
    std::lock_guard<std::mutex> lock(options_mutex_);
    options_ = options;
}

RpcResponse RpcClient::Call(const std::string& service_name,
                            const std::string& method_name,
                            const std::string& payload,
                            std::chrono::milliseconds timeout) {
    return CallAsync(service_name, method_name, payload, timeout).get();
}

std::future<RpcResponse> RpcClient::CallAsync(const std::string& service_name,
                                              const std::string& method_name,
                                              const std::string& payload,
                                              std::chrono::milliseconds timeout) {
    const uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
    auto pending = std::make_shared<PendingCall>();
    auto future = pending->promise.get_future();
    if (timeout <= std::chrono::milliseconds::zero()) {
        pending->TryComplete(ErrorResponse(request_id, StatusCode::kTimeout, "rpc call timed out"));
        return future;
    }

    const auto expire_time = std::chrono::steady_clock::now() + timeout;
    pending->expire_time = expire_time;

    const Status connected = ConnectWithRetry(expire_time);
    if (!connected.ok()) {
        pending->TryComplete(ErrorResponse(request_id, connected.code(), connected.message()));
        return future;
    }

    RpcRequest request;
    request.request_id = request_id;
    request.service_name = service_name;
    request.method_name = method_name;
    request.payload = payload;
    request.deadline_unix_ms = DeadlineUnixMs(timeout);

    ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = MessageType::kRequest;
    frame.codec_type = CodecType::kProtobuf;
    try {
        frame.body = EncodeRequestBody(request);
    } catch (const std::exception& ex) {
        pending->TryComplete(ErrorResponse(request_id, StatusCode::kSerializeError, ex.what()));
        return future;
    }

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_[request_id] = pending;
    }
    pending_cv_.notify_one();

    Status send_status = tcp_client_.SendFrame(frame);
    if (!send_status.ok()) {
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.erase(request_id);
        }
        pending_cv_.notify_one();
        pending->TryComplete(ErrorResponse(request_id, send_status.code(), send_status.message()));
        tcp_client_.Close();
        return future;
    }

    return future;
}

RpcClientOptions RpcClient::OptionsSnapshot() const {
    std::lock_guard<std::mutex> lock(options_mutex_);
    return options_;
}

Status RpcClient::ConnectWithRetry(std::chrono::steady_clock::time_point expire_time) {
    const RpcClientOptions options = OptionsSnapshot();
    Status last_status = Status::Error(StatusCode::kNetworkError, "connect not attempted");
    for (std::size_t attempt = 0; attempt <= options.max_reconnect_attempts; ++attempt) {
        if (std::chrono::steady_clock::now() >= expire_time) {
            return Status::Error(StatusCode::kTimeout, "rpc call timed out before reconnect");
        }
        last_status = Connect();
        if (last_status.ok()) {
            return last_status;
        }
        if (attempt == options.max_reconnect_attempts) {
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        const auto remaining = expire_time - now;
        if (remaining <= std::chrono::steady_clock::duration::zero()) {
            return Status::Error(StatusCode::kTimeout, "rpc call timed out before reconnect");
        }
        const auto sleep_for = std::min(remaining,
                                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                            options.reconnect_interval));
        std::this_thread::sleep_for(sleep_for);
    }
    return last_status;
}

void RpcClient::OnFrame(ProtocolFrame frame) {
    if (frame.message_type != MessageType::kResponse) {
        FailPending(StatusCode::kProtocolError, "unexpected non-response frame");
        tcp_client_.Close();
        return;
    }
    RpcResponse response;
    try {
        response = DecodeResponseBody(frame.request_id, frame.body);
    } catch (const std::exception& ex) {
        response = ErrorResponse(frame.request_id, StatusCode::kDeserializeError, ex.what());
    }
    std::shared_ptr<PendingCall> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_.find(frame.request_id);
        if (it != pending_.end()) {
            pending = std::move(it->second);
            pending_.erase(it);
        }
    }
    if (!pending) {
        return;
    }
    if (std::chrono::steady_clock::now() >= pending->expire_time) {
        pending->TryComplete(ErrorResponse(frame.request_id, StatusCode::kTimeout, "rpc call timed out"));
        return;
    }
    pending->TryComplete(std::move(response));
}

void RpcClient::OnClose(TcpClient::CloseReason, const std::string& message) {
    FailPending(StatusCode::kNetworkError,
                message.empty() ? std::string("connection closed by peer") : message);
}

void RpcClient::FailPending(StatusCode code, const std::string& message) {
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> drained;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        drained.swap(pending_);
    }
    pending_cv_.notify_all();
    for (auto& [request_id, pending] : drained) {
        pending->TryComplete(ErrorResponse(request_id, code, message));
    }
}

void RpcClient::TimeoutCleanerLoop() {
    while (true) {
        std::vector<std::pair<uint64_t, std::shared_ptr<PendingCall>>> expired;
        {
            std::unique_lock<std::mutex> lock(pending_mutex_);
            pending_cv_.wait(lock, [this] {
                return stopping_.load(std::memory_order_acquire) || !pending_.empty();
            });
            if (stopping_.load(std::memory_order_acquire)) {
                return;
            }

            while (!pending_.empty()) {
                const auto next = std::min_element(
                    pending_.begin(), pending_.end(),
                    [](const auto& lhs, const auto& rhs) {
                        return lhs.second->expire_time < rhs.second->expire_time;
                    });
                const auto next_expire_time = next->second->expire_time;
                const auto now = std::chrono::steady_clock::now();
                if (now < next_expire_time) {
                    pending_cv_.wait_until(lock, next_expire_time);
                    if (stopping_.load(std::memory_order_acquire)) {
                        return;
                    }
                    continue;
                }

                for (auto it = pending_.begin(); it != pending_.end();) {
                    if (now >= it->second->expire_time) {
                        expired.emplace_back(it->first, std::move(it->second));
                        it = pending_.erase(it);
                    } else {
                        ++it;
                    }
                }
                break;
            }
        }
        for (auto& [request_id, pending] : expired) {
            pending->TryComplete(ErrorResponse(request_id, StatusCode::kTimeout, "rpc call timed out"));
        }
    }
}

RpcResponse RpcClient::ErrorResponse(uint64_t request_id,
                                     StatusCode code,
                                     const std::string& message) const {
    RpcResponse response;
    response.request_id = request_id;
    response.status_code = static_cast<int32_t>(code);
    response.error_message = message;
    return response;
}

RpcClientPool::RpcClientPool(Endpoint endpoint, std::size_t size, RpcClientOptions options) {
    const std::size_t pool_size = size == 0 ? 1 : size;
    clients_.reserve(pool_size);
    for (std::size_t i = 0; i < pool_size; ++i) {
        auto client = std::make_unique<RpcClient>(endpoint);
        client->SetOptions(options);
        clients_.push_back(std::move(client));
    }
}

Status RpcClientPool::Connect() {
    for (auto& client : clients_) {
        const Status status = client->Connect();
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

void RpcClientPool::Close() {
    for (auto& client : clients_) {
        client->Close();
    }
}

RpcResponse RpcClientPool::Call(const std::string& service_name,
                                const std::string& method_name,
                                const std::string& payload,
                                std::chrono::milliseconds timeout) {
    return NextClient().Call(service_name, method_name, payload, timeout);
}

std::future<RpcResponse> RpcClientPool::CallAsync(const std::string& service_name,
                                                  const std::string& method_name,
                                                  const std::string& payload,
                                                  std::chrono::milliseconds timeout) {
    return NextClient().CallAsync(service_name, method_name, payload, timeout);
}

RpcClient& RpcClientPool::NextClient() {
    const std::size_t index = next_index_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
    return *clients_[index];
}

}  // namespace minirpc
