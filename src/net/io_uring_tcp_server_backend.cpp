#include "minirpc/net/tcp_server_backend.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <poll.h>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <liburing.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/net/socket_utils.h"
#include "minirpc/observability/logger.h"
#include "minirpc/protocol/codec.h"

namespace minirpc {
namespace {

constexpr unsigned kRingEntries = 256;
constexpr std::size_t kReadChunkSize = 4096;
thread_local const void* current_io_uring_reactor = nullptr;

void CloseFd(int* fd) {
    if (*fd != -1) {
        ::close(*fd);
        *fd = -1;
    }
}

void CompleteWrite(std::function<void(Status)>* completion, Status status) {
    if (!*completion) {
        return;
    }
    std::function<void(Status)> callback = std::move(*completion);
    try {
        callback(std::move(status));
    } catch (...) {
        Logger::Log(LogLevel::kError, "write completion callback threw");
    }
}

std::string LiburingError(const char* action, int rc) {
    std::ostringstream message;
    message << action << ": " << std::strerror(-rc);
    return message.str();
}

bool IsTransientAcceptError(int error) {
    return error == EAGAIN || error == EINTR || error == ECONNABORTED || error == EPROTO;
}

bool IsResourceAcceptError(int error) {
    return error == EMFILE || error == ENFILE || error == ENOBUFS || error == ENOMEM;
}

}  // namespace

class IoUringTcpServerBackend final : public TcpServerBackend {
public:
    using OnOpenFn = TcpServer::OnOpenFn;
    using OnFrameFn = TcpServer::OnFrameFn;
    using OnCloseFn = TcpServer::OnCloseFn;
    using OnBackpressureFn = TcpServer::OnBackpressureFn;

    IoUringTcpServerBackend();
    ~IoUringTcpServerBackend() override;

    IoUringTcpServerBackend(const IoUringTcpServerBackend&) = delete;
    IoUringTcpServerBackend& operator=(const IoUringTcpServerBackend&) = delete;

    void SetOnOpen(OnOpenFn fn) override;
    void SetOnFrame(OnFrameFn fn) override;
    void SetOnClose(OnCloseFn fn) override;
    void SetOnBackpressure(OnBackpressureFn fn) override;
    void SetOptions(const TcpServerOptions& opts) override;

    Status Start(const Endpoint& endpoint) override;
    void Stop() override;
    void StopAccepting() override;
    bool running() const override;

    Status SendFrame(ConnectionId conn_id,
                     uint64_t generation,
                     const ProtocolFrame& frame,
                     bool close_after_send,
                     std::function<void(Status)> completion) override;
    void CloseConnection(ConnectionId conn_id, uint64_t generation) override;

private:
    enum class OpKind {
        kAccept,
        kWakePoll,
        kRecv,
        kSend,
        kCancel
    };

    struct PendingWrite {
        std::string data;
        std::size_t offset = 0;
        std::function<void(Status)> completion;
    };

    struct Operation {
        uint64_t token = 0;
        OpKind kind = OpKind::kAccept;
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
        uint64_t target_token = 0;
        std::array<char, kReadChunkSize> recv_buffer{};
        PendingWrite write;
    };

    struct Connection {
        ConnectionId id = 0;
        int fd = -1;
        uint64_t generation = 0;
        std::string read_buffer;
        std::deque<PendingWrite> write_buffers;
        std::size_t write_buffer_bytes = 0;
        uint64_t recv_token = 0;
        uint64_t send_token = 0;
        bool graceful_closing = false;
        bool hard_closing = false;
        bool in_backpressure = false;
    };

    struct QueuedResponse {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
        std::string data;
        bool close_after_send = false;
        std::function<void(Status)> completion;
    };

    struct QueuedClose {
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
    };

    struct CancelRequest {
        uint64_t target_token = 0;
        ConnectionId conn_id = 0;
        uint64_t generation = 0;
    };

    Status SetupListener();
    Status SetupRing();
    Status ProbeRing(const io_uring_params& params);
    void CleanupAfterJoin();
    void RunEventLoop();
    void ApplyControlState();
    void SchedulePendingOperations();
    void ReapReadyCompletions();
    void BeginFatalShutdown();
    bool SubmitAccept();
    bool SubmitWakePoll();
    bool SubmitRecv(Connection& conn);
    bool SubmitSend(Connection& conn);
    void SubmitPendingCancels();
    void RequestCancel(uint64_t target_token, ConnectionId conn_id, uint64_t generation);
    void HandleCompletion(uint64_t token, int result);
    void HandleAccept(Operation& operation, int result);
    void HandleWakePoll(Operation& operation, int result);
    void HandleRecv(Operation& operation, int result);
    void HandleSend(Operation& operation, int result);
    void HandleCancel(Operation& operation, int result);
    void DrainWakeEvents();
    void DrainResponses();
    void DrainCloseRequests();
    void WakeEventLoop();
    void CloseListenFd();
    Connection* FindById(ConnectionId conn_id);
    bool MatchesGeneration(ConnectionId conn_id, uint64_t generation) const;
    void BeginClose(ConnectionId conn_id);
    void FailPendingWrites(Connection& conn, const char* message);
    void FailQueuedResponses(const char* message);
    void MaybeFinalizeConnection(ConnectionId conn_id);
    bool HasOperationsForConnection(ConnectionId conn_id, uint64_t generation) const;
    void FinalizeConnection(ConnectionId conn_id);
    void SetBackpressure(Connection& conn, bool enabled);
    void MaybeEnterBackpressure(Connection& conn);
    void MaybeLeaveBackpressure(Connection& conn);
    uint64_t NextOperationToken();

    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_{false};
    Endpoint endpoint_;
    TcpServerOptions options_;
    RpcCodec codec_;
    OnOpenFn on_open_;
    OnFrameFn on_frame_;
    OnCloseFn on_close_;
    OnBackpressureFn on_backpressure_;

    std::thread reactor_thread_;
    int listen_fd_ = -1;
    int wake_fd_ = -1;
    io_uring ring_{};
    bool ring_initialized_ = false;
    bool shutdown_started_ = false;
    bool event_loop_failed_ = false;

    std::unordered_map<int, ConnectionId> connection_by_fd_;
    std::unordered_map<ConnectionId, Connection> connections_;
    ConnectionId next_connection_id_ = 1;
    uint64_t next_generation_ = 1;

    std::mutex response_mutex_;
    std::mutex wake_mutex_;
    std::vector<QueuedResponse> response_queue_;
    std::vector<QueuedClose> close_queue_;

    std::unordered_map<uint64_t, std::unique_ptr<Operation>> operations_;
    std::deque<CancelRequest> pending_cancels_;
    std::unordered_set<uint64_t> cancel_requested_tokens_;
    uint64_t next_operation_token_ = 1;
    uint64_t accept_token_ = 0;
    uint64_t wake_token_ = 0;
};

IoUringTcpServerBackend::IoUringTcpServerBackend() = default;

IoUringTcpServerBackend::~IoUringTcpServerBackend() { Stop(); }

void IoUringTcpServerBackend::SetOnOpen(OnOpenFn fn) { on_open_ = std::move(fn); }
void IoUringTcpServerBackend::SetOnFrame(OnFrameFn fn) { on_frame_ = std::move(fn); }
void IoUringTcpServerBackend::SetOnClose(OnCloseFn fn) { on_close_ = std::move(fn); }
void IoUringTcpServerBackend::SetOnBackpressure(OnBackpressureFn fn) {
    on_backpressure_ = std::move(fn);
}
void IoUringTcpServerBackend::SetOptions(const TcpServerOptions& opts) { options_ = opts; }

Status IoUringTcpServerBackend::Start(const Endpoint& endpoint) {
    if (running_.load(std::memory_order_acquire)) {
        return Status::Ok();
    }
    if (reactor_thread_.joinable()) {
        if (current_io_uring_reactor == this) {
            return Status::Error(StatusCode::kNetworkError,
                                 "cannot restart io_uring reactor from its callback");
        }
        reactor_thread_.join();
        CleanupAfterJoin();
    }

    endpoint_ = endpoint;
    shutdown_started_ = false;
    event_loop_failed_ = false;

    Status status = SetupListener();
    if (!status.ok()) {
        CleanupAfterJoin();
        return status;
    }
    status = SetupRing();
    if (!status.ok()) {
        CleanupAfterJoin();
        return status;
    }

    accepting_.store(true, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    try {
        reactor_thread_ = std::thread([this] {
            current_io_uring_reactor = this;
            RunEventLoop();
            current_io_uring_reactor = nullptr;
        });
    } catch (const std::system_error& error) {
        running_.store(false, std::memory_order_release);
        accepting_.store(false, std::memory_order_release);
        CleanupAfterJoin();
        return Status::Error(StatusCode::kNetworkError,
                             std::string("failed to start io_uring reactor thread: ") + error.what());
    }
    return Status::Ok();
}

void IoUringTcpServerBackend::Stop() {
    running_.store(false, std::memory_order_release);
    accepting_.store(false, std::memory_order_release);
    WakeEventLoop();

    if (reactor_thread_.joinable()) {
        if (current_io_uring_reactor == this) {
            return;
        }
        reactor_thread_.join();
    }
    CleanupAfterJoin();
}

void IoUringTcpServerBackend::StopAccepting() {
    accepting_.store(false, std::memory_order_release);
    WakeEventLoop();
}

bool IoUringTcpServerBackend::running() const {
    return running_.load(std::memory_order_acquire);
}

Status IoUringTcpServerBackend::SendFrame(ConnectionId conn_id,
                                          uint64_t generation,
                                          const ProtocolFrame& frame,
                                          bool close_after_send,
                                          std::function<void(Status)> completion) {
    if (!running_.load(std::memory_order_acquire)) {
        return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
    }

    QueuedResponse response;
    response.conn_id = conn_id;
    response.generation = generation;
    response.data = codec_.Encode(frame);
    response.close_after_send = close_after_send;
    response.completion = std::move(completion);

    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (!running_.load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kNetworkError, "TcpServer is stopped");
        }
        if (response_queue_.size() >= options_.max_response_queue) {
            close_queue_.push_back({conn_id, generation});
            overflow = true;
        } else {
            response_queue_.push_back(std::move(response));
        }
    }
    WakeEventLoop();
    if (overflow) {
        return Status::Error(StatusCode::kServerError, "response queue full");
    }
    return Status::Ok();
}

void IoUringTcpServerBackend::CloseConnection(ConnectionId conn_id, uint64_t generation) {
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        if (!running_.load(std::memory_order_acquire)) {
            return;
        }
        close_queue_.push_back({conn_id, generation});
    }
    WakeEventLoop();
}

Status IoUringTcpServerBackend::SetupListener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("socket failed", errno));
    }

    const int enabled = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) == -1) {
        return Status::Error(StatusCode::kNetworkError,
                             LastSocketError("setsockopt(SO_REUSEADDR) failed", errno));
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint_.port);
    if (::inet_pton(AF_INET, endpoint_.host.c_str(), &address.sin_addr) != 1) {
        return Status::Error(StatusCode::kNetworkError, "invalid listen host: " + endpoint_.host);
    }
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("bind failed", errno));
    }
    if (::listen(listen_fd_, options_.backlog) == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("listen failed", errno));
    }

    const int wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd == -1) {
        return Status::Error(StatusCode::kNetworkError, LastSocketError("eventfd failed", errno));
    }
    {
        std::lock_guard<std::mutex> lock(wake_mutex_);
        CloseFd(&wake_fd_);
        wake_fd_ = wake_fd;
    }
    return Status::Ok();
}

Status IoUringTcpServerBackend::SetupRing() {
    ring_ = {};
    io_uring_params params{};
    const int rc = io_uring_queue_init_params(kRingEntries, &ring_, &params);
    if (rc < 0) {
        return Status::Error(StatusCode::kNetworkError,
                             LiburingError("io_uring_queue_init_params failed", rc));
    }
    ring_initialized_ = true;
    return ProbeRing(params);
}

Status IoUringTcpServerBackend::ProbeRing(const io_uring_params& params) {
    constexpr unsigned kRequiredFeatures = IORING_FEAT_NODROP | IORING_FEAT_FAST_POLL;
    if ((params.features & kRequiredFeatures) != kRequiredFeatures) {
        return Status::Error(StatusCode::kNetworkError,
                             "io_uring requires IORING_FEAT_NODROP and IORING_FEAT_FAST_POLL");
    }

    io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
    if (probe == nullptr) {
        return Status::Error(StatusCode::kNetworkError, "io_uring opcode probe failed");
    }

    const unsigned required_opcodes[] = {
        IORING_OP_ACCEPT,
        IORING_OP_RECV,
        IORING_OP_SEND,
        IORING_OP_POLL_ADD,
        IORING_OP_ASYNC_CANCEL,
    };
    for (unsigned opcode : required_opcodes) {
        if (io_uring_opcode_supported(probe, static_cast<int>(opcode)) == 0) {
            io_uring_free_probe(probe);
            std::ostringstream message;
            message << "io_uring opcode is not supported: " << opcode;
            return Status::Error(StatusCode::kNetworkError, message.str());
        }
    }
    io_uring_free_probe(probe);
    return Status::Ok();
}

void IoUringTcpServerBackend::CleanupAfterJoin() {
    if (ring_initialized_) {
        if (event_loop_failed_) {
            ReapReadyCompletions();
        }
        io_uring_queue_exit(&ring_);
        ring_initialized_ = false;
        ring_ = {};
    }

    for (auto& entry : operations_) {
        Operation& operation = *entry.second;
        if (operation.kind == OpKind::kSend) {
            CompleteWrite(
                &operation.write.completion,
                Status::Error(StatusCode::kNetworkError,
                              "TcpServer stopped before response was sent"));
        }
    }
    operations_.clear();
    pending_cancels_.clear();
    cancel_requested_tokens_.clear();
    accept_token_ = 0;
    wake_token_ = 0;

    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
        ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
        FinalizeConnection(id);
    }
    connection_by_fd_.clear();

    CloseListenFd();
    {
        std::lock_guard<std::mutex> lock(wake_mutex_);
        CloseFd(&wake_fd_);
    }

    FailQueuedResponses("TcpServer stopped before response was sent");
}

void IoUringTcpServerBackend::RunEventLoop() {
    while (true) {
        ApplyControlState();
        SchedulePendingOperations();

        if (event_loop_failed_) {
            break;
        }
        if (!running_.load(std::memory_order_acquire) && !shutdown_started_) {
            continue;
        }

        if (shutdown_started_ && operations_.empty() && pending_cancels_.empty()) {
            break;
        }

        const int rc = io_uring_submit_and_wait(&ring_, 1);
        if (rc < 0) {
            if (rc == -EINTR) {
                continue;
            }
            Logger::Log(LogLevel::kError, LiburingError("io_uring_submit_and_wait failed", rc));
            BeginFatalShutdown();
            ReapReadyCompletions();
            break;
        }

        ReapReadyCompletions();

        if (event_loop_failed_) {
            break;
        }

        if (running_.load(std::memory_order_acquire)) {
            DrainResponses();
        }
    }

    CleanupAfterJoin();
}

void IoUringTcpServerBackend::ApplyControlState() {
    if (!accepting_.load(std::memory_order_acquire)) {
        if (accept_token_ != 0) {
            RequestCancel(accept_token_, 0, 0);
        } else {
            CloseListenFd();
        }
    }

    if (running_.load(std::memory_order_acquire) || shutdown_started_) {
        return;
    }

    shutdown_started_ = true;
    accepting_.store(false, std::memory_order_release);
    if (accept_token_ != 0) {
        RequestCancel(accept_token_, 0, 0);
    }
    if (wake_token_ != 0) {
        RequestCancel(wake_token_, 0, 0);
    }

    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
        ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
        BeginClose(id);
    }
}

void IoUringTcpServerBackend::SchedulePendingOperations() {
    SubmitPendingCancels();

    if (running_.load(std::memory_order_acquire)) {
        if (wake_fd_ != -1 && wake_token_ == 0) {
            if (!SubmitWakePoll()) {
                return;
            }
        }
        if (running_.load(std::memory_order_acquire) &&
            accepting_.load(std::memory_order_acquire) && listen_fd_ != -1 &&
            accept_token_ == 0) {
            (void)SubmitAccept();
        }
    }

    if (!running_.load(std::memory_order_acquire)) {
        return;
    }

    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
        ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
        if (!running_.load(std::memory_order_acquire)) {
            return;
        }
        Connection* conn = FindById(id);
        if (conn == nullptr || conn->hard_closing) {
            continue;
        }
        if (!conn->graceful_closing && !conn->in_backpressure && conn->recv_token == 0) {
            (void)SubmitRecv(*conn);
        }
        conn = FindById(id);
        if (conn != nullptr && !conn->hard_closing && conn->send_token == 0 &&
            !conn->write_buffers.empty()) {
            (void)SubmitSend(*conn);
        }
    }
}

void IoUringTcpServerBackend::ReapReadyCompletions() {
    while (true) {
        unsigned head = 0;
        unsigned completed = 0;
        io_uring_cqe* cqe = nullptr;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            HandleCompletion(io_uring_cqe_get_data64(cqe), cqe->res);
            ++completed;
        }
        if (completed == 0) {
            return;
        }
        io_uring_cq_advance(&ring_, completed);
    }
}

void IoUringTcpServerBackend::BeginFatalShutdown() {
    event_loop_failed_ = true;
    running_.store(false, std::memory_order_release);
    accepting_.store(false, std::memory_order_release);
    CloseListenFd();

    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
        ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
        BeginClose(id);
    }
}

bool IoUringTcpServerBackend::SubmitAccept() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
        return false;
    }

    auto operation = std::make_unique<Operation>();
    const uint64_t token = NextOperationToken();
    if (token == 0) {
        return false;
    }
    operation->token = token;
    operation->kind = OpKind::kAccept;
    operations_.emplace(token, std::move(operation));

    io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, token);
    accept_token_ = token;
    return true;
}

bool IoUringTcpServerBackend::SubmitWakePoll() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
        return false;
    }

    auto operation = std::make_unique<Operation>();
    const uint64_t token = NextOperationToken();
    if (token == 0) {
        return false;
    }
    operation->token = token;
    operation->kind = OpKind::kWakePoll;
    operations_.emplace(token, std::move(operation));

    io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
    io_uring_sqe_set_data64(sqe, token);
    wake_token_ = token;
    return true;
}

bool IoUringTcpServerBackend::SubmitRecv(Connection& conn) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
        return false;
    }

    auto operation = std::make_unique<Operation>();
    const uint64_t token = NextOperationToken();
    if (token == 0) {
        return false;
    }
    operation->token = token;
    operation->kind = OpKind::kRecv;
    operation->conn_id = conn.id;
    operation->generation = conn.generation;
    Operation* raw = operation.get();
    operations_.emplace(token, std::move(operation));

    io_uring_prep_recv(sqe, conn.fd, raw->recv_buffer.data(), raw->recv_buffer.size(), 0);
    io_uring_sqe_set_data64(sqe, token);
    conn.recv_token = token;
    return true;
}

bool IoUringTcpServerBackend::SubmitSend(Connection& conn) {
    if (conn.write_buffers.empty()) {
        return true;
    }

    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
        return false;
    }

    auto operation = std::make_unique<Operation>();
    const uint64_t token = NextOperationToken();
    if (token == 0) {
        return false;
    }
    operation->token = token;
    operation->kind = OpKind::kSend;
    operation->conn_id = conn.id;
    operation->generation = conn.generation;
    operation->write = std::move(conn.write_buffers.front());
    conn.write_buffers.pop_front();

    Operation* raw = operation.get();
    operations_.emplace(token, std::move(operation));

    const std::size_t remaining = raw->write.data.size() - raw->write.offset;
    io_uring_prep_send(sqe,
                       conn.fd,
                       raw->write.data.data() + raw->write.offset,
                       remaining,
                       MSG_NOSIGNAL);
    io_uring_sqe_set_data64(sqe, token);
    conn.send_token = token;
    return true;
}

void IoUringTcpServerBackend::SubmitPendingCancels() {
    while (!pending_cancels_.empty()) {
        const CancelRequest request = pending_cancels_.front();
        if (operations_.find(request.target_token) == operations_.end()) {
            cancel_requested_tokens_.erase(request.target_token);
            pending_cancels_.pop_front();
            if (request.conn_id != 0) {
                MaybeFinalizeConnection(request.conn_id);
            }
            continue;
        }

        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe == nullptr) {
            return;
        }

        auto operation = std::make_unique<Operation>();
        const uint64_t token = NextOperationToken();
        if (token == 0) {
            return;
        }
        operation->token = token;
        operation->kind = OpKind::kCancel;
        operation->conn_id = request.conn_id;
        operation->generation = request.generation;
        operation->target_token = request.target_token;
        operations_.emplace(token, std::move(operation));

        io_uring_prep_cancel64(sqe, request.target_token, 0);
        io_uring_sqe_set_data64(sqe, token);
        pending_cancels_.pop_front();
    }
}

void IoUringTcpServerBackend::RequestCancel(uint64_t target_token,
                                            ConnectionId conn_id,
                                            uint64_t generation) {
    if (target_token == 0 || operations_.find(target_token) == operations_.end()) {
        return;
    }
    if (!cancel_requested_tokens_.insert(target_token).second) {
        return;
    }
    pending_cancels_.push_back({target_token, conn_id, generation});
}

void IoUringTcpServerBackend::HandleCompletion(uint64_t token, int result) {
    const auto it = operations_.find(token);
    if (it == operations_.end()) {
        return;
    }

    std::unique_ptr<Operation> operation = std::move(it->second);
    operations_.erase(it);
    cancel_requested_tokens_.erase(token);

    switch (operation->kind) {
        case OpKind::kAccept:
            HandleAccept(*operation, result);
            break;
        case OpKind::kWakePoll:
            HandleWakePoll(*operation, result);
            break;
        case OpKind::kRecv:
            HandleRecv(*operation, result);
            break;
        case OpKind::kSend:
            HandleSend(*operation, result);
            break;
        case OpKind::kCancel:
            HandleCancel(*operation, result);
            break;
    }
}

void IoUringTcpServerBackend::HandleAccept(Operation& operation, int result) {
    if (accept_token_ != operation.token) {
        if (result >= 0) {
            ::close(result);
        }
        return;
    }
    accept_token_ = 0;

    if (result >= 0) {
        if (!running_.load(std::memory_order_acquire) ||
            !accepting_.load(std::memory_order_acquire)) {
            ::close(result);
            return;
        }

        const ConnectionId id = next_connection_id_++;
        const uint64_t generation = next_generation_++;
        Connection conn;
        conn.id = id;
        conn.fd = result;
        conn.generation = generation;
        connection_by_fd_[result] = id;
        connections_.emplace(id, std::move(conn));
        if (on_open_) {
            on_open_(id, generation);
        }
        return;
    }

    const int error = -result;
    if (result == -ECANCELED || !running_.load(std::memory_order_acquire) ||
        !accepting_.load(std::memory_order_acquire)) {
        return;
    }
    if (IsResourceAcceptError(error)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return;
    }
    if (!IsTransientAcceptError(error)) {
        Logger::Log(LogLevel::kWarn, LiburingError("io_uring accept failed", result));
    }
}

void IoUringTcpServerBackend::HandleWakePoll(Operation& operation, int result) {
    if (wake_token_ != operation.token) {
        return;
    }
    wake_token_ = 0;
    if (result >= 0) {
        DrainWakeEvents();
        if (running_.load(std::memory_order_acquire)) {
            DrainResponses();
        }
        return;
    }
    if (result != -ECANCELED) {
        Logger::Log(LogLevel::kError, LiburingError("io_uring eventfd poll failed", result));
        running_.store(false, std::memory_order_release);
    }
}

void IoUringTcpServerBackend::HandleRecv(Operation& operation, int result) {
    Connection* conn = FindById(operation.conn_id);
    if (conn == nullptr || conn->generation != operation.generation ||
        conn->recv_token != operation.token) {
        return;
    }
    conn->recv_token = 0;
    if (conn->hard_closing) {
        MaybeFinalizeConnection(conn->id);
        return;
    }

    if (result > 0) {
        conn->read_buffer.append(operation.recv_buffer.data(), static_cast<std::size_t>(result));

        const int client_fd = conn->fd;
        while (true) {
            ProtocolFrame frame;
            std::string error;
            const DecodeResult decode_result = codec_.TryDecode(conn->read_buffer, &frame, &error);
            if (decode_result == DecodeResult::kNeedMoreData) {
                if (conn->read_buffer.size() > options_.max_read_buffer_bytes) {
                    BeginClose(conn->id);
                }
                break;
            }
            if (decode_result == DecodeResult::kProtocolError) {
                BeginClose(operation.conn_id);
                return;
            }

            const ConnectionId id = conn->id;
            const uint64_t generation = conn->generation;
            if (on_frame_) {
                on_frame_(id, generation, std::move(frame));
            }
            const auto fd_it = connection_by_fd_.find(client_fd);
            if (fd_it == connection_by_fd_.end()) {
                return;
            }
            conn = FindById(fd_it->second);
            if (conn == nullptr || conn->generation != generation) {
                return;
            }
        }
        return;
    }

    if (result == -EAGAIN || result == -EINTR) {
        return;
    }
    if (result == -ECANCELED && conn->hard_closing) {
        MaybeFinalizeConnection(conn->id);
        return;
    }
    BeginClose(conn->id);
}

void IoUringTcpServerBackend::HandleSend(Operation& operation, int result) {
    Connection* conn = FindById(operation.conn_id);
    if (conn == nullptr || conn->generation != operation.generation ||
        conn->send_token != operation.token) {
        CompleteWrite(
            &operation.write.completion,
            Status::Error(StatusCode::kNetworkError,
                          "connection changed before response was sent"));
        return;
    }
    conn->send_token = 0;
    if (conn->hard_closing) {
        CompleteWrite(
            &operation.write.completion,
            Status::Error(StatusCode::kNetworkError,
                          "connection closed before response was sent"));
        MaybeFinalizeConnection(conn->id);
        return;
    }

    const std::size_t remaining = operation.write.data.size() - operation.write.offset;
    if (result > 0 && static_cast<std::size_t>(result) <= remaining) {
        const std::size_t sent = static_cast<std::size_t>(result);
        conn->write_buffer_bytes -= sent;
        operation.write.offset += sent;
        if (operation.write.offset < operation.write.data.size()) {
            conn->write_buffers.push_front(std::move(operation.write));
        } else {
            CompleteWrite(&operation.write.completion, Status::Ok());
        }
        MaybeLeaveBackpressure(*conn);
    } else if (result == -EAGAIN || result == -EINTR) {
        conn->write_buffers.push_front(std::move(operation.write));
    } else if (result == -ECANCELED && conn->hard_closing) {
        CompleteWrite(
            &operation.write.completion,
            Status::Error(StatusCode::kNetworkError,
                          "response send was cancelled because the connection closed"));
        MaybeFinalizeConnection(conn->id);
        return;
    } else {
        CompleteWrite(
            &operation.write.completion,
            Status::Error(StatusCode::kNetworkError,
                          "send failed before response was completed"));
        BeginClose(conn->id);
        return;
    }

    conn = FindById(operation.conn_id);
    if (conn == nullptr || conn->hard_closing) {
        return;
    }
    if (conn->graceful_closing && conn->write_buffer_bytes == 0 &&
        conn->write_buffers.empty()) {
        BeginClose(conn->id);
    }
}

void IoUringTcpServerBackend::HandleCancel(Operation& operation, int result) {
    if (result < 0 && result != -ENOENT && result != -EALREADY) {
        Logger::Log(LogLevel::kError, LiburingError("io_uring async cancel failed", result));
        BeginFatalShutdown();
        return;
    }
    if (operation.conn_id != 0) {
        MaybeFinalizeConnection(operation.conn_id);
    }
}

void IoUringTcpServerBackend::DrainWakeEvents() {
    uint64_t value = 0;
    while (true) {
        const ssize_t n = ::read(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        return;
    }
}

void IoUringTcpServerBackend::DrainResponses() {
    DrainCloseRequests();

    std::vector<QueuedResponse> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(response_queue_);
    }

    for (auto& response : pending) {
        Connection* conn = FindById(response.conn_id);
        if (conn == nullptr || conn->hard_closing ||
            (response.generation != 0 && conn->generation != response.generation)) {
            CompleteWrite(
                &response.completion,
                Status::Error(StatusCode::kNetworkError,
                              "connection closed before response was sent"));
            continue;
        }

        const std::size_t bytes = response.data.size();
        conn->write_buffers.push_back(
            {std::move(response.data), 0, std::move(response.completion)});
        conn->write_buffer_bytes += bytes;
        if (conn->write_buffer_bytes > options_.max_write_buffer_bytes) {
            BeginClose(conn->id);
            continue;
        }
        MaybeEnterBackpressure(*conn);
        conn = FindById(response.conn_id);
        if (conn == nullptr || conn->hard_closing) {
            continue;
        }
        if (response.close_after_send) {
            conn->graceful_closing = true;
        }
    }

    DrainCloseRequests();
}

void IoUringTcpServerBackend::DrainCloseRequests() {
    std::vector<QueuedClose> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(close_queue_);
    }
    for (const QueuedClose& close : pending) {
        if (MatchesGeneration(close.conn_id, close.generation)) {
            BeginClose(close.conn_id);
        }
    }
}

void IoUringTcpServerBackend::WakeEventLoop() {
    std::lock_guard<std::mutex> lock(wake_mutex_);
    if (wake_fd_ == -1) {
        return;
    }
    const uint64_t value = 1;
    while (true) {
        const ssize_t n = ::write(wake_fd_, &value, sizeof(value));
        if (n == static_cast<ssize_t>(sizeof(value))) {
            return;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        return;
    }
}

void IoUringTcpServerBackend::CloseListenFd() { CloseFd(&listen_fd_); }

IoUringTcpServerBackend::Connection* IoUringTcpServerBackend::FindById(ConnectionId conn_id) {
    const auto it = connections_.find(conn_id);
    return it == connections_.end() ? nullptr : &it->second;
}

bool IoUringTcpServerBackend::MatchesGeneration(ConnectionId conn_id, uint64_t generation) const {
    const auto it = connections_.find(conn_id);
    if (it == connections_.end()) {
        return false;
    }
    return generation == 0 || it->second.generation == generation;
}

void IoUringTcpServerBackend::BeginClose(ConnectionId conn_id) {
    Connection* conn = FindById(conn_id);
    if (conn == nullptr || conn->hard_closing) {
        return;
    }
    conn->hard_closing = true;
    conn->graceful_closing = false;
    if (conn->in_backpressure) {
        SetBackpressure(*conn, false);
        conn = FindById(conn_id);
        if (conn == nullptr) {
            return;
        }
    }
    FailPendingWrites(*conn, "connection closed before response was sent");
    RequestCancel(conn->recv_token, conn->id, conn->generation);
    RequestCancel(conn->send_token, conn->id, conn->generation);
    MaybeFinalizeConnection(conn_id);
}

void IoUringTcpServerBackend::MaybeFinalizeConnection(ConnectionId conn_id) {
    Connection* conn = FindById(conn_id);
    if (conn == nullptr || !conn->hard_closing) {
        return;
    }
    if (HasOperationsForConnection(conn->id, conn->generation)) {
        return;
    }
    FinalizeConnection(conn_id);
}

bool IoUringTcpServerBackend::HasOperationsForConnection(ConnectionId conn_id,
                                                         uint64_t generation) const {
    for (const auto& entry : operations_) {
        const Operation& operation = *entry.second;
        if (operation.conn_id == conn_id && operation.generation == generation) {
            return true;
        }
    }
    for (const CancelRequest& request : pending_cancels_) {
        if (request.conn_id == conn_id && request.generation == generation) {
            return true;
        }
    }
    return false;
}

void IoUringTcpServerBackend::FinalizeConnection(ConnectionId conn_id) {
    const auto it = connections_.find(conn_id);
    if (it == connections_.end()) {
        return;
    }

    const int fd = it->second.fd;
    const uint64_t generation = it->second.generation;
    FailPendingWrites(it->second, "connection closed before response was sent");
    if (it->second.in_backpressure) {
        SetBackpressure(it->second, false);
    }
    connection_by_fd_.erase(fd);
    connections_.erase(it);
    ::close(fd);
    if (on_close_) {
        on_close_(conn_id, generation);
    }
}

void IoUringTcpServerBackend::FailPendingWrites(Connection& conn, const char* message) {
    while (!conn.write_buffers.empty()) {
        PendingWrite write = std::move(conn.write_buffers.front());
        conn.write_buffers.pop_front();
        CompleteWrite(
            &write.completion,
            Status::Error(StatusCode::kNetworkError, message));
    }
    conn.write_buffer_bytes = 0;
}

void IoUringTcpServerBackend::FailQueuedResponses(const char* message) {
    std::vector<QueuedResponse> pending;
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        pending.swap(response_queue_);
        close_queue_.clear();
    }
    for (QueuedResponse& response : pending) {
        CompleteWrite(
            &response.completion,
            Status::Error(StatusCode::kNetworkError, message));
    }
}

void IoUringTcpServerBackend::SetBackpressure(Connection& conn, bool enabled) {
    if (conn.in_backpressure == enabled) {
        return;
    }
    conn.in_backpressure = enabled;

    std::ostringstream message;
    message << (enabled ? "enter backpressure" : "leave backpressure")
            << " fd=" << conn.fd
            << " write_buffer_size=" << conn.write_buffer_bytes;
    Logger::Log(LogLevel::kWarn, message.str());

    if (on_backpressure_) {
        on_backpressure_(conn.id,
                         conn.generation,
                         conn.fd,
                         enabled,
                         conn.write_buffer_bytes);
    }
}

void IoUringTcpServerBackend::MaybeEnterBackpressure(Connection& conn) {
    if (!conn.in_backpressure && conn.write_buffer_bytes >= options_.high_watermark_bytes) {
        SetBackpressure(conn, true);
    }
}

void IoUringTcpServerBackend::MaybeLeaveBackpressure(Connection& conn) {
    if (conn.in_backpressure && conn.write_buffer_bytes <= options_.low_watermark_bytes) {
        SetBackpressure(conn, false);
    }
}

uint64_t IoUringTcpServerBackend::NextOperationToken() {
    if (next_operation_token_ == 0) {
        if (!event_loop_failed_) {
            Logger::Log(LogLevel::kError, "io_uring operation token space exhausted");
        }
        BeginFatalShutdown();
        return 0;
    }
    return next_operation_token_++;
}

std::unique_ptr<TcpServerBackend> MakeIoUringTcpServerBackend() {
    return std::make_unique<IoUringTcpServerBackend>();
}

}  // namespace minirpc
