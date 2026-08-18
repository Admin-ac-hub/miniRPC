#include "minirpc/net/tcp_server.h"

#include <utility>

#include "minirpc/net/tcp_server_backend.h"

namespace minirpc {

struct TcpServer::Impl {
    std::unique_ptr<TcpServerBackend> backend = MakeEpollTcpServerBackend();
};

TcpServer::TcpServer()
    : impl_(std::make_unique<Impl>()) {}

TcpServer::~TcpServer() { Stop(); }

void TcpServer::SetOnOpen(OnOpenFn fn) { impl_->backend->SetOnOpen(std::move(fn)); }
void TcpServer::SetOnFrame(OnFrameFn fn) { impl_->backend->SetOnFrame(std::move(fn)); }
void TcpServer::SetOnClose(OnCloseFn fn) { impl_->backend->SetOnClose(std::move(fn)); }
void TcpServer::SetOnBackpressure(OnBackpressureFn fn) {
    impl_->backend->SetOnBackpressure(std::move(fn));
}
void TcpServer::SetOptions(const TcpServerOptions& opts) { impl_->backend->SetOptions(opts); }

Status TcpServer::Start(const Endpoint& endpoint) { return impl_->backend->Start(endpoint); }
void TcpServer::Stop() { impl_->backend->Stop(); }
void TcpServer::StopAccepting() { impl_->backend->StopAccepting(); }
bool TcpServer::running() const { return impl_->backend->running(); }

Status TcpServer::SendFrame(ConnectionId conn_id,
                            uint64_t generation,
                            const ProtocolFrame& frame,
                            bool close_after_send) {
    return SendFrameWithCompletion(conn_id, generation, frame, close_after_send, {});
}

Status TcpServer::SendFrameWithCompletion(ConnectionId conn_id,
                                          uint64_t generation,
                                          const ProtocolFrame& frame,
                                          bool close_after_send,
                                          std::function<void(Status)> completion) {
    return impl_->backend->SendFrame(
        conn_id, generation, frame, close_after_send, std::move(completion));
}

void TcpServer::CloseConnection(ConnectionId conn_id, uint64_t generation) {
    impl_->backend->CloseConnection(conn_id, generation);
}

Status TcpServerInternalAccess::SendFrame(TcpServer& server,
                                          ConnectionId conn_id,
                                          uint64_t generation,
                                          const ProtocolFrame& frame,
                                          bool close_after_send,
                                          std::function<void(Status)> completion) {
    return server.SendFrameWithCompletion(
        conn_id, generation, frame, close_after_send, std::move(completion));
}

}  // namespace minirpc
