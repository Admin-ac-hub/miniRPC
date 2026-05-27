#pragma once

namespace minirpc {

enum class ConnectionState {
    kDisconnected,
    kConnecting,
    kConnected,
    kClosing
};

}  // namespace minirpc
