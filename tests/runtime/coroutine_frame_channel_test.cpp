#include <cassert>
#include <cerrno>
#include <chrono>
#include <string>
#include <thread>

#include <sys/socket.h>
#include <unistd.h>

#include "minirpc/core/status.h"
#include "minirpc/net/socket_utils.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/coroutine_frame_channel.h"
#include "minirpc/runtime/coroutine_io_context.h"

namespace {

using namespace std::chrono_literals;

class SocketPair {
public:
    SocketPair() {
        const int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_);
        assert(rc == 0);
        std::string error;
        assert(minirpc::SetNonBlocking(fds_[0], &error));
        assert(minirpc::SetNonBlocking(fds_[1], &error));
    }

    ~SocketPair() {
        if (fds_[0] != -1) {
            ::close(fds_[0]);
        }
        if (fds_[1] != -1) {
            ::close(fds_[1]);
        }
    }

    int first() const noexcept { return fds_[0]; }
    int second() const noexcept { return fds_[1]; }

private:
    int fds_[2]{-1, -1};
};

minirpc::ProtocolFrame MakeFrame(uint64_t request_id, std::string body) {
    minirpc::ProtocolFrame frame;
    frame.request_id = request_id;
    frame.message_type = minirpc::MessageType::kRequest;
    frame.codec_type = minirpc::CodecType::kRaw;
    frame.body = std::move(body);
    return frame;
}

void TestReadFrameHandlesPartialInput() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::CoroutineFrameChannel channel(&io);
    minirpc::RpcCodec codec;
    const std::string encoded = codec.Encode(MakeFrame(101, "partial"));

    std::thread writer([&] {
        std::this_thread::sleep_for(10ms);
        const ssize_t n1 = ::write(sockets.second(), encoded.data(), 7);
        assert(n1 == 7);
        std::this_thread::sleep_for(10ms);
        const ssize_t n2 = ::write(sockets.second(), encoded.data() + 7, encoded.size() - 7);
        assert(n2 == static_cast<ssize_t>(encoded.size() - 7));
    });

    io.Spawn([&] {
        minirpc::ProtocolFrame frame;
        const minirpc::Status status = channel.ReadFrame(sockets.first(), &frame);
        assert(status.ok());
        assert(frame.request_id == 101);
        assert(frame.body == "partial");
        assert(channel.buffered_bytes() == 0);
    });

    io.Run();
    writer.join();
}

void TestReadFrameHandlesStickyInput() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::CoroutineFrameChannel channel(&io);
    minirpc::RpcCodec codec;
    const std::string encoded =
        codec.Encode(MakeFrame(201, "first")) + codec.Encode(MakeFrame(202, "second"));

    std::thread writer([&] {
        std::this_thread::sleep_for(10ms);
        const ssize_t n = ::write(sockets.second(), encoded.data(), encoded.size());
        assert(n == static_cast<ssize_t>(encoded.size()));
    });

    io.Spawn([&] {
        minirpc::ProtocolFrame first;
        minirpc::ProtocolFrame second;
        assert(channel.ReadFrame(sockets.first(), &first).ok());
        assert(first.request_id == 201);
        assert(first.body == "first");

        assert(channel.ReadFrame(sockets.first(), &second).ok());
        assert(second.request_id == 202);
        assert(second.body == "second");
        assert(channel.buffered_bytes() == 0);
    });

    io.Run();
    writer.join();
}

void TestWriteFrameCanBeReadByCodec() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::CoroutineFrameChannel channel(&io);
    std::string received;

    std::thread reader([&] {
        char buffer[256];
        while (received.size() < minirpc::kProtocolHeaderSize + 5) {
            const ssize_t n = ::read(sockets.second(), buffer, sizeof(buffer));
            if (n > 0) {
                received.append(buffer, static_cast<std::size_t>(n));
                continue;
            }
            if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                std::this_thread::sleep_for(1ms);
                continue;
            }
            break;
        }
    });

    io.Spawn([&] {
        const minirpc::Status status = channel.WriteFrame(sockets.first(), MakeFrame(301, "hello"));
        assert(status.ok());
    });

    io.Run();
    reader.join();

    minirpc::RpcCodec codec;
    minirpc::ProtocolFrame decoded;
    std::string error;
    assert(codec.TryDecode(received, &decoded, &error) == minirpc::DecodeResult::kSuccess);
    assert(decoded.request_id == 301);
    assert(decoded.body == "hello");
    assert(received.empty());
}

void TestProtocolErrorIsReturned() {
    SocketPair sockets;
    minirpc::CoroutineIoContext io;
    minirpc::CoroutineFrameChannel channel(&io);
    std::string invalid(minirpc::kProtocolHeaderSize, '\0');

    std::thread writer([&] {
        std::this_thread::sleep_for(10ms);
        const ssize_t n = ::write(sockets.second(), invalid.data(), invalid.size());
        assert(n == static_cast<ssize_t>(invalid.size()));
    });

    io.Spawn([&] {
        minirpc::ProtocolFrame frame;
        const minirpc::Status status = channel.ReadFrame(sockets.first(), &frame);
        assert(!status.ok());
        assert(status.code() == minirpc::StatusCode::kProtocolError);
    });

    io.Run();
    writer.join();
}

}  // namespace

int main() {
    TestReadFrameHandlesPartialInput();
    TestReadFrameHandlesStickyInput();
    TestWriteFrameCanBeReadByCodec();
    TestProtocolErrorIsReturned();
    return 0;
}
