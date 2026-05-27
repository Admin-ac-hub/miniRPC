# Net Layer Design (TcpServer / TcpClient extraction)

Date: 2026-05-26
Status: Draft (awaiting user review)

## 1. Background

`include/minirpc/net/{tcp_server,tcp_client,tcp_connection}.h` and the
corresponding `src/net/*.cpp` files are skeletons that return
`StatusCode::kNotImplemented`. The actual TCP / epoll reactor is implemented
inline inside `src/server/rpc_server.cpp` (Linux-only, guarded by
`#ifdef __linux__`), and `src/client/rpc_client.cpp` performs blocking socket
I/O directly with a background reader thread.

The goal of this work is to extract the network plumbing out of the RPC layer
into the `net/` module, so:

- `TcpServer` owns the listening socket, epoll reactor, per-connection
  read/write buffers, and the cross-thread response queue.
- `TcpClient` owns the client socket and a background reader thread.
- `RpcServer` keeps only the service registry, business thread pool, and
  dispatch logic.
- `RpcClient` keeps only the `request_id → promise` pending map, request
  ID generator, and synchronous `Call` API with timeout.

The net layer remains Linux-only for this iteration. macOS (kqueue) support
is out of scope.

## 2. Module Boundaries

```text
TcpServer (new content moved from rpc_server.cpp)
  Owns: listen_fd, epoll_fd, wake_fd, reactor_thread, connections map,
        response_queue, RpcCodec*
  API:  Start / Stop / SendFrame / CloseConnection
  Callbacks: OnFrame(conn_id, generation, ProtocolFrame)
             OnClose(conn_id, generation)

TcpClient
  Owns: fd, reader_thread, closing_, RpcCodec*, send_mutex
  API:  Connect / Close / SendFrame / state
  Callbacks: OnFrame(ProtocolFrame)
             OnClose(CloseReason, message)

RpcServer (slimmed)
  Owns: TcpServer, ThreadPool, ServiceRegistry
  No epoll / accept / read-write-buffer code remains.

RpcClient (slimmed)
  Owns: TcpClient, pending map, next_request_id
  No raw socket / reader thread code remains.

socket_utils  unchanged
```

### Header changes

- `include/minirpc/net/connection.h`: keep `using ConnectionId = uint64_t;`.
  Delete `ConnectionStateData`, `QueuedResponse`, `QueuedClose` (these are
  internal to `TcpServer`; move them to an anonymous namespace inside
  `src/net/tcp_server.cpp`).
- `include/minirpc/net/tcp_connection.h`: keep `enum class ConnectionState`
  (used by `TcpClient::state()`). Delete the `TcpConnection` abstract base
  class — no implementer remains, and the connection-per-object model does
  not match the `(conn_id, generation)` pattern the reactor uses.

## 3. Interface Signatures

### TcpServer

```cpp
struct TcpServerOptions {
    std::size_t max_read_buffer_bytes  = 2 * 1024 * 1024;
    std::size_t max_write_buffer_bytes = 2 * 1024 * 1024;
    std::size_t max_response_queue     = 10000;
    int backlog                        = 128;
};

class TcpServer {
public:
    using OnFrameFn = std::function<void(ConnectionId, uint64_t generation, ProtocolFrame)>;
    using OnCloseFn = std::function<void(ConnectionId, uint64_t generation)>;

    TcpServer();
    ~TcpServer();
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void SetOnFrame(OnFrameFn fn);
    void SetOnClose(OnCloseFn fn);
    void SetOptions(const TcpServerOptions& opts);

    Status Start(const Endpoint& endpoint);
    void   Stop();
    bool   running() const;

    // Thread-safe; callable from any thread.
    // Errors: stale generation, queue full, server stopped.
    Status SendFrame(ConnectionId conn_id,
                     uint64_t generation,
                     const ProtocolFrame& frame,
                     bool close_after_send);

    void CloseConnection(ConnectionId conn_id, uint64_t generation);
};
```

### TcpClient

```cpp
class TcpClient {
public:
    enum class CloseReason {
        kPeerClosed,
        kIoError,
        kProtocolError,
        kLocalClose,
    };

    using OnFrameFn = std::function<void(ProtocolFrame)>;
    using OnCloseFn = std::function<void(CloseReason, const std::string& message)>;

    TcpClient();
    ~TcpClient();
    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    void SetOnFrame(OnFrameFn fn);
    void SetOnClose(OnCloseFn fn);

    Status Connect(const Endpoint& endpoint);
    void   Close();

    // Thread-safe. Synchronous send. Returns IO / closed errors.
    Status SendFrame(const ProtocolFrame& frame);

    ConnectionState state() const;
};
```

### Convention choices

- `OnFrame` takes the `ProtocolFrame` by value (move-friendly).
- `SendFrame` takes `const ProtocolFrame&` so callers can retry / reuse.
- `TcpClient::CloseReason` is an explicit enum; the callback also carries
  a descriptive string for logging.
- The `TcpClient` socket is blocking. Timeouts are enforced one layer up
  (`RpcClient::Call` `future::wait_for`). Blocking matches the existing
  `RpcClient` implementation and keeps `SendFrame` simple.

## 4. Thread Model and Data Flow

### Threads

```text
TcpServer:
  reactor_thread     -- epoll_wait, accept, recv, codec.TryDecode,
                        OnFrame dispatch, send, DrainResponses.

RpcServer:
  thread_pool worker -- runs RpcHandler, then calls TcpServer.SendFrame.
  (caller thread)    -- Start / Stop / RegisterService.

TcpClient:
  reader_thread      -- recv, codec.TryDecode, OnFrame dispatch.

RpcClient:
  (caller thread)    -- Call() builds promise, calls TcpClient.SendFrame,
                        waits on future.
```

### Server request data flow

```text
[reactor] epoll_wait → recv → read_buffer.append
[reactor] while codec.TryDecode(read_buffer) == kOk:
              OnFrame(conn_id, generation, frame)
[reactor]   └→ RpcServer-registered closure:
                  thread_pool.Post([conn_id, gen, frame]{
                      response = handler(frame)
                      tcp_server.SendFrame(conn_id, gen, resp_frame, false)
                  })
[worker]  handler runs
[worker]  SendFrame: codec.Encode → lock response_mutex → enqueue → write(wake_fd)
[reactor] epoll wakes → DrainResponses:
              check conn alive + generation matches
              SendAll → kOk done / kWouldBlock buffer + arm EPOLLOUT
                       / kIoError CloseConnection
[reactor] EPOLLOUT fires → FlushWriteBuffer → buffer drained → clear EPOLLOUT
```

### Client call data flow

```text
[caller]  Call(): allocate request_id, build frame
[caller]  pending_[id] = promise (under pending_mutex)
[caller]  tcp_client.SendFrame(frame):
              lock send_mutex → SendAll → return Status
[caller]  future.wait_for(timeout)
[reader]  recv → buffer.append → codec.TryDecode loop
[reader]  OnFrame(frame) ← TcpClient-internal dispatch
[reader]   └→ RpcClient closure:
                 pending_.find(frame.request_id)
                 promise.set_value(decode body)
[caller]  future ready → return
```

### Synchronization

```text
TcpServer:
  response_mutex   -- guards response_queue, close_queue (cross-thread)
  connections map  -- accessed only by reactor_thread, no lock needed

TcpClient:
  send_mutex       -- serializes SendFrame across caller threads
  state_mutex      -- guards fd_ during connect/close
  closing_         -- atomic<bool> for reader loop exit

RpcClient:
  pending_mutex    -- guards pending map (caller + reader contention)
```

### Shutdown ordering

```text
TcpServer.Stop:
  running_ = false
  close(listen_fd), write(wake_fd) to wake reactor
  reactor_thread.join
  iterate connections → CloseConnection (each triggers OnClose)
  close(epoll_fd, wake_fd)

TcpClient.Close:
  closing_ = true
  shutdown + close(fd_) → reader_thread sees recv == 0 / -1 and exits
  reader_thread.join (if not self)
  OnClose(kLocalClose) fires exactly once (guarded by closing_)
```

## 5. Error Handling

### TcpServer

```text
Start:
  socket / bind / listen / epoll_create / eventfd failure
    → Stop() cleanup, return Status(kNetworkError, msg)
  invalid host string → Status(kNetworkError, "invalid listen host: ...")
  EADDRINUSE          → errno propagated via LastSocketError

Runtime (reactor thread):
  recv == 0  → peer closed → CloseConnection → OnClose(conn_id, gen)
  recv == -1 (other than EINTR/EAGAIN) → IO error → CloseConnection
  read_buffer.size > max_read_buffer_bytes → CloseConnection (DoS guard)
  codec.TryDecode == kProtocolError → CloseConnection
  frame.message_type != kRequest → CloseConnection (protocol violation)
  thread_pool.Post failure (queue full)
    → synthesize kServerError response + close_after_send=true

SendFrame (cross-thread):
  generation mismatch → Status(kNetworkError, "stale connection")
  conn not found      → Status(kNetworkError, "connection closed")
  response_queue full → enqueue close request instead, return Status(kServerError, "queue full")
  wake_fd write failure → Status; queued state still picked up on next epoll

DrainResponses (reactor):
  conn missing / generation stale → silently drop
  SendAll kIoError → CloseConnection
  SendAll kWouldBlock → buffer; exceeds max_write_buffer_bytes → CloseConnection
  close_after_send=true:
    write_buffer empty → close immediately
    otherwise → set closing flag, close after FlushWriteBuffer drains
```

### TcpClient

```text
Connect:
  socket / inet_pton / connect failure → Status(kNetworkError, msg)
  already connected (fd_ != -1) → Status::Ok (idempotent)
  stale reader_thread → join before reconnecting

Runtime (reader thread):
  recv == 0  → OnClose(kPeerClosed, "")
  recv == -1 (not EINTR) → OnClose(kIoError, errno-message)
  codec.TryDecode kProtocolError → OnClose(kProtocolError, error)
  unexpected msg_type → OnClose(kProtocolError, "unexpected non-response frame")
  on exit: close fd under state_mutex; OnClose fires iff closing_ was false

SendFrame:
  fd == -1   → Status(kNetworkError, "connection is closed")
  SendAll kIoError → Status(kNetworkError, "send failed");
                     do not actively close (reader will detect)
  SendAll kWouldBlock should not occur (fd is blocking)

Close:
  idempotent; closing_ atomic guards against re-entry
  if reader_thread.get_id() == current → do not join (reader self-call)
  OnClose fires exactly once (closing_ flag-guarded)
```

### RpcServer error responses

```text
service missing → kServiceNotFound (close_after_send=false)
method missing  → kMethodNotFound
DecodeRequestBody throws BodyCodecError → kDeserializeError
handler throws std::exception → kServerError(what)
handler throws ...           → kServerError("unknown server error")
thread_pool full → kServerError("thread pool queue is full"), close_after_send=true
```

### RpcClient errors

```text
TcpClient.Connect failure → ErrorResponse(kNetworkError)
EncodeRequestBody throws  → ErrorResponse(kSerializeError)
TcpClient.SendFrame failure → erase pending → ErrorResponse(kNetworkError); call Close()
future.wait_for timeout   → erase pending → ErrorResponse(kTimeout)
OnClose callback          → FailPending(kNetworkError, message)
```

### Deadlock / re-entry notes

- `OnClose` fires from the reactor thread (server) or reader thread
  (client). The RpcServer / RpcClient closures only mutate their own
  state (pending map, etc.) and do not call back into the net layer,
  so there is no re-entrant lock acquisition.
- When `response_queue` is full and `close_after_send` cannot ride along
  with a response, the close request is routed through `close_queue`
  which TcpServer drains in the reactor loop.

## 6. Testing Strategy

### Unit / integration tests (Linux only)

```text
tests/net/tcp_server_test.cpp                (new)
  Start / Stop idempotency
  Bind to occupied port → Status(kNetworkError)
  SendFrame with stale generation → Status error, silently dropped
  CloseConnection triggers OnClose exactly once
  Multiple concurrent accept attempts → no drops

tests/net/tcp_client_test.cpp                (new)
  Connect success → state == kConnected
  Connect twice idempotent
  Peer closes → OnClose(kPeerClosed) fires exactly once
  SendFrame after close → Status(kNetworkError)

tests/net/tcp_echo_integration_test.cpp      (new)
  TcpServer.OnFrame = SendFrame(echo)
  TcpClient.OnFrame receives the same frame back
  100 concurrent clients → all responses match
  Large frame (~1 MB) → multi-recv / multi-send still matches
  Client closes → server OnClose fires

tests/basic_structure_test.cpp               (existing, must still pass:
                                              exercises codec, registry, and
                                              a full RpcServer + RpcClient
                                              EchoService round-trip)
```

### Manual / examples

```text
examples/echo_server.cpp, examples/echo_client.cpp
  Refactor consumers; run end-to-end and verify behaviour unchanged.
```

Performance work (benchmark/) is out of scope for this iteration.

### Acceptance criteria

1. `cmake --build` succeeds on Linux.
2. All existing tests stay green.
3. New net-layer tests all green.
4. `examples/echo_server` + `examples/echo_client` work end-to-end.
5. `src/server/rpc_server.cpp` shrinks substantially (epoll / accept /
   read-write-buffer code fully migrated into `src/net/tcp_server.cpp`).

## 7. Out of Scope

- macOS / kqueue support.
- Connection pooling, heartbeat, reconnect (Milestone 6 items in the
  roadmap).
- Async client `AsyncCall` (Milestone 5).
- Performance benchmarking.
- TLS / compression / auth.
