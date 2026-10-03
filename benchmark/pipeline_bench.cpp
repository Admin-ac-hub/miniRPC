// P4 验证工具：量化 codec 解码路径的 buffer 搬移代价。
//
// 两种模式：
//   codec  —— 纯 codec 基准，不联网。构造恰好铺满一次 recv 的粘包 buffer，
//             反复 TryDecode 直到解完，扫描 body 尺寸。用来隔离并量化
//             `RpcCodec::TryDecode` 里 `buffer.erase(0, frame_size)` 的搬移代价。
//   socket —— 端到端 pipeline。单连接、单客户端线程，改变在途请求数 K，
//             观察服务端 read_buffer 的粘包深度对吞吐的影响。
//
// 判读方式见 docs/optimization_plan.md 的 P4 条目。

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "minirpc/client/rpc_client.h"
#include "minirpc/core/status.h"
#include "minirpc/protocol/codec.h"
#include "minirpc/protocol/frame.h"
#include "minirpc/protocol/message.h"
#include "minirpc/server/rpc_server.h"

namespace {

using SteadyClock = std::chrono::steady_clock;

// 与 src/net/epoll_tcp_server_backend.cpp 的 kReadChunkSize 保持一致：
// 服务端单次 recv 的上限，也就是 read_buffer 里粘包深度的现实上界。
constexpr std::size_t kReadChunkSize = 4096;

struct Options {
    std::string mode = "codec";
    std::string host = "127.0.0.1";
    uint16_t port = 19800;
    std::size_t repeats = 5;
    std::size_t samples = 1000;
    std::size_t body_size = 16;
    std::size_t payload_size = 16;
    std::size_t requests = 5000;
    int timeout_ms = 3000;
    std::vector<std::size_t> frames;
    std::vector<std::size_t> depths;
};

void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0 << " [options]\n"
              << "  --mode codec|socket|both          default: codec\n"
              << "  --frames A,B,C                    codec 模式扫描的粘包帧数, default: 1,2,4,8,16,32,64,128\n"
              << "  --body-size N                     codec 模式每帧 body 字节数, default: 16\n"
              << "  --samples N                       codec 模式每个计时区间的解包轮数, default: 1000\n"
              << "  --repeats N                       codec 模式每档重复次数(取中位数), default: 5\n"
              << "  --depths A,B,C                    socket 模式扫描的在途请求数, default: 1,2,4,8,16,32,64,128,256\n"
              << "  --requests N                      socket 模式每档总请求数, default: 5000\n"
              << "  --payload-size N                  socket 模式 payload 大小, default: 16\n"
              << "  --host HOST                       default: 127.0.0.1\n"
              << "  --port N                          default: 19800\n"
              << "  --timeout-ms N                    default: 3000\n";
}

bool ParseSize(const std::string& value, std::size_t* out) {
    if (value.empty() || value.front() == '-') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (errno == ERANGE || end == value.c_str() || *end != '\0' ||
        parsed > std::numeric_limits<std::size_t>::max()) {
        return false;
    }
    *out = static_cast<std::size_t>(parsed);
    return true;
}

bool ParseInt(const std::string& value, int* out) {
    errno = 0;
    char* end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (errno == ERANGE || end == value.c_str() || *end != '\0' || parsed < std::numeric_limits<int>::min() ||
        parsed > std::numeric_limits<int>::max()) {
        return false;
    }
    *out = static_cast<int>(parsed);
    return true;
}

bool ParseSizeList(const std::string& value, std::vector<std::size_t>* out) {
    std::stringstream input(value);
    std::string item;
    std::vector<std::size_t> parsed;
    while (std::getline(input, item, ',')) {
        std::size_t size = 0;
        if (item.empty() || !ParseSize(item, &size) || size == 0) {
            return false;
        }
        parsed.push_back(size);
    }
    if (parsed.empty()) {
        return false;
    }
    *out = std::move(parsed);
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) {
            std::cerr << "missing value for " << arg << "\n";
            return false;
        }
        const std::string value = argv[++i];
        if (arg == "--mode") {
            options->mode = value;
        } else if (arg == "--host") {
            options->host = value;
        } else if (arg == "--body-size" || arg == "--body_size") {
            if (!ParseSize(value, &options->body_size)) return false;
        } else if (arg == "--frames") {
            if (!ParseSizeList(value, &options->frames)) return false;
        } else if (arg == "--samples") {
            if (!ParseSize(value, &options->samples)) return false;
        } else if (arg == "--repeats") {
            if (!ParseSize(value, &options->repeats)) return false;
        } else if (arg == "--depths") {
            if (!ParseSizeList(value, &options->depths)) return false;
        } else if (arg == "--requests") {
            if (!ParseSize(value, &options->requests)) return false;
        } else if (arg == "--payload-size" || arg == "--payload_size") {
            if (!ParseSize(value, &options->payload_size)) return false;
        } else if (arg == "--timeout-ms" || arg == "--timeout_ms") {
            if (!ParseInt(value, &options->timeout_ms)) return false;
        } else if (arg == "--port") {
            std::size_t port = 0;
            if (!ParseSize(value, &port) || port == 0 || port > 65535) return false;
            options->port = static_cast<uint16_t>(port);
        } else {
            std::cerr << "unknown option: " << arg << "\n";
            return false;
        }
    }

    if (options->mode != "codec" && options->mode != "socket" && options->mode != "both") {
        std::cerr << "--mode must be codec, socket, or both\n";
        return false;
    }
    if (options->frames.empty()) {
        options->frames = {1, 2, 4, 8, 16, 32, 64, 128};
    }
    if (options->depths.empty()) {
        options->depths = {1, 2, 4, 8, 16, 32, 64, 128, 256};
    }
    if (options->repeats == 0 || options->samples == 0 || options->body_size == 0 ||
        options->requests == 0 || options->timeout_ms <= 0) {
        std::cerr << "repeats, samples, body-size, requests, and timeout-ms must be positive\n";
        return false;
    }
    return true;
}

double MedianOf(std::vector<double> values) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t middle = values.size() / 2;
    if (values.size() % 2 != 0) {
        return values[middle];
    }
    return (values[middle - 1] + values[middle]) / 2.0;
}

double PercentileOf(std::vector<double> values, double percentile) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double index = percentile * static_cast<double>(values.size() - 1);
    return values[static_cast<std::size_t>(index + 0.5)];
}

// ---- codec 模式 ----

struct StickyBuffer {
    std::string data;
    std::size_t frames = 0;
};

StickyBuffer BuildStickyBuffer(std::size_t frames, std::size_t body_size) {
    const minirpc::RpcCodec codec;
    StickyBuffer out;
    out.frames = frames;
    out.data.reserve(frames * (minirpc::kProtocolHeaderSize + body_size));
    for (std::size_t i = 0; i < frames; ++i) {
        minirpc::ProtocolFrame frame;
        frame.request_id = i + 1;
        frame.message_type = minirpc::MessageType::kRequest;
        frame.codec_type = minirpc::CodecType::kRaw;
        frame.body.assign(body_size, 'x');
        out.data += codec.Encode(frame);
    }
    return out;
}

struct CodecDepthResult {
    std::size_t frames = 0;
    std::size_t buffer_bytes = 0;
    double parse_only_ns_per_frame = 0.0;
    double full_ns_per_frame = 0.0;
};

// 每档解完整个粘包 buffer 的 per-frame 耗时。parse_only 传 frame=nullptr，
// 跳过 body 拷贝，隔离出 erase 的搬移代价；full 走完整解析，反映真实路径。
//
// 测量方法的两处讲究：
//  1. 单帧解析只有几纳秒，低于 steady_clock 的分辨率。所以每个计时区间里
//     重复 samples 轮，把总耗时拉到微秒级以上再摊薄。
//  2. 每轮先把整份粘包 append 进同一个 buffer 再解空，复现服务端
//     recv -> append -> 循环 decode 的真实形态：buffer 常驻热缓存，
//     capacity 复用不重分配，且 append 摊到每帧恒为 frame_bytes。
//     若改成固定 chunk 大小扫 body 尺寸，append 的摊薄量会随帧数漂移，
//     把 erase 的代价淹掉。
CodecDepthResult MeasureCodecDepth(std::size_t frames, std::size_t body_size, std::size_t samples,
                                   std::size_t repeats) {
    const minirpc::RpcCodec codec;
    const StickyBuffer sticky = BuildStickyBuffer(frames, body_size);

    std::vector<double> parse_only_samples;
    std::vector<double> full_samples;
    std::string error;
    parse_only_samples.reserve(repeats);
    full_samples.reserve(repeats);

    const double frames_total = static_cast<double>(samples) * static_cast<double>(frames);

    for (std::size_t repeat = 0; repeat < repeats; ++repeat) {
        {
            std::string buffer;
            buffer.reserve(sticky.data.size());
            std::size_t decoded = 0;
            const auto begin = SteadyClock::now();
            for (std::size_t i = 0; i < samples; ++i) {
                buffer.append(sticky.data);
                while (true) {
                    const minirpc::DecodeResult result = codec.TryDecode(buffer, nullptr, &error);
                    if (result != minirpc::DecodeResult::kSuccess) {
                        if (result != minirpc::DecodeResult::kNeedMoreData) {
                            std::cerr << "codec mode: unexpected decode error: " << error << "\n";
                            std::exit(2);
                        }
                        break;
                    }
                    ++decoded;
                }
            }
            const auto end = SteadyClock::now();
            if (decoded != samples * frames) {
                std::cerr << "codec mode: decoded " << decoded << " of " << samples * frames << " frames\n";
                std::exit(2);
            }
            parse_only_samples.push_back(
                std::chrono::duration<double, std::nano>(end - begin).count() / frames_total);
        }

        {
            std::string buffer;
            buffer.reserve(sticky.data.size());
            const auto begin = SteadyClock::now();
            for (std::size_t i = 0; i < samples; ++i) {
                buffer.append(sticky.data);
                while (true) {
                    minirpc::ProtocolFrame frame;
                    const minirpc::DecodeResult result = codec.TryDecode(buffer, &frame, &error);
                    if (result != minirpc::DecodeResult::kSuccess) {
                        break;
                    }
                }
            }
            const auto end = SteadyClock::now();
            full_samples.push_back(
                std::chrono::duration<double, std::nano>(end - begin).count() / frames_total);
        }
    }

    CodecDepthResult result;
    result.frames = frames;
    result.buffer_bytes = sticky.data.size();
    result.parse_only_ns_per_frame = MedianOf(std::move(parse_only_samples));
    result.full_ns_per_frame = MedianOf(std::move(full_samples));
    return result;
}

void RunCodecMode(const Options& options) {
    const std::size_t frame_bytes = minirpc::kProtocolHeaderSize + options.body_size;
    std::cout << "\nmode=codec body_size=" << options.body_size << " frame_bytes=" << frame_bytes
              << " samples=" << options.samples << " repeats=" << options.repeats << "\n";
    std::cout << "note: 服务端单次 recv 上限 " << kReadChunkSize << " 字节, 即 body=" << options.body_size
              << " 时粘包深度现实上界约 " << kReadChunkSize / frame_bytes << " 帧\n";
    std::cout << std::left << std::setw(10) << "frames" << std::right << std::setw(14) << "buffer_bytes" << std::setw(12)
              << "ns/frame" << std::setw(12) << "base_ns" << std::setw(12) << "move_ns" << std::setw(13)
              << "move_share" << std::setw(14) << "full_ns/frame" << '\n';

    // 进程首次测量会撞上 CPU 频率爬升与缓存冷启动，读数明显偏高。先跑一轮丢弃。
    if (!options.frames.empty()) {
        (void)MeasureCodecDepth(options.frames.back(), options.body_size, options.samples, options.repeats);
    }

    // frames=1 没有剩余数据可搬移，其 per-frame 耗时即纯解析基线。
    const double baseline_ns =
        MeasureCodecDepth(1, options.body_size, options.samples, options.repeats).parse_only_ns_per_frame;

    for (std::size_t frames : options.frames) {
        const CodecDepthResult result =
            MeasureCodecDepth(frames, options.body_size, options.samples, options.repeats);
        const double ns_per_frame = result.parse_only_ns_per_frame;
        const double move_ns = ns_per_frame - baseline_ns;
        const double move_share = ns_per_frame > 0.0 ? move_ns / ns_per_frame : 0.0;

        std::cout << std::left << std::setw(10) << frames << std::right << std::setw(14) << result.buffer_bytes
                  << std::setw(12) << std::fixed << std::setprecision(1) << ns_per_frame << std::setw(12)
                  << std::setprecision(1) << baseline_ns << std::setw(12) << std::setprecision(1) << move_ns
                  << std::setw(12) << std::setprecision(1) << move_share * 100.0 << "%" << std::setw(14)
                  << std::setprecision(1) << result.full_ns_per_frame << '\n';
    }
}

// ---- socket 模式 ----

struct PipelineResult {
    std::size_t depth = 0;
    std::size_t requests = 0;
    std::size_t success = 0;
    std::size_t failed = 0;
    double seconds = 0.0;
    double qps = 0.0;
    double p50_batch_us = 0.0;
    double p99_batch_us = 0.0;
};

minirpc::RpcResponse MakeEchoResponse(const minirpc::RpcRequest& request) {
    minirpc::RpcResponse response;
    response.request_id = request.request_id;
    response.status_code = static_cast<int32_t>(minirpc::StatusCode::kOk);
    response.payload = request.payload;
    return response;
}

PipelineResult RunPipelineOnce(const Options& options, std::size_t depth, uint16_t port) {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        return MakeEchoResponse(request);
    });

    const minirpc::Endpoint endpoint{options.host, port};
    const minirpc::Status status = server.Start(endpoint);
    if (!status.ok()) {
        std::cerr << "failed to start server on port " << port << ": " << status.ToString() << "\n";
        std::exit(2);
    }

    const std::string payload(options.payload_size, 'x');
    const std::chrono::milliseconds timeout(options.timeout_ms);
    minirpc::RpcClient client(endpoint);

    const minirpc::RpcResponse warmup = client.Call("EchoService", "Echo", payload, timeout);
    if (warmup.status_code != static_cast<int32_t>(minirpc::StatusCode::kOk) || warmup.payload != payload) {
        client.Close();
        server.Stop();
        std::cerr << "warmup call failed on port " << port << "\n";
        std::exit(2);
    }

    std::size_t success = 0;
    std::size_t failed = 0;
    std::size_t issued = 0;
    std::vector<double> batch_samples;
    batch_samples.reserve(options.requests / depth + 1);

    const auto begin = SteadyClock::now();
    while (issued < options.requests) {
        const std::size_t batch = std::min(depth, options.requests - issued);
        const auto batch_begin = SteadyClock::now();
        std::vector<std::future<minirpc::RpcResponse>> futures;
        futures.reserve(batch);
        for (std::size_t i = 0; i < batch; ++i) {
            futures.push_back(client.CallAsync("EchoService", "Echo", payload, timeout));
        }
        for (auto& future : futures) {
            const minirpc::RpcResponse response = future.get();
            if (response.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk) &&
                response.payload == payload) {
                ++success;
            } else {
                ++failed;
            }
        }
        const auto batch_end = SteadyClock::now();
        batch_samples.push_back(std::chrono::duration<double, std::micro>(batch_end - batch_begin).count());
        issued += batch;
    }
    const auto end = SteadyClock::now();

    client.Close();
    server.Stop();

    PipelineResult result;
    result.depth = depth;
    result.requests = options.requests;
    result.success = success;
    result.failed = failed;
    result.seconds = std::chrono::duration<double>(end - begin).count();
    result.qps = result.seconds > 0.0 ? static_cast<double>(options.requests) / result.seconds : 0.0;
    result.p50_batch_us = PercentileOf(batch_samples, 0.50);
    result.p99_batch_us = PercentileOf(batch_samples, 0.99);
    return result;
}

void RunSocketMode(const Options& options) {
    std::cout << "\nmode=socket host=" << options.host << " payload_size=" << options.payload_size
              << " requests_per_depth=" << options.requests << " timeout_ms=" << options.timeout_ms << "\n";
    std::cout << std::left << std::setw(10) << "depth" << std::right << std::setw(10) << "requests" << std::setw(10)
              << "success" << std::setw(10) << "failed" << std::setw(12) << "seconds" << std::setw(12) << "qps"
              << std::setw(14) << "p50_batch_us" << std::setw(14) << "p99_batch_us" << '\n';

    bool nagle_suspected = false;
    for (std::size_t index = 0; index < options.depths.size(); ++index) {
        const std::size_t depth = options.depths[index];
        const uint16_t port = static_cast<uint16_t>(options.port + index);
        std::cerr << "running depth=" << depth << " port=" << port << '\n';
        const PipelineResult result = RunPipelineOnce(options, depth, port);
        std::cout << std::left << std::setw(10) << result.depth << std::right << std::setw(10) << result.requests
                  << std::setw(10) << result.success << std::setw(10) << result.failed << std::setw(12)
                  << std::fixed << std::setprecision(3) << result.seconds << std::setw(12) << std::setprecision(1)
                  << result.qps << std::setw(14) << std::setprecision(2) << result.p50_batch_us << std::setw(14)
                  << std::setprecision(2) << result.p99_batch_us << '\n';
        // 小帧在途多个时，Nagle 会把后续帧压到 delayed ACK 超时(40ms)才发。
        // 这是 socket 选项问题，与 codec 无关，必须区分开，否则会误判成 P4 的代价。
        if (result.p50_batch_us > 10000.0) {
            nagle_suspected = true;
        }
    }

    if (nagle_suspected) {
        std::cout << "\nwarning: 部分档位的 batch 耗时出现毫秒级平台, 典型成因是 Nagle + delayed ACK。\n"
                     "         项目当前未对任何 socket 设置 TCP_NODELAY, pipeline 模式下小帧会被 Nagle 缓存。\n"
                     "         该延迟与 codec 无关, 需先修 socket 选项才能用本模式隔离 codec 的代价。\n";
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        PrintUsage(argv[0]);
        return 1;
    }

    if (options.mode == "codec" || options.mode == "both") {
        RunCodecMode(options);
    }
    if (options.mode == "socket" || options.mode == "both") {
        RunSocketMode(options);
    }
    return 0;
}
