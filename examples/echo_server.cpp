#include <algorithm>
#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

#include "minirpc/server/rpc_server.h"

int main() {
    minirpc::RpcServer server;

    // Echo — 原样回传
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.payload = request.payload;
        return response;
    });

    // Add — payload 格式 "a,b"，返回 a+b
    server.RegisterService("MathService", "Add", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        auto comma = request.payload.find(',');
        if (comma == std::string::npos) {
            response.error_message = "invalid format, expected \"a,b\"";
            return response;
        }
        int a = std::stoi(request.payload.substr(0, comma));
        int b = std::stoi(request.payload.substr(comma + 1));
        response.payload = std::to_string(a + b);
        return response;
    });

    // GetServerTime — 无参数，返回服务端当前时间
    server.RegisterService("InfoService", "GetServerTime", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        auto now = std::chrono::system_clock::now();
        auto t   = std::chrono::system_clock::to_time_t(now);
        char buf[64];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
        response.payload = buf;
        return response;
    });

    // Reverse — 翻转字符串
    server.RegisterService("StringService", "Reverse", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        std::string s = request.payload;
        std::reverse(s.begin(), s.end());
        response.payload = s;
        return response;
    });

    // UpperCase — 转大写
    server.RegisterService("StringService", "UpperCase", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        std::string s = request.payload;
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
            return std::toupper(c);
        });
        response.payload = s;
        return response;
    });

    auto status = server.Start({"127.0.0.1", 9000});
    if (!status.ok()) {
        std::cerr << status.ToString() << '\n';
        return 1;
    }

    std::cout << "Echo server listening on 127.0.0.1:9000. Press Enter to stop.\n";
    std::string line;
    std::getline(std::cin, line);
    server.Stop();
    return 0;
}
