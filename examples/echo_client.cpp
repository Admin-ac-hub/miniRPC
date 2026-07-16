#include <chrono>
#include <iostream>

#include "minirpc/client/rpc_client.h"

using namespace std::chrono_literals;

static void CheckAndPrint(const minirpc::RpcResponse& response, const char* label) {
    if (response.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk)) {
        std::cout << label << response.payload << '\n';
    } else {
        std::cout << label << "ERROR: " << response.error_message << '\n';
    }
}

int main() {
    minirpc::RpcClient client({"127.0.0.1", 9000});

    CheckAndPrint(client.Call("EchoService", "Echo", "hello miniRPC", 3s),       "[Echo]       ");
    CheckAndPrint(client.Call("MathService", "Add", "17,25", 3s),                "[Add 17+25]  ");
    CheckAndPrint(client.Call("InfoService", "GetServerTime", "", 3s),           "[ServerTime] ");
    CheckAndPrint(client.Call("StringService", "Reverse", "miniRPC", 3s),        "[Reverse]    ");
    CheckAndPrint(client.Call("StringService", "UpperCase", "hello world", 3s),  "[UpperCase]  ");

    client.Close();
    return 0;
}
