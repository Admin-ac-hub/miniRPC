#include <chrono>
#include <iostream>

#include "minirpc/client/rpc_client.h"

int main() {
    minirpc::RpcClient client({"127.0.0.1", 9000});
    for (int i = 0; i < 3; ++i) {
        auto response = client.Call("EchoService", "Echo", "hello miniRPC", std::chrono::seconds(3));
        if (response.status_code == static_cast<int32_t>(minirpc::StatusCode::kOk)) {
            std::cout << response.payload << '\n';
        } else {
            std::cout << response.error_message << '\n';
        }
    }
    client.Close();
    return 0;
}
