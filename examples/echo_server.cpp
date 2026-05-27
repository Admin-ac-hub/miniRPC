#include <iostream>
#include <string>

#include "minirpc/server/rpc_server.h"

int main() {
    minirpc::RpcServer server;
    server.RegisterService("EchoService", "Echo", [](const minirpc::RpcRequest& request) {
        minirpc::RpcResponse response;
        response.request_id = request.request_id;
        response.payload = request.payload;
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
