cmake_minimum_required(VERSION 3.16)

project(miniRPC VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINIRPC_BUILD_EXAMPLES "Build miniRPC examples" ON)
option(MINIRPC_BUILD_TESTS "Build miniRPC tests" ON)
option(MINIRPC_BUILD_BENCHMARKS "Build miniRPC benchmarks" ON)

find_package(Threads REQUIRED)

file(GLOB_RECURSE MINIRPC_SOURCES CONFIGURE_DEPENDS
    src/*.cpp
)

add_library(minirpc STATIC ${MINIRPC_SOURCES})
add_library(minirpc::minirpc ALIAS minirpc)

target_include_directories(minirpc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(minirpc
    PRIVATE
        $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Wall -Wextra -Wpedantic>
)

target_link_libraries(minirpc PUBLIC Threads::Threads)

if(MINIRPC_BUILD_EXAMPLES)
    add_executable(echo_server examples/echo_server.cpp)
    target_link_libraries(echo_server PRIVATE minirpc::minirpc)

    add_executable(echo_client examples/echo_client.cpp)
    target_link_libraries(echo_client PRIVATE minirpc::minirpc)
endif()

if(MINIRPC_BUILD_TESTS)
    enable_testing()

    add_executable(basic_structure_test tests/basic_structure_test.cpp)
    target_link_libraries(basic_structure_test PRIVATE minirpc::minirpc)
    add_test(NAME basic_structure_test COMMAND basic_structure_test)

    add_executable(tcp_server_test tests/net/tcp_server_test.cpp)
    target_link_libraries(tcp_server_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_server_test COMMAND tcp_server_test)

    add_executable(tcp_client_test tests/net/tcp_client_test.cpp)
    target_link_libraries(tcp_client_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_client_test COMMAND tcp_client_test)

    add_executable(tcp_echo_integration_test tests/net/tcp_echo_integration_test.cpp)
    target_link_libraries(tcp_echo_integration_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_echo_integration_test COMMAND tcp_echo_integration_test)

    add_executable(rpc_client_test tests/client/rpc_client_test.cpp)
    target_link_libraries(rpc_client_test PRIVATE minirpc::minirpc)
    add_test(NAME rpc_client_test COMMAND rpc_client_test)
endif()

if(MINIRPC_BUILD_BENCHMARKS)
    add_executable(rpc_bench benchmark/rpc_bench.cpp)
    target_link_libraries(rpc_bench PRIVATE minirpc::minirpc)
endif()
