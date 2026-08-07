cmake_minimum_required(VERSION 3.16)

project(miniRPC VERSION 0.1.0 LANGUAGES CXX ASM)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINIRPC_BUILD_EXAMPLES "Build miniRPC examples" ON)
option(MINIRPC_BUILD_TESTS "Build miniRPC tests" ON)
option(MINIRPC_BUILD_BENCHMARKS "Build miniRPC benchmarks" ON)

set(MINIRPC_TCP_SERVER_BACKEND "epoll" CACHE STRING "TCP server IO backend")
set_property(CACHE MINIRPC_TCP_SERVER_BACKEND PROPERTY STRINGS epoll io_uring)
if(NOT MINIRPC_TCP_SERVER_BACKEND STREQUAL "epoll" AND
   NOT MINIRPC_TCP_SERVER_BACKEND STREQUAL "io_uring")
    message(FATAL_ERROR
        "MINIRPC_TCP_SERVER_BACKEND must be one of: epoll, io_uring "
        "(got '${MINIRPC_TCP_SERVER_BACKEND}')"
    )
endif()

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

file(GLOB_RECURSE MINIRPC_SOURCES CONFIGURE_DEPENDS
    src/*.cpp
)

if(MINIRPC_TCP_SERVER_BACKEND STREQUAL "epoll")
    list(FILTER MINIRPC_SOURCES EXCLUDE REGEX "/io_uring_tcp_server_backend\\.cpp$")
    set(MINIRPC_LIBURING_VERSION "not-linked")
else()
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
    find_package(Liburing 2.0 REQUIRED)
    if(Liburing_VERSION)
        set(MINIRPC_LIBURING_VERSION "${Liburing_VERSION}")
    else()
        set(MINIRPC_LIBURING_VERSION "unknown")
    endif()
endif()

execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    RESULT_VARIABLE MINIRPC_GIT_REVISION_RESULT
    OUTPUT_VARIABLE MINIRPC_GIT_REVISION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT MINIRPC_GIT_REVISION_RESULT EQUAL 0)
    set(MINIRPC_GIT_REVISION "unknown")
else()
    execute_process(
        COMMAND git status --porcelain
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE MINIRPC_GIT_STATUS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(MINIRPC_GIT_STATUS)
        string(APPEND MINIRPC_GIT_REVISION "-dirty")
    endif()
endif()

set(MINIRPC_ASM_SOURCES
    src/runtime/coctx_swap.S
)

set(PROTO_FILES proto/minirpc_body.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(minirpc STATIC ${MINIRPC_SOURCES} ${MINIRPC_ASM_SOURCES} ${PROTO_SRCS})
add_library(minirpc::minirpc ALIAS minirpc)

target_include_directories(minirpc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_options(minirpc
    PRIVATE
        $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Wall -Wextra -Wpedantic>
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    set_source_files_properties(${PROTO_SRCS} PROPERTIES COMPILE_FLAGS "-w")
endif()

target_link_libraries(minirpc
    PUBLIC
        Threads::Threads
    PRIVATE
        protobuf::libprotobuf
)

if(MINIRPC_TCP_SERVER_BACKEND STREQUAL "io_uring")
    target_link_libraries(minirpc PRIVATE Liburing::Liburing)
    target_compile_definitions(minirpc PRIVATE MINIRPC_TCP_SERVER_BACKEND_IO_URING=1)
endif()

if(MINIRPC_BUILD_EXAMPLES)
    add_executable(echo_server examples/echo_server.cpp)
    target_link_libraries(echo_server PRIVATE minirpc::minirpc)

    add_executable(echo_client examples/echo_client.cpp)
    target_link_libraries(echo_client PRIVATE minirpc::minirpc)
endif()

if(MINIRPC_BUILD_TESTS)
    enable_testing()

    set(MINIRPC_TEST_TARGETS)

    add_executable(basic_structure_test tests/basic_structure_test.cpp)
    target_link_libraries(basic_structure_test PRIVATE minirpc::minirpc)
    add_test(NAME basic_structure_test COMMAND basic_structure_test)
    list(APPEND MINIRPC_TEST_TARGETS basic_structure_test)

    add_executable(tcp_server_test tests/net/tcp_server_test.cpp)
    target_link_libraries(tcp_server_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_server_test COMMAND tcp_server_test)
    list(APPEND MINIRPC_TEST_TARGETS tcp_server_test)

    add_executable(tcp_client_test tests/net/tcp_client_test.cpp)
    target_link_libraries(tcp_client_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_client_test COMMAND tcp_client_test)
    list(APPEND MINIRPC_TEST_TARGETS tcp_client_test)

    add_executable(tcp_echo_integration_test tests/net/tcp_echo_integration_test.cpp)
    target_link_libraries(tcp_echo_integration_test PRIVATE minirpc::minirpc)
    add_test(NAME tcp_echo_integration_test COMMAND tcp_echo_integration_test)
    list(APPEND MINIRPC_TEST_TARGETS tcp_echo_integration_test)

    add_executable(rpc_client_test tests/client/rpc_client_test.cpp)
    target_link_libraries(rpc_client_test PRIVATE minirpc::minirpc)
    add_test(NAME rpc_client_test COMMAND rpc_client_test)
    list(APPEND MINIRPC_TEST_TARGETS rpc_client_test)

    add_executable(metrics_test tests/observability/metrics_test.cpp)
    target_link_libraries(metrics_test PRIVATE minirpc::minirpc)
    add_test(NAME metrics_test COMMAND metrics_test)
    list(APPEND MINIRPC_TEST_TARGETS metrics_test)

    add_executable(protobuf_body_codec_test tests/protocol/protobuf_body_codec_test.cpp)
    target_link_libraries(protobuf_body_codec_test PRIVATE minirpc::minirpc)
    add_test(NAME protobuf_body_codec_test COMMAND protobuf_body_codec_test)
    list(APPEND MINIRPC_TEST_TARGETS protobuf_body_codec_test)

    add_executable(coroutine_test tests/runtime/coroutine_test.cpp)
    target_link_libraries(coroutine_test PRIVATE minirpc::minirpc)
    add_test(NAME coroutine_test COMMAND coroutine_test)
    list(APPEND MINIRPC_TEST_TARGETS coroutine_test)

    add_executable(scheduler_test tests/runtime/scheduler_test.cpp)
    target_link_libraries(scheduler_test PRIVATE minirpc::minirpc)
    add_test(NAME scheduler_test COMMAND scheduler_test)
    list(APPEND MINIRPC_TEST_TARGETS scheduler_test)

    add_executable(timer_queue_test tests/runtime/timer_queue_test.cpp)
    target_link_libraries(timer_queue_test PRIVATE minirpc::minirpc)
    add_test(NAME timer_queue_test COMMAND timer_queue_test)
    list(APPEND MINIRPC_TEST_TARGETS timer_queue_test)

    add_executable(coroutine_io_context_test tests/runtime/coroutine_io_context_test.cpp)
    target_link_libraries(coroutine_io_context_test PRIVATE minirpc::minirpc)
    add_test(NAME coroutine_io_context_test COMMAND coroutine_io_context_test)
    list(APPEND MINIRPC_TEST_TARGETS coroutine_io_context_test)

    add_executable(coroutine_frame_channel_test tests/runtime/coroutine_frame_channel_test.cpp)
    target_link_libraries(coroutine_frame_channel_test PRIVATE minirpc::minirpc)
    add_test(NAME coroutine_frame_channel_test COMMAND coroutine_frame_channel_test)
    list(APPEND MINIRPC_TEST_TARGETS coroutine_frame_channel_test)

    add_executable(coroutine_rpc_connection_test tests/server/coroutine_rpc_connection_test.cpp)
    target_link_libraries(coroutine_rpc_connection_test PRIVATE minirpc::minirpc)
    add_test(NAME coroutine_rpc_connection_test COMMAND coroutine_rpc_connection_test)
    list(APPEND MINIRPC_TEST_TARGETS coroutine_rpc_connection_test)

    add_executable(coroutine_rpc_server_test tests/server/coroutine_rpc_server_test.cpp)
    target_link_libraries(coroutine_rpc_server_test PRIVATE minirpc::minirpc)
    add_test(NAME coroutine_rpc_server_test COMMAND coroutine_rpc_server_test)
    list(APPEND MINIRPC_TEST_TARGETS coroutine_rpc_server_test)

    foreach(MINIRPC_TEST_TARGET IN LISTS MINIRPC_TEST_TARGETS)
        target_compile_options(${MINIRPC_TEST_TARGET}
            PRIVATE
                $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-UNDEBUG>
        )
    endforeach()
endif()

if(MINIRPC_BUILD_BENCHMARKS)
    add_executable(rpc_bench benchmark/rpc_bench.cpp)
    target_link_libraries(rpc_bench PRIVATE minirpc::minirpc)
    target_compile_definitions(rpc_bench PRIVATE
        MINIRPC_GIT_REVISION="${MINIRPC_GIT_REVISION}"
        MINIRPC_LIBURING_VERSION="${MINIRPC_LIBURING_VERSION}"
        MINIRPC_TCP_SERVER_BACKEND_NAME="${MINIRPC_TCP_SERVER_BACKEND}"
    )
endif()
