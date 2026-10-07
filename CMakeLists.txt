cmake_minimum_required(VERSION 3.20)

project(ShardIO LANGUAGES CXX)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(SHARDIO_SANITIZER "" CACHE STRING "ASan/UBSan/TSan: address | undefined | thread")
if(SHARDIO_SANITIZER)
    add_compile_options(-fsanitize=${SHARDIO_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${SHARDIO_SANITIZER})
endif()

set(SHARDIO_SOURCES
    src/backend/accept_op.cpp
    src/backend/accept_multi_op.cpp
    src/backend/cancel_op.cpp
    src/backend/connect_op.cpp
    src/backend/recv_op.cpp
    src/backend/recv_multi_op.cpp
    src/backend/send_op.cpp
    src/backend/send_zc_op.cpp
    src/backend/timeout_op.cpp
    src/backend/uring_backend.cpp
    src/core/context.cpp
    src/core/io_context.cpp
    src/core/context_scheduler.cpp
    src/core/cpu_executor.cpp
    src/core/yield_awaiter.cpp
    src/runtime/cross_thread_inbox.cpp
    src/core/op_awaiter.cpp
    src/core/task.cpp
    src/backend/io_operation.cpp
    src/memory/id_free_list.cpp
    src/memory/buffer_handle.cpp
    src/memory/buffer_pool.cpp
    src/memory/buffer_ring.cpp
    src/net/endpoint.cpp
    src/net/batch.cpp
    src/net/multi_stream.cpp
    src/net/send_pipeline.cpp
    src/net/tcp_stream.cpp
    src/net/tcp_listener.cpp
    src/runtime/io_context_pool.cpp
)

find_path(LIBURING_INCLUDE_DIR liburing.h)
find_library(LIBURING_LIBRARY uring)

add_library(shardio STATIC ${SHARDIO_SOURCES})

target_compile_features(shardio PUBLIC cxx_std_20)

target_include_directories(shardio
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

find_package(Threads REQUIRED)
target_link_libraries(shardio PUBLIC Threads::Threads)

option(SHARDIO_RING_STATS "BufferRing flush counters for diag_recv_multi" OFF)
if(SHARDIO_RING_STATS)
    target_compile_definitions(shardio PUBLIC SHARDIO_RING_STATS)
endif()

option(SHARDIO_BUILD_BENCHMARKS "Build the standalone comparison benchmark" OFF)

if(LIBURING_INCLUDE_DIR AND LIBURING_LIBRARY)
    target_include_directories(shardio PUBLIC ${LIBURING_INCLUDE_DIR})
    target_link_libraries(shardio PUBLIC ${LIBURING_LIBRARY})

    add_executable(shardio_example example/example.cpp)
    target_compile_features(shardio_example PRIVATE cxx_std_20)
    target_link_libraries(shardio_example PRIVATE shardio)

    add_executable(e2e_runtime tests/e2e/runtime_suite.cpp)
    target_compile_features(e2e_runtime PRIVATE cxx_std_20)
    target_link_libraries(e2e_runtime PRIVATE shardio)
    add_test(NAME e2e_correctness COMMAND e2e_runtime --group correctness)
    add_test(NAME e2e_corner COMMAND e2e_runtime --group corner)
    add_test(NAME e2e_scheduler COMMAND e2e_runtime --group scheduler)
    add_test(NAME e2e_stress COMMAND e2e_runtime --group stress)
    add_test(NAME e2e_benchmark COMMAND e2e_runtime --group benchmark)
    set_tests_properties(e2e_correctness PROPERTIES LABELS "e2e;correctness")
    set_tests_properties(e2e_corner PROPERTIES LABELS "e2e;corner")
    set_tests_properties(e2e_scheduler PROPERTIES LABELS "e2e;scheduler")
    set_tests_properties(e2e_stress PROPERTIES LABELS "e2e;stress" TIMEOUT 120)
    set_tests_properties(e2e_benchmark PROPERTIES LABELS "benchmark" DISABLED TRUE)

    add_executable(diag_recv_multi EXCLUDE_FROM_ALL tests/diag_recv_multi.cpp)
    target_compile_features(diag_recv_multi PRIVATE cxx_std_20)
    target_link_libraries(diag_recv_multi PRIVATE shardio)

    if(SHARDIO_BUILD_BENCHMARKS)
        add_executable(bench_client bench/client/bench_client.cpp)
        target_compile_features(bench_client PRIVATE cxx_std_20)
        target_compile_options(bench_client PRIVATE -O3)
        target_compile_definitions(bench_client PRIVATE NDEBUG)
        target_link_libraries(bench_client PRIVATE Threads::Threads)

        add_executable(inferio_server bench/servers/inferio_server.cpp)
        target_compile_features(inferio_server PRIVATE cxx_std_20)
        target_compile_options(inferio_server PRIVATE -O3)
        target_compile_definitions(inferio_server PRIVATE NDEBUG)
        target_link_libraries(inferio_server PRIVATE shardio)

        add_executable(raw_uring_server bench/servers/raw_uring_server.cpp)
        target_compile_features(raw_uring_server PRIVATE cxx_std_20)
        target_compile_options(raw_uring_server PRIVATE -O3)
        target_compile_definitions(raw_uring_server PRIVATE NDEBUG)
        target_link_libraries(raw_uring_server PRIVATE ${LIBURING_LIBRARY} Threads::Threads)

        find_package(Boost QUIET)
        if(Boost_FOUND)
            add_executable(asio_uring_server bench/servers/asio_uring_server.cpp)
            target_compile_features(asio_uring_server PRIVATE cxx_std_20)
            target_compile_options(asio_uring_server PRIVATE -O3)
            target_compile_definitions(asio_uring_server PRIVATE
                NDEBUG BOOST_ASIO_HAS_IO_URING BOOST_ASIO_DISABLE_EPOLL)
            target_include_directories(asio_uring_server PRIVATE ${Boost_INCLUDE_DIRS})
            target_link_libraries(asio_uring_server PRIVATE ${LIBURING_LIBRARY} Threads::Threads)
        else()
            message(WARNING "Boost headers not found; asio_uring_server will not be built")
        endif()
    endif()
endif()
