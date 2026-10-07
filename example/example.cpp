/* example.cpp, 最小回显示例：每个 I/O worker 独立拥有连接与流水线 */

#include "shardio/shardio.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <netinet/in.h>
#include <thread>

namespace {

/**
 * @brief 接受一个连接并把收到的数据原样发回。
 * @param listener 当前 worker 独占的监听器。
 */
Task<void> echo_once(TcpListener listener)
{
    auto accepted = co_await listener.accept();
    if (!accepted) {
        co_return;
    }

    TcpStream stream = std::move(accepted.value);
    std::array<std::byte, 1024> buffer{};
    for (;;) {
        auto received = co_await stream.recv(
            MutableBuffer{buffer.data(), buffer.size()});
        if (!received || received.value == 0) {
            co_return;
        }
        if (!(co_await stream.send_all(
                ConstBuffer{buffer.data(), received.value}))) {
            co_return;
        }
    }
}

/**
 * @brief 用连接自管的 SendPipeline 发送一条消息并校验回显。
 * @param context 本连接固定所属的 I/O worker。
 * @param peer 对应 worker 的服务地址。
 * @param worker 用于输出的 worker 编号。
 * @param completed 完成的客户端数量。
 * @param passed 成功的客户端数量。
 */
Task<void> pipeline_client(Context context, Endpoint peer, std::size_t worker,
                           std::atomic<int>& completed,
                           std::atomic<int>& passed)
{
    auto connected = co_await TcpStream::connect(context, peer);
    if (!connected) {
        completed.fetch_add(1, std::memory_order_release);
        co_return;
    }

    TcpStream stream = std::move(connected.value);
    constexpr char message[] = "hello shardio";
    auto pipeline = stream.send_pipeline({
        .depth = 8,
        .buffer_size = 16 * 1024,
        .zero_copy = true,
    });
    auto queued = co_await pipeline.send(ConstBuffer{
        reinterpret_cast<const std::byte*>(message), sizeof(message) - 1});
    auto drained = co_await pipeline.close();
    (void)stream.shutdown(Shutdown::Write);

    std::array<std::byte, sizeof(message) - 1> echo{};
    std::size_t received = 0;
    while (received < echo.size()) {
        auto part = co_await stream.recv(MutableBuffer{
            echo.data() + received, echo.size() - received});
        if (!part || part.value == 0) {
            break;
        }
        received += part.value;
    }

    if (queued && drained && received == echo.size() &&
        std::memcmp(echo.data(), message, echo.size()) == 0) {
        std::printf("worker %zu: echo ok\n", worker);
        passed.fetch_add(1, std::memory_order_relaxed);
    }
    completed.fetch_add(1, std::memory_order_release);
}

} // namespace

/**
 * @brief 在两个 I/O worker 上分别运行一个完整的连接示例。
 * @return 两个 worker 都成功时返回 0，否则返回 1。
 */
int main()
{
    IoContextPool runtime({
        .io_workers = 2,
        .cpu_workers = 2,
        .pin_threads = false,
    });

    std::array<TcpListener, 2> listeners;
    std::array<Endpoint, 2> peers;
    for (std::size_t i = 0; i < listeners.size(); ++i) {
        auto bound = TcpListener::bind(
            runtime.context(i), Endpoint::ipv4(INADDR_LOOPBACK, 0));
        if (!bound) {
            std::fprintf(stderr, "bind worker %zu: %s\n", i,
                         bound.error.message().c_str());
            return 1;
        }
        listeners[i] = std::move(bound.value);
        peers[i] = listeners[i].local_endpoint();
    }

    std::atomic<int> completed{0};
    std::atomic<int> passed{0};
    runtime.start();
    for (std::size_t i = 0; i < listeners.size(); ++i) {
        Context context = runtime.context(i);
        auto listener = std::make_shared<TcpListener>(std::move(listeners[i]));
        context.post([&, context, i, listener] {
            context.spawn(echo_once(std::move(*listener)));
            context.spawn(pipeline_client(
                context, peers[i], i, completed, passed));
        });
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_acquire) != 2 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    runtime.stop();
    runtime.join();
    return passed.load(std::memory_order_relaxed) == 2 ? 0 : 1;
}
