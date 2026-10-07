/* runtime_suite.cpp, ShardIO 端到端：correctness、corner、scheduler、stress、benchmark */

#include "backend/io_operation.hpp"
#include "backend/uring_backend.hpp"
#include "core/io_context.hpp"
#include "core/op_awaiter.hpp"
#include "core/task.hpp"
#include "memory/buffer_pool.hpp"
#include "memory/buffer_ring.hpp"
#include "net/send_pipeline.hpp"
#include "net/endpoint.hpp"
#include "net/tcp_listener.hpp"
#include "net/tcp_stream.hpp"
#include "runtime/cross_thread_inbox.hpp"
#include "runtime/io_context_pool.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <vector>

namespace {

constexpr std::size_t kZeroCopySize = 16 * 1024;

int g_fails = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,       \
                         #cond);                                               \
            ++g_fails;                                                         \
        }                                                                      \
    } while (0)

struct Gate {
    std::mutex mu;
    std::condition_variable cv;
    bool ready{false};

    void signal()
    {
        std::lock_guard<std::mutex> lock(mu);
        ready = true;
        cv.notify_all();
    }

    bool wait_ms(int ms)
    {
        std::unique_lock<std::mutex> lock(mu);
        return cv.wait_for(lock, std::chrono::milliseconds(ms),
                           [this] { return ready; });
    }
};

void close_fd(int& fd)
{
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

int make_socketpair(int fds[2])
{
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        throw std::runtime_error("socketpair");
    }
    return 0;
}

void set_nonblock(int fd)
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    CHECK(flags >= 0);
    CHECK(::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0);
}

bool write_fully(int fd, const void* data, std::size_t n, int timeout_ms)
{
    const auto* p = static_cast<const std::byte*>(data);
    std::size_t off = 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (off < n) {
        const ssize_t w = ::write(fd, p + off, n - off);
        if (w > 0) {
            off += static_cast<std::size_t>(w);
            continue;
        }
        if (w < 0 && errno == EINTR) {
            continue;
        }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  deadline - std::chrono::steady_clock::now())
                                  .count();
            if (left <= 0) {
                return false;
            }
            pollfd pfd{fd, POLLOUT, 0};
            const int pr = ::poll(&pfd, 1, static_cast<int>(left));
            if (pr <= 0) {
                return false;
            }
            continue;
        }
        return false;
    }
    return true;
}

struct Runtime {
    UringBackend* backend{nullptr};
    std::unique_ptr<IoContext> ctx;
    std::thread io;

    explicit Runtime(unsigned qd = 1024,
                     IoBufferConfig buffers = IoBufferConfig{})
    {
        auto b = std::make_unique<UringBackend>(qd);
        backend = b.get();
        ctx = std::make_unique<IoContext>(std::move(b), nullptr, buffers);
        io = std::thread([this] { ctx->run(); });
    }

    ~Runtime()
    {
        ctx->stop();
        if (io.joinable()) {
            io.join();
        }
    }

    template <typename F>
    void on_io(F&& fn)
    {
        ctx->post(std::forward<F>(fn));
    }

    /**
     * @brief 等待此前投递和完成回调退出 I/O 线程调用栈。
     * @param timeout_ms 最大等待时间。
     * @return barrier 在期限内执行时返回 true。
     */
    bool barrier(int timeout_ms = 2000)
    {
        Gate done;
        on_io([&done] { done.signal(); });
        return done.wait_ms(timeout_ms);
    }
};

bool wait_done(Runtime& rt, IoOperation& op, int ms);
bool wait_done(IoContext& ctx, IoOperation& op, int ms);

void test_pool_basic()
{
    BufferPool pool({4096, 8});
    CHECK(pool.capacity() == 8);
    CHECK(pool.buffer_size() == 4096);
    auto a = pool.try_acquire();
    CHECK(static_cast<bool>(a));
    CHECK(a->id() < 8);
    a.reset();
    // 归还后再借应成功
    auto b = pool.try_acquire();
    CHECK(static_cast<bool>(b));
}

void test_post_wakeup_stop()
{
    Runtime rt;
    Gate gate;
    std::atomic<int> n{0};
    rt.ctx->post([&] {
        n.fetch_add(1);
        gate.signal();
    });
    CHECK(gate.wait_ms(2000));
    CHECK(n.load() == 1);
}

void test_send_recv_oneshot()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    const char msg[] = "hello-shardio";
    char recv_buf[64]{};
    RecvOp recv_op(fds[0], MutableBuffer{reinterpret_cast<std::byte*>(recv_buf),
                                      sizeof(recv_buf)});
    SendOp send_op(fds[1], MutableBuffer{reinterpret_cast<std::byte*>(
                                          const_cast<char*>(msg)),
                                      sizeof(msg) - 1});
    rt.on_io([&] {
        rt.ctx->submit(recv_op);
        rt.ctx->submit(send_op);
    });
    CHECK(wait_done(rt, recv_op, 2000));
    CHECK(wait_done(rt, send_op, 2000));
    CHECK(recv_op.result() == static_cast<int>(sizeof(msg) - 1));
    CHECK(std::memcmp(recv_buf, msg, sizeof(msg) - 1) == 0);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_registered_recv()
{
    BufferPool pool({64, 4});
    Runtime rt(1024, IoBufferConfig{0, 0});
    Gate reg;
    rt.on_io([&] {
        rt.backend->register_buffers(pool);
        reg.signal();
    });
    CHECK(reg.wait_ms(2000));
    CHECK(pool.is_registered());
    int fds[2]{-1, -1};
    make_socketpair(fds);
    auto handle = pool.try_acquire();
    CHECK(static_cast<bool>(handle));
    handle->resize(handle->capacity());
    const char msg[] = "reg";
    RecvOp recv_op(fds[0], handle->view(), handle->id());
    SendOp send_op(fds[1],
                   MutableBuffer{reinterpret_cast<std::byte*>(const_cast<char*>(msg)),
                              sizeof(msg) - 1});
    rt.on_io([&] {
        rt.ctx->submit(recv_op);
        rt.ctx->submit(send_op);
    });
    CHECK(wait_done(rt, recv_op, 2000));
    CHECK(recv_op.result() == 3);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

struct RecvSinkDrop final : MultiShotSink<Buffer> {
    std::atomic<int> shots{0};
    std::atomic<long long> bytes{0};
    std::atomic<int> end_res{1};
    Gate* gate{nullptr};
    std::vector<std::uint64_t> gaps_ns;
    std::chrono::steady_clock::time_point last{};
    bool have_last{false};

    void on_shot(Buffer value) noexcept override
    {
        bytes.fetch_add(static_cast<long long>(value.size()));
        shots.fetch_add(1);
        const auto now = std::chrono::steady_clock::now();
        // 只在预先 reserve 时记间隔，避免 noexcept 里 realloc
        if (have_last && gaps_ns.size() < gaps_ns.capacity()) {
            gaps_ns.push_back(static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now - last)
                    .count()));
        }
        last = now;
        have_last = true;
        (void)value;
    }

    void on_end(int result) noexcept override
    {
        end_res.store(result);
        if (gate != nullptr) {
            gate->signal();
        }
    }
};

struct RecvSinkStress final : MultiShotSink<Buffer> {
    Runtime* rt{nullptr};
    BufferRing* ring{nullptr};
    int fd{-1};
    std::atomic<int> shots{0};
    std::atomic<long long> bytes{0};
    std::atomic<int> enobufs{0};
    std::atomic<int> end_res{1};
    Gate* eof_gate{nullptr};
    std::unique_ptr<RecvMultiOp> op;
    std::vector<std::uint64_t> gaps_ns;
    std::chrono::steady_clock::time_point last{};
    bool have_last{false};

    void arm()
    {
        op = std::make_unique<RecvMultiOp>(fd, *ring, *this);
        rt->ctx->submit(*op);
    }

    void on_shot(Buffer value) noexcept override
    {
        bytes.fetch_add(static_cast<long long>(value.size()));
        shots.fetch_add(1);
        const auto now = std::chrono::steady_clock::now();
        if (have_last && gaps_ns.size() < gaps_ns.capacity()) {
            gaps_ns.push_back(static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now - last)
                    .count()));
        }
        last = now;
        have_last = true;
        (void)value;
    }

    void on_end(int result) noexcept override
    {
        end_res.store(result);
        // 内核可能在同一批 CQ 里先耗尽 provided buffer 再报 ENOBUFS，
        // 此时 Handle 已 reclaim，只需补货并换一颗新 Op（旧对象还在 complete 栈上）
        if (result == -ENOBUFS) {
            enobufs.fetch_add(1);
            // IoContext 会在跑本 post 前 flush_reclaims；available==0 再挂只会立刻再 ENOBUFS
            rt->on_io([this] {
                ring->flush_reclaims();
                if (ring->kernel_available() > 0) {
                    arm();
                }
            });
            return;
        }
        if (eof_gate != nullptr) {
            eof_gate->signal();
        }
    }
};

struct RecvSinkHold final : MultiShotSink<Buffer> {
    std::atomic<int> shots{0};
    std::atomic<int> end_res{1};
    Gate* gate{nullptr};
    Buffer hold;

    void on_shot(Buffer value) noexcept override
    {
        shots.fetch_add(1);
        if (!hold) {
            hold = std::move(value);
        }
    }

    void on_end(int result) noexcept override
    {
        end_res.store(result);
        if (gate != nullptr) {
            gate->signal();
        }
    }
};

bool wait_pred(int ms, const std::function<bool()>& pred)
{
    const int step = 10;
    for (int i = 0; i < ms / step; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(step));
    }
    return pred();
}

/** 只在 I/O 线程挂 waiter；完成后再 Gate，主线程不要直接读 completed_。 */
struct ExistingOpWait {
    IoOperation* op{nullptr};

    bool await_ready() const noexcept
    {
        return op->is_completed();
    }

    void await_suspend(std::coroutine_handle<> h) noexcept
    {
        op->set_waiter(h);
    }

    void await_resume() noexcept {}
};

Task<void> observe_op(Context ex, IoOperation& op, Gate& done)
{
    if (op.is_completed()) {
        done.signal();
        co_return;
    }
    if (op.is_submitted()) {
        co_await ExistingOpWait{&op};
        done.signal();
        co_return;
    }
    (void)co_await OpAwaiter(ex, op);
    done.signal();
}

bool wait_done(IoContext& ctx, IoOperation& op, int ms)
{
    Gate g;
    ctx.post([&] { ctx.spawn(observe_op(ctx.context(), op, g)); });
    return g.wait_ms(ms);
}

bool wait_done(Runtime& rt, IoOperation& op, int ms)
{
    return wait_done(*rt.ctx, op, ms);
}

std::uint16_t ipv4_port(const Endpoint& a)
{
    sockaddr_in in{};
    std::memcpy(&in, a.native(), sizeof(in));
    return ntohs(in.sin_port);
}

int make_listen_loopback(sockaddr_in& addr)
{
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        throw std::runtime_error("socket");
    }
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        throw std::runtime_error("bind");
    }
    if (::listen(fd, 16) != 0) {
        ::close(fd);
        throw std::runtime_error("listen");
    }
    socklen_t alen = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &alen) != 0) {
        ::close(fd);
        throw std::runtime_error("getsockname");
    }
    return fd;
}

sockaddr_storage to_storage(const sockaddr_in& in)
{
    sockaddr_storage ss{};
    std::memcpy(&ss, &in, sizeof(in));
    return ss;
}

std::uint64_t percentile_ns(std::vector<std::uint64_t> xs, double p)
{
    if (xs.empty()) {
        return 0;
    }
    const std::size_t i = static_cast<std::size_t>(
        (xs.size() - 1) * p);
    std::nth_element(xs.begin(), xs.begin() + static_cast<std::ptrdiff_t>(i),
                     xs.end());
    return xs[i];
}

double ru_sec(const timeval& t)
{
    return static_cast<double>(t.tv_sec) +
           static_cast<double>(t.tv_usec) / 1000000.0;
}

struct CpuSnap {
    double user_sec{0};
    double sys_sec{0};
    long nvcsw{0};
    long nivcsw{0};
};

CpuSnap cpu_now()
{
    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    return {ru_sec(ru.ru_utime), ru_sec(ru.ru_stime), ru.ru_nvcsw, ru.ru_nivcsw};
}

void print_cpu(const CpuSnap& a, const CpuSnap& b, double wall_sec)
{
    const double user = b.user_sec - a.user_sec;
    const double sys = b.sys_sec - a.sys_sec;
    const double pct = wall_sec > 0 ? ((user + sys) / wall_sec) * 100.0 : 0;
    std::printf("cpu_user_sec    : %.4f\n", user);
    std::printf("cpu_system_sec  : %.4f\n", sys);
    std::printf("cpu_percent     : %.1f\n", pct);
    std::printf("ctx_switches    : %ld\n",
                (b.nvcsw + b.nivcsw) - (a.nvcsw + a.nivcsw));
}

void print_rss(const char* prefix)
{
    FILE* f = std::fopen("/proc/self/status", "r");
    if (f == nullptr) {
        return;
    }
    char line[256];
    while (std::fgets(line, sizeof(line), f) != nullptr) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) {
            std::printf("%srss           :%s", prefix, line + 6);
            break;
        }
    }
    std::fclose(f);
}

void print_bench(const char* name, double sec, long long ops, long long bytes,
                 std::vector<std::uint64_t> lat_ns, int enobufs = 0)
{
    const std::uint64_t p50 = percentile_ns(lat_ns, 0.50);
    const std::uint64_t p95 = percentile_ns(lat_ns, 0.95);
    const std::uint64_t p99 = percentile_ns(lat_ns, 0.99);
    std::printf("\n=== bench %s ===\n", name);
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("ops            : %lld\n", ops);
    std::printf("throughput     : %.0f ops/s\n", sec > 0 ? ops / sec : 0);
    if (bytes > 0) {
        std::printf("bytes          : %lld\n", bytes);
        std::printf("bandwidth      : %.2f MiB/s\n",
                    sec > 0 ? (static_cast<double>(bytes) / (1024.0 * 1024.0)) / sec
                            : 0);
    }
    std::printf("p50_us         : %.3f\n", p50 / 1000.0);
    std::printf("p95_us         : %.3f\n", p95 / 1000.0);
    std::printf("p99_us         : %.3f\n", p99 / 1000.0);
    std::printf("enobufs        : %d\n", enobufs);
    print_rss("");
}

struct YieldSnap {
    std::uint64_t suspends{0};
    std::uint64_t resumes{0};
    std::size_t pending{0};
    std::size_t pending_max{0};
};

YieldSnap snap_yield(IoContext& ctx)
{
    YieldSnap s;
    Gate g;
    ctx.post([&] {
        s.suspends = ctx.yield_suspends();
        s.resumes = ctx.yield_resumes();
        s.pending = ctx.pending_handles_size();
        s.pending_max = ctx.pending_handles_max();
        g.signal();
    });
    (void)g.wait_ms(2000);
    return s;
}

void print_yield(const char* name, const YieldSnap& s)
{
    std::printf("\n=== %s ===\n", name);
    std::printf("yield_suspends : %llu\n",
                static_cast<unsigned long long>(s.suspends));
    std::printf("yield_resumes  : %llu\n",
                static_cast<unsigned long long>(s.resumes));
    std::printf("pending_now    : %zu\n", s.pending);
    std::printf("pending_max    : %zu\n", s.pending_max);
}

void print_yield(const char* name, IoContext& ctx)
{
    print_yield(name, snap_yield(ctx));
}

void test_recv_multi_and_reclaim()
{
    BufferPool pool({128, 16});
    BufferRing ring(pool, BufferRing::Config{.group_id = 1, .capacity = 16});
    Runtime rt;
    Gate armed;
    rt.on_io([&] {
        rt.backend->register_buffer_ring(ring);
        ring.provide_all();
        armed.signal();
    });
    CHECK(armed.wait_ms(2000));

    int fds[2]{-1, -1};
    make_socketpair(fds);
    RecvSinkDrop sink;
    Gate ended;
    sink.gate = &ended;
    RecvMultiOp op(fds[0], ring, sink);
    Gate submitted;
    rt.on_io([&] {
        rt.ctx->submit(op);
        submitted.signal();
    });
    CHECK(submitted.wait_ms(2000));

    const char a[] = "aaaa";
    const char b[] = "bbbb";
    CHECK(::write(fds[1], a, 4) == 4);
    CHECK(::write(fds[1], b, 4) == 4);
    CHECK(wait_pred(3000, [&] { return sink.bytes.load() >= 8; }));
    CHECK(sink.shots.load() >= 1);
    ::shutdown(fds[1], SHUT_WR);
    if (!ended.wait_ms(1000)) {
        rt.ctx->cancel(op);
        CHECK(ended.wait_ms(2000) || wait_done(rt, op, 2000));
    }
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_recv_multi_enobufs_corner()
{
    BufferPool pool({32, 2});
    BufferRing ring(pool, BufferRing::Config{.group_id = 2, .capacity = 2});
    Runtime rt;
    Gate armed;
    rt.on_io([&] {
        rt.backend->register_buffer_ring(ring);
        // 不 provide_all：有数据时 BUFFER_SELECT 应直接 -ENOBUFS
        armed.signal();
    });
    CHECK(armed.wait_ms(2000));

    int fds[2]{-1, -1};
    make_socketpair(fds);
    RecvSinkHold sink;
    Gate ended;
    sink.gate = &ended;
    RecvMultiOp op(fds[0], ring, sink);
    Gate submitted;
    rt.on_io([&] {
        rt.ctx->submit(op);
        submitted.signal();
    });
    CHECK(submitted.wait_ms(2000));

    char blob[16];
    std::memset(blob, 'x', sizeof(blob));
    CHECK(::write(fds[1], blob, sizeof(blob)) == static_cast<ssize_t>(sizeof(blob)));
    CHECK(ended.wait_ms(3000));
    CHECK(sink.end_res.load() == -ENOBUFS);
    Gate dropped;
    rt.on_io([&] {
        sink.hold = Buffer{};
        dropped.signal();
    });
    CHECK(dropped.wait_ms(2000));
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_recv_multi_enobufs_refill()
{
    BufferPool pool({64, 4});
    BufferRing ring(pool, BufferRing::Config{.group_id = 2, .capacity = 4});
    Runtime rt;
    Gate armed;
    rt.on_io([&] {
        rt.backend->register_buffer_ring(ring);
        armed.signal();
    });
    CHECK(armed.wait_ms(2000));

    int fds[2]{-1, -1};
    make_socketpair(fds);
    RecvSinkDrop sink1;
    Gate ended1;
    sink1.gate = &ended1;
    RecvMultiOp op1(fds[0], ring, sink1);
    Gate submitted1;
    rt.on_io([&] {
        rt.ctx->submit(op1);
        submitted1.signal();
    });
    CHECK(submitted1.wait_ms(2000));
    char blob[16];
    std::memset(blob, 'y', sizeof(blob));
    CHECK(::write(fds[1], blob, sizeof(blob)) == static_cast<ssize_t>(sizeof(blob)));
    CHECK(ended1.wait_ms(3000));
    CHECK(sink1.end_res.load() == -ENOBUFS);

    // 上层责任：补货后再挂一颗新的 RecvMulti（已 finish 的不能重提交）
    RecvSinkDrop sink2;
    Gate ended2;
    sink2.gate = &ended2;
    RecvMultiOp op2(fds[0], ring, sink2);
    Gate submitted2;
    rt.on_io([&] {
        ring.provide_all();
        rt.ctx->submit(op2);
        submitted2.signal();
    });
    CHECK(submitted2.wait_ms(2000));
    CHECK(::write(fds[1], blob, sizeof(blob)) == static_cast<ssize_t>(sizeof(blob)));
    CHECK(wait_pred(3000, [&] { return sink2.bytes.load() >= 16; }));
    CHECK(sink2.shots.load() >= 1);
    ::shutdown(fds[1], SHUT_WR);
    if (!ended2.wait_ms(1000)) {
        rt.ctx->cancel(op2);
        (void)ended2.wait_ms(2000);
    }
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_connect_accept()
{
    Runtime rt;
    sockaddr_in addr{};
    int listen_fd = make_listen_loopback(addr);
    int cli = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(cli >= 0);
    AcceptOp acc(listen_fd);
    ConnectOp conn(cli, to_storage(addr));
    rt.on_io([&] {
        rt.ctx->submit(acc);
        rt.ctx->submit(conn);
    });
    CHECK(wait_done(rt, conn, 3000));
    CHECK(wait_done(rt, acc, 3000));
    CHECK(conn.result() == 0);
    CHECK(acc.result() >= 0);
    int srv = acc.result();
    close_fd(srv);
    close_fd(cli);
    close_fd(listen_fd);
}

void test_send_zc()
{
    Runtime rt;
    sockaddr_in addr{};
    int listen_fd = make_listen_loopback(addr);
    int cli = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(cli >= 0);
    AcceptOp acc(listen_fd);
    ConnectOp conn(cli, to_storage(addr));
    rt.on_io([&] {
        rt.ctx->submit(acc);
        rt.ctx->submit(conn);
    });
    CHECK(wait_done(rt, conn, 3000) && conn.result() == 0);
    CHECK(wait_done(rt, acc, 3000) && acc.result() >= 0);
    if (acc.result() < 0) {
        close_fd(cli);
        close_fd(listen_fd);
        return;
    }
    int srv = acc.result();

    const char msg[] = "zc-hello";
    char recv_buf[32]{};
    RecvOp recv_op(srv, MutableBuffer{reinterpret_cast<std::byte*>(recv_buf),
                                   sizeof(recv_buf)});
    SendZcOp send_op(cli, MutableBuffer{reinterpret_cast<std::byte*>(
                                          const_cast<char*>(msg)),
                                      sizeof(msg) - 1});
    rt.on_io([&] {
        rt.ctx->submit(recv_op);
        rt.ctx->submit(send_op);
    });
    CHECK(wait_done(rt, send_op, 3000));
    CHECK(wait_done(rt, recv_op, 3000));
    // 本机 TCP 通常支持 send_zc；不支持则至少要 complete 出负 errno
    if (send_op.result() > 0) {
        CHECK(recv_op.result() == static_cast<int>(sizeof(msg) - 1));
        CHECK(std::memcmp(recv_buf, msg, sizeof(msg) - 1) == 0);
    } else {
        CHECK(send_op.result() < 0);
        std::printf("send_zc unsupported: %d\n", send_op.result());
    }
    close_fd(srv);
    close_fd(cli);
    close_fd(listen_fd);
}

void test_timeout()
{
    Runtime rt;
    TimeoutOp op(20 * 1000 * 1000ULL);  // 20ms
    rt.on_io([&] { rt.ctx->submit(op); });
    CHECK(wait_done(rt, op, 2000));
    CHECK(op.result() < 0);  // 通常 -ETIME
}

void test_cancel_recv()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    char buf[16]{};
    RecvOp recv_op(fds[0], MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
    rt.on_io([&] { rt.ctx->submit(recv_op); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    rt.ctx->cancel(recv_op);
    CHECK(wait_done(rt, recv_op, 2000));
    CHECK(recv_op.result() == -ECANCELED);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

struct AcceptSink final : MultiShotSink<AcceptedFd> {
    std::atomic<int> shots{0};
    std::atomic<int> end_res{1};
    Gate* gate{nullptr};
    std::atomic<int> taken[8]{-1, -1, -1, -1, -1, -1, -1, -1};

    void on_shot(AcceptedFd value) noexcept override
    {
        const int fd = value.release();
        const int i = shots.load(std::memory_order_relaxed);
        if (i >= 0 && i < 8) {
            taken[i].store(fd, std::memory_order_relaxed);
            shots.fetch_add(1, std::memory_order_release);
        } else if (fd >= 0) {
            ::close(fd);
        }
    }

    void on_end(int result) noexcept override
    {
        end_res.store(result);
        if (gate != nullptr) {
            gate->signal();
        }
    }
};

void test_accept_multi()
{
    Runtime rt;
    int listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(listen_fd >= 0);
    int yes = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    CHECK(::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    CHECK(::listen(listen_fd, 8) == 0);
    socklen_t alen = sizeof(addr);
    CHECK(::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &alen) == 0);

    AcceptSink sink;
    Gate gate;
    sink.gate = &gate;
    AcceptMultiOp op(listen_fd, sink);
    rt.on_io([&] { rt.ctx->submit(op); });

    int cli = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(cli >= 0);
    CHECK(::connect(cli, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);

    for (int i = 0; i < 50 && sink.shots.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    CHECK(sink.shots.load(std::memory_order_acquire) >= 1);
    CHECK(sink.taken[0].load(std::memory_order_relaxed) >= 0);
    int c2 = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int c3 = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(c2 >= 0 && c3 >= 0);
    CHECK(::connect(c2, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    CHECK(::connect(c3, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    CHECK(wait_pred(3000, [&] { return sink.shots.load() >= 3; }));
    rt.ctx->cancel(op);
    CHECK(wait_done(rt, op, 2000));
    close_fd(cli);
    close_fd(c2);
    close_fd(c3);
    for (auto& slot : sink.taken) {
        int fd = slot.load(std::memory_order_relaxed);
        close_fd(fd);
    }
    close_fd(listen_fd);
}

void test_stress_recv_multi()
{
    constexpr unsigned kBuf = 4096;
    constexpr unsigned kCount = 512;
    constexpr int kWrites = 65536;
    constexpr int kChunk = 1024;
    constexpr int kWarmup = 256;
    BufferPool pool({kBuf, kCount});
    BufferRing ring(pool, BufferRing::Config{
        .group_id = 1,
        .capacity = kCount,
        .reclaim_batch = 32,
        .low_watermark = 64,
    });
    Runtime rt;
    Gate armed;
    rt.on_io([&] {
        rt.backend->register_buffer_ring(ring);
        ring.provide_all();
        armed.signal();
    });
    CHECK(armed.wait_ms(2000));

    int fds[2]{-1, -1};
    make_socketpair(fds);
    int snd = 256 * 1024;
    ::setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
    ::setsockopt(fds[0], SOL_SOCKET, SO_RCVBUF, &snd, sizeof(snd));

    RecvSinkStress sink;
    sink.rt = &rt;
    sink.ring = &ring;
    sink.fd = fds[0];
    sink.gaps_ns.reserve(300000);
    Gate eof;
    sink.eof_gate = &eof;
    Gate submitted;
    rt.on_io([&] {
        sink.arm();
        submitted.signal();
    });
    CHECK(submitted.wait_ms(2000));
    set_nonblock(fds[1]);

    std::vector<char> chunk(static_cast<std::size_t>(kChunk), 's');
    constexpr int kWriteTimeoutMs = 60000;
    bool writes_ok = true;
    for (int i = 0; i < kWarmup && writes_ok; ++i) {
        writes_ok = write_fully(fds[1], chunk.data(), chunk.size(), kWriteTimeoutMs);
    }
    CHECK(writes_ok);
    CHECK(wait_pred(5000, [&] {
        return sink.bytes.load() >= static_cast<long long>(kWarmup) * kChunk;
    }));
    const long long base_bytes = sink.bytes.load();
    const int base_shots = sink.shots.load();
    const CpuSnap cpu0 = cpu_now();
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kWrites && writes_ok; ++i) {
        writes_ok = write_fully(fds[1], chunk.data(), chunk.size(), kWriteTimeoutMs);
    }
    CHECK(writes_ok);
    ::shutdown(fds[1], SHUT_WR);
    const long long want = static_cast<long long>(kWrites) * kChunk;
    CHECK(wait_pred(120000, [&] {
        return sink.bytes.load() - base_bytes >= want;
    }));
    const auto t_bytes = std::chrono::steady_clock::now();
    // 吞吐只计「写完到字节收齐」；FIN/cancel 是控制路径，不掺进带宽
    if (!eof.wait_ms(200)) {
        if (sink.op) {
            rt.ctx->cancel(*sink.op);
        }
        (void)eof.wait_ms(2000);
    }
    const double sec =
        std::chrono::duration<double>(t_bytes - t0).count();
    const int shots = sink.shots.load() - base_shots;
    const long long got = sink.bytes.load() - base_bytes;
    const std::uint64_t p50 = percentile_ns(sink.gaps_ns, 0.50);
    const std::uint64_t p99 = percentile_ns(sink.gaps_ns, 0.99);
    std::printf("\n=== stress recv_multi ===\n");
    std::printf("writes         : %d x %d B (warmup %d)\n", kWrites, kChunk,
                kWarmup);
    std::printf("bytes_got      : %lld (want %lld)\n", got, want);
    std::printf("cqe_shots      : %d\n", shots);
    std::printf("enobufs_reload : %d\n", sink.enobufs.load());
    std::printf("last_end       : %d\n", sink.end_res.load());
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("throughput     : %.2f MiB/s\n",
                sec > 0 ? (static_cast<double>(got) / (1024.0 * 1024.0)) / sec
                        : 0);
    std::printf("shots_per_sec  : %.0f\n", sec > 0 ? shots / sec : 0);
    std::printf("inter_shot_p50 : %.3f us\n", p50 / 1000.0);
    std::printf("inter_shot_p99 : %.3f us\n", p99 / 1000.0);
    print_cpu(cpu0, cpu_now(), sec);
    CHECK(got >= want * 9 / 10);
    CHECK(shots >= 1);
    print_rss("stress ");
    Gate dropped;
    rt.on_io([&] {
        sink.op.reset();
        dropped.signal();
    });
    CHECK(dropped.wait_ms(2000));
    close_fd(fds[0]);
    close_fd(fds[1]);
}

Task<void> tcp_serve_echo(TcpListener listener, std::size_t nbytes, Gate& gate)
{
    auto acc = co_await listener.accept();
    if (!acc) {
        gate.signal();
        co_return;
    }
    TcpStream s = std::move(acc.value);
    std::vector<std::byte> buf(nbytes == 0 ? 1 : nbytes);
    std::size_t got = 0;
    while (got < nbytes) {
        MutableBuffer v{buf.data() + got, buf.size() - got};
        auto r = co_await s.recv(v);
        if (!r || r.value == 0) {
            break;
        }
        got += r.value;
    }
    std::size_t sent = 0;
    while (sent < got) {
        ConstBuffer v{buf.data() + sent, got - sent};
        auto w = co_await s.send(v);
        if (!w || w.value == 0) {
            break;
        }
        sent += w.value;
    }
    gate.signal();
}

Task<void> tcp_client_echo(Context ex, Endpoint peer, std::vector<std::byte> msg,
                           std::vector<std::byte>& out, Gate& gate)
{
    auto c = co_await TcpStream::connect(ex, peer);
    if (!c) {
        gate.signal();
        co_return;
    }
    TcpStream s = std::move(c.value);
    auto w = co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
    if (!w) {
        gate.signal();
        co_return;
    }
    out.assign(msg.size(), std::byte{0});
    std::size_t got = 0;
    while (got < msg.size()) {
        auto r = co_await s.recv(MutableBuffer{out.data() + got, out.size() - got});
        if (!r || r.value == 0) {
            out.resize(got);
            break;
        }
        got += r.value;
    }
    (void)s.shutdown(Shutdown::Write);
    gate.signal();
}

void test_tcp_echo_basic()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    CHECK(peer.size() > 0);

    Gate srv;
    Gate cli;
    std::vector<std::byte> msg(13);
    std::memcpy(msg.data(), "hello-shardio", 13);
    std::vector<std::byte> got;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_serve_echo(std::move(listener), msg.size(), srv));
    });
    rt.on_io([&] {
        rt.ctx->spawn(tcp_client_echo(rt.ctx->context(), peer, msg, got, cli));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(got.size() == msg.size());
    CHECK(std::memcmp(got.data(), msg.data(), msg.size()) == 0);
}

void test_tcp_partial_sizes()
{
    const std::size_t sizes[] = {1, 63, 4095, 4096, 4097, 8193, 65536, 1 << 20};
    for (std::size_t n : sizes) {
        Runtime rt;
        auto bind = TcpListener::bind(
            rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
        CHECK(static_cast<bool>(bind));
        if (!bind) {
            return;
        }
        TcpListener listener = std::move(bind.value);
        Endpoint peer = listener.local_endpoint();
        Gate srv;
        Gate cli;
        std::vector<std::byte> msg(n);
        for (std::size_t i = 0; i < n; ++i) {
            msg[i] = static_cast<std::byte>(i & 0xff);
        }
        std::vector<std::byte> got;
        rt.on_io([&] {
            rt.ctx->spawn(tcp_serve_echo(std::move(listener), n, srv));
        });
        rt.on_io([&] {
            rt.ctx->spawn(tcp_client_echo(rt.ctx->context(), peer, msg, got, cli));
        });
        CHECK(cli.wait_ms(20000));
        CHECK(srv.wait_ms(20000));
        CHECK(got.size() == n);
        CHECK(got == msg);
    }
}

Task<void> tcp_server_drain_eof(TcpListener listener, std::vector<std::byte>& got,
                                Gate& gate)
{
    auto acc = co_await listener.accept();
    if (!acc) {
        gate.signal();
        co_return;
    }
    TcpStream s = std::move(acc.value);
    std::array<std::byte, 256> buf{};
    for (;;) {
        auto r = co_await s.recv(MutableBuffer{buf.data(), buf.size()});
        if (!r) {
            break;
        }
        if (r.value == 0) {
            break;
        }
        got.insert(got.end(), buf.data(), buf.data() + r.value);
    }
    gate.signal();
}

Task<void> tcp_client_half_close(Context ex, Endpoint peer,
                                 const std::vector<std::byte>& msg, Gate& gate)
{
    auto c = co_await TcpStream::connect(ex, peer);
    if (!c) {
        gate.signal();
        co_return;
    }
    TcpStream s = std::move(c.value);
    (void)co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
    (void)s.shutdown(Shutdown::Write);
    gate.signal();
}

void test_tcp_eof_half_close()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::vector<std::byte> msg(64, std::byte{'e'});
    std::vector<std::byte> got;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_server_drain_eof(std::move(listener), got, srv));
    });
    rt.on_io([&] {
        rt.ctx->spawn(tcp_client_half_close(rt.ctx->context(), peer, msg, cli));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(got == msg);
}

void test_tcp_econnrefused()
{
    Runtime rt;
    Gate gate;
    std::error_code err;
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Gate& g, std::error_code& e) -> Task<void> {
            auto c = co_await TcpStream::connect(
                ex, Endpoint::ipv4(INADDR_LOOPBACK, 1));
            e = c.error;
            g.signal();
            co_return;
        }(rt.ctx->context(), gate, err));
    });
    CHECK(gate.wait_ms(5000));
    CHECK(static_cast<bool>(err));
}

void test_tcp_rst()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::atomic<int> recv_err{0};
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener l, Gate& g, std::atomic<int>& recverr) -> Task<void> {
            auto acc = co_await l.accept();
            if (!acc) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(acc.value);
            linger lin{};
            lin.l_onoff = 1;
            lin.l_linger = 0;
            ::setsockopt(s.native_handle(), SOL_SOCKET, SO_LINGER, &lin, sizeof(lin));
            s.close();
            g.signal();
            (void)recverr;
            co_return;
        }(std::move(listener), srv, recv_err));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, Gate& g,
                         std::atomic<int>& recverr) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                recverr.store(c.error.value());
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            std::array<std::byte, 16> buf{};
            auto r = co_await s.recv(MutableBuffer{buf.data(), buf.size()});
            if (!r) {
                recverr.store(r.error.value());
            } else if (r.value == 0) {
                recverr.store(0);
            }
            g.signal();
            co_return;
        }(rt.ctx->context(), peer, cli, recv_err));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(recv_err.load() != 1);
}

void test_tcp_send_handle()
{
    BufferPool pool({64, 4});
    BufferPool zc_pool({kZeroCopySize, 2});
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::vector<std::byte> got;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_serve_echo(std::move(listener), 16, srv));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, BufferPool& pool, Gate& g,
                         std::vector<std::byte>& got) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            auto h = pool.try_acquire();
            if (!h) {
                g.signal();
                co_return;
            }
            const char msg[] = "handle-send-ok................";
            const std::size_t n = 16;
            h->resize(n);
            std::memcpy(h->view().data, msg, n);
            auto w = co_await s.send(ConstBuffer{h->view().data, h->size()});
            if (!w) {
                (void)s.shutdown(Shutdown::Write);
                g.signal();
                co_return;
            }
            got.assign(n, std::byte{0});
            std::size_t nread = 0;
            while (nread < n) {
                auto r = co_await s.recv(
                    MutableBuffer{got.data() + nread, got.size() - nread});
                if (!r || r.value == 0) {
                    got.resize(nread);
                    break;
                }
                nread += r.value;
            }
            (void)s.shutdown(Shutdown::Write);
            g.signal();
            co_return;
        }(rt.ctx->context(), peer, pool, cli, got));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(got.size() == 16);
    CHECK(std::memcmp(got.data(), "handle-send-ok................", 16) == 0);

    auto bind2 = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind2));
    if (!bind2) {
        return;
    }
    TcpListener listener2 = std::move(bind2.value);
    Endpoint peer2 = listener2.local_endpoint();
    Gate srv2;
    Gate cli2;
    std::atomic<int> zc_res{1};
    std::atomic<int> zc_got{0};
    rt.on_io([&] {
        rt.ctx->spawn(
            tcp_serve_echo(std::move(listener2), kZeroCopySize, srv2));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, BufferPool& pool, Gate& g,
                         std::atomic<int>& zc_res,
                         std::atomic<int>& zc_got) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            auto h = pool.try_acquire();
            if (!h) {
                g.signal();
                co_return;
            }
            h->resize(kZeroCopySize);
            std::memset(h->view().data, 'Z', kZeroCopySize);
            auto w = co_await s.send(ConstBuffer{h->view().data, h->size()}, SendOptions{.zero_copy = true});
            zc_res.store(w ? static_cast<int>(w.value) : w.error.value());
            if (!w) {
                (void)s.shutdown(Shutdown::Write);
                g.signal();
                co_return;
            }
            std::vector<std::byte> buf(kZeroCopySize);
            std::size_t nread = 0;
            while (nread < kZeroCopySize) {
                auto r = co_await s.recv(
                    MutableBuffer{buf.data() + nread, buf.size() - nread});
                if (!r || r.value == 0) {
                    break;
                }
                nread += r.value;
            }
            zc_got.store(static_cast<int>(nread));
            (void)s.shutdown(Shutdown::Write);
            g.signal();
            co_return;
        }(rt.ctx->context(), peer2, zc_pool, cli2, zc_res, zc_got));
    });
    CHECK(cli2.wait_ms(10000));
    CHECK(srv2.wait_ms(10000));
    if (zc_res.load() > 0) {
        CHECK(zc_got.load() > 0);
    }
}

void test_tcp_recv_multi_api()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    std::atomic<long long> bytes{0};
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener l, std::atomic<long long>& bytes,
                         Gate& g) -> Task<void> {
            auto acc = co_await l.accept();
            if (!acc) {
                g.signal();
                co_return;
            }
            TcpStream stream = std::move(acc.value);
            auto recv = stream.recv_multi(RecvMultiOptions{.queue_capacity = 64});
            while (auto item = co_await recv.next()) {
                bytes.fetch_add(static_cast<long long>(item.value().size()));
            }
            g.signal();
            co_return;
        }(std::move(listener), bytes, srv));
    });
    Gate cli;
    const char msg[] = "multi-via-stream";
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, Gate& g) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            const char payload[] = "multi-via-stream";
            (void)co_await s.send_all(ConstBuffer{
                reinterpret_cast<const std::byte*>(payload), sizeof(payload) - 1});
            (void)s.shutdown(Shutdown::Write);
            g.signal();
            co_return;
        }(rt.ctx->context(), peer, cli));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(bytes.load() >= static_cast<long long>(sizeof(msg) - 1));
}

void test_tcp_accept_multi_api()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate started;
    Gate ended;
    std::atomic<int> shots{0};
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener listener, std::atomic<int>& shots,
                         Gate& started, Gate& ended) -> Task<void> {
            auto accepts = listener.accept_multi(
                AcceptMultiOptions{.queue_capacity = 8});
            started.signal();
            for (int i = 0; i < 2; ++i) {
                auto item = co_await accepts.next();
                if (!item) break;
                ++shots;
            }
            accepts.cancel();
            (void)co_await accepts.next();
            ended.signal();
        }(std::move(listener), shots, started, ended));
    });
    CHECK(started.wait_ms(2000));
    int cli = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(cli >= 0);
    sockaddr_in addr{};
    CHECK(peer.size() >= static_cast<socklen_t>(sizeof(sockaddr_in)));
    std::memcpy(&addr, peer.native(), sizeof(addr));
    CHECK(::connect(cli, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    int c2 = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(c2 >= 0);
    CHECK(::connect(c2, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    CHECK(wait_pred(3000, [&] { return shots.load() >= 2; }));
    CHECK(ended.wait_ms(3000));
    close_fd(cli);
    close_fd(c2);
}

Task<void> echo_until_eof(TcpStream stream);

Task<void> consume_accepts(TcpListener& listener,
                           std::unique_ptr<AcceptStream>& stream,
                           std::atomic<int>& accepted, Gate& started,
                           Gate& ended)
{
    stream = std::make_unique<AcceptStream>(listener.accept_multi());
    started.signal();
    while (auto item = co_await stream->next()) {
        accepted.fetch_add(1, std::memory_order_relaxed);
    }
    ended.signal();
}

Task<void> consume_echo_accepts(Context context, TcpListener& listener,
                                std::unique_ptr<AcceptStream>& stream,
                                std::atomic<int>& accepted, Gate& started,
                                Gate& ended)
{
    stream = std::make_unique<AcceptStream>(listener.accept_multi());
    started.signal();
    while (auto item = co_await stream->next()) {
        accepted.fetch_add(1, std::memory_order_relaxed);
        context.spawn(echo_until_eof(std::move(item).value()));
    }
    ended.signal();
}

Task<void> send_view(TcpStream& s, ConstBuffer v, Gate& g)
{
    (void)co_await s.send_all(v);
    g.signal();
}

Task<void> recv_exact(TcpStream& s, std::vector<std::byte>& out, std::size_t n, Gate& g)
{
    out.assign(n, std::byte{0});
    std::size_t got = 0;
    while (got < n) {
        auto r = co_await s.recv(MutableBuffer{out.data() + got, n - got});
        if (!r || r.value == 0) {
            out.resize(got);
            break;
        }
        got += r.value;
    }
    g.signal();
}

void test_tcp_bidirectional()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    std::vector<std::byte> c2s(256, std::byte{'c'});
    std::vector<std::byte> s2c(256, std::byte{'s'});
    std::vector<std::byte> srv_got;
    std::vector<std::byte> cli_got;
    Gate srv_send;
    Gate srv_recv;
    Gate cli_send;
    Gate cli_recv;
    std::optional<TcpStream> srv;
    std::optional<TcpStream> cli;
    rt.on_io([&] {
        rt.ctx->spawn([](IoContext& ctx, TcpListener l, std::optional<TcpStream>& held,
                         ConstBuffer out, std::vector<std::byte>& got,
                         std::size_t expect, Gate& gs, Gate& gr) -> Task<void> {
            auto acc = co_await l.accept();
            if (!acc) {
                gs.signal();
                gr.signal();
                co_return;
            }
            held = std::move(acc.value);
            ctx.spawn(send_view(*held, out, gs));
            ctx.spawn(recv_exact(*held, got, expect, gr));
        }(*rt.ctx, std::move(listener), srv, ConstBuffer{s2c.data(), s2c.size()},
                       srv_got, c2s.size(), srv_send, srv_recv));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](IoContext& ctx, Context ex, Endpoint p,
                         std::optional<TcpStream>& held, ConstBuffer out,
                         std::vector<std::byte>& got, std::size_t expect, Gate& gs,
                         Gate& gr) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                gs.signal();
                gr.signal();
                co_return;
            }
            held = std::move(c.value);
            ctx.spawn(send_view(*held, out, gs));
            ctx.spawn(recv_exact(*held, got, expect, gr));
        }(*rt.ctx, rt.ctx->context(), peer, cli, ConstBuffer{c2s.data(), c2s.size()},
                       cli_got, s2c.size(), cli_send, cli_recv));
    });
    CHECK(srv_send.wait_ms(5000));
    CHECK(srv_recv.wait_ms(5000));
    CHECK(cli_send.wait_ms(5000));
    CHECK(cli_recv.wait_ms(5000));
    CHECK(srv_got == c2s);
    CHECK(cli_got == s2c);
}

void test_tcp_epipe()
{
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::atomic<int> send_err{0};
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener l, Gate& g) -> Task<void> {
            auto acc = co_await l.accept();
            if (acc) {
                TcpStream s = std::move(acc.value);
                s.close();
            }
            g.signal();
        }(std::move(listener), srv));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, Gate& g,
                         std::atomic<int>& send_err) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            std::vector<std::byte> msg(256 * 1024, std::byte{'e'});
            for (int i = 0; i < 8; ++i) {
                auto w = co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
                if (!w) {
                    send_err.store(w.error.value());
                    break;
                }
            }
            g.signal();
        }(rt.ctx->context(), peer, cli, send_err));
    });
    CHECK(cli.wait_ms(10000));
    CHECK(send_err.load() != 0);
}

void test_send_strategy_threshold()
{
    const std::size_t sizes[] = {kZeroCopySize - 1, kZeroCopySize,
                                 kZeroCopySize + 1};
    for (std::size_t n : sizes) {
        BufferPool pool({n, 2});
        Runtime rt;
        auto bind = TcpListener::bind(
            rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
        CHECK(static_cast<bool>(bind));
        if (!bind) {
            return;
        }
        TcpListener listener = std::move(bind.value);
        Endpoint peer = listener.local_endpoint();
        Gate srv;
        Gate cli;
        std::vector<std::byte> got;
        rt.on_io([&] {
            rt.ctx->spawn(tcp_serve_echo(std::move(listener), n, srv));
        });
        rt.on_io([&] {
            rt.ctx->spawn([](Context ex, Endpoint p, BufferPool& pool,
                             std::size_t n, std::vector<std::byte>& got,
                             Gate& g) -> Task<void> {
                auto c = co_await TcpStream::connect(ex, p);
                if (!c) {
                    g.signal();
                    co_return;
                }
                TcpStream s = std::move(c.value);
                auto h = pool.try_acquire();
                if (!h) {
                    g.signal();
                    co_return;
                }
                h->resize(n);
                std::memset(h->view().data, 'T', n);
                auto w = co_await s.send(
                    ConstBuffer{h->view().data, h->size()},
                    SendOptions{.zero_copy = n >= kZeroCopySize});
                if (!w) {
                    g.signal();
                    co_return;
                }
                got.assign(n, std::byte{0});
                std::size_t off = 0;
                while (off < n) {
                    auto r = co_await s.recv(MutableBuffer{got.data() + off, n - off});
                    if (!r || r.value == 0) {
                        got.resize(off);
                        break;
                    }
                    off += r.value;
                }
                (void)s.shutdown(Shutdown::Write);
                g.signal();
            }(rt.ctx->context(), peer, pool, n, got, cli));
        });
        CHECK(cli.wait_ms(15000));
        CHECK(srv.wait_ms(15000));
        CHECK(got.size() == n);
        CHECK(got[0] == std::byte{'T'});
        CHECK(got.back() == std::byte{'T'});
        CHECK(pool.available() == pool.capacity());
    }
}

void test_send_pipeline()
{
    BufferPool pool({64, 8});
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::atomic<long long> got{0};
    std::atomic<int> submitted{0};
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener l, std::atomic<long long>& got, Gate& g) -> Task<void> {
            auto acc = co_await l.accept();
            if (!acc) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(acc.value);
            std::array<std::byte, 256> buf{};
            for (;;) {
                auto r = co_await s.recv(MutableBuffer{buf.data(), buf.size()});
                if (!r || r.value == 0) {
                    break;
                }
                got.fetch_add(static_cast<long long>(r.value));
            }
            g.signal();
        }(std::move(listener), got, srv));
    });
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, BufferPool& pool, Gate& g,
                         std::atomic<int>& submitted) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            std::array<std::byte, 64> payload{};
            payload.fill(std::byte{'P'});

            // 默认入口自行创建 4 个稳定发送缓冲块。
            auto pipe = s.send_pipeline({
                .depth = 4, .buffer_size = payload.size(), .zero_copy = true});
            for (int i = 0; i < 4; ++i) {
                if (co_await pipe.send(ConstBuffer{payload.data(), payload.size()})) {
                    submitted.fetch_add(1);
                }
            }
            (void)(co_await pipe.close());

            // 高级入口复用调用方提供的池，覆盖 registered/pinned 等扩展路径。
            auto external = s.send_pipeline(
                {.depth = 4, .buffer_size = payload.size(), .zero_copy = true},
                pool);
            for (int i = 0; i < 4; ++i) {
                if (co_await external.send(
                        ConstBuffer{payload.data(), payload.size()})) {
                    submitted.fetch_add(1);
                }
            }
            (void)(co_await external.close());
            (void)s.shutdown(Shutdown::Write);
            g.signal();
        }(rt.ctx->context(), peer, pool, cli, submitted));
    });
    CHECK(cli.wait_ms(10000));
    CHECK(srv.wait_ms(10000));
    CHECK(submitted.load() == 8);
    if (got.load() == 0) {
        std::printf("send_pipeline: no bytes (send_zc unsupported?)\n");
    } else {
        CHECK(got.load() == 8 * 64);
    }
}

void test_send_pipeline_close_lifecycle()
{
    constexpr std::size_t kSize = 1024 * 1024;
    BufferPool pool({kSize, 2});
    int fds[2]{-1, -1};
    make_socketpair(fds);
    Runtime rt;
    Gate done;
    std::atomic<bool> restored{false};

    rt.on_io([&] {
        rt.ctx->spawn([](Context context, int fd, BufferPool& pool,
                         std::atomic<bool>& restored, Gate& done) -> Task<void> {
            TcpStream stream(fd, context);
            std::vector<std::byte> payload(kSize, std::byte{'L'});
            auto pipe = stream.send_pipeline(
                {.depth = 2, .buffer_size = kSize, .zero_copy = true}, pool);
            (void)(co_await pipe.send(
                ConstBuffer{payload.data(), payload.size()}));

            // 连接关闭必须先停止/取消流水线，再等 CQE 与 ZC 通知退休。
            stream.close();
            (void)(co_await pipe.drain());
            restored.store(pool.available() == pool.capacity(),
                           std::memory_order_release);
            done.signal();
        }(rt.ctx->context(), fds[0], pool, restored, done));
    });

    CHECK(done.wait_ms(10000));
    CHECK(restored.load(std::memory_order_acquire));
    close_fd(fds[1]);
}

void test_batch_public_api()
{
    int first[2]{-1, -1};
    int second[2]{-1, -1};
    make_socketpair(first);
    make_socketpair(second);
    Runtime rt;
    Gate done;
    std::array<std::byte, 8> one{};
    std::array<std::byte, 8> two{};
    one.fill(std::byte{'1'});
    two.fill(std::byte{'2'});
    const int first_owned = first[0];
    const int second_owned = second[0];
    rt.on_io([&, first_owned, second_owned] {
        rt.ctx->context().spawn(
            [](Context context, int first_fd, int second_fd,
               std::array<std::byte, 8>& one,
               std::array<std::byte, 8>& two, Gate& done) -> Task<void> {
                TcpStream first_stream(first_fd, context);
                TcpStream second_stream(second_fd, context);
                auto batch = context.batch();
                batch.send(first_stream, ConstBuffer{one.data(), one.size()});
                batch.send(second_stream, ConstBuffer{two.data(), two.size()});
                auto result = co_await batch.submit();
                CHECK(static_cast<bool>(result));
                done.signal();
            }(rt.ctx->context(), first_owned, second_owned, one, two, done));
    });
    first[0] = -1;
    second[0] = -1;
    CHECK(done.wait_ms(3000));
    std::array<std::byte, 8> received{};
    CHECK(::recv(first[1], received.data(), received.size(), MSG_WAITALL) == 8);
    CHECK(received[0] == std::byte{'1'});
    CHECK(::recv(second[1], received.data(), received.size(), MSG_WAITALL) == 8);
    CHECK(received[0] == std::byte{'2'});
    close_fd(first[1]);
    close_fd(second[1]);
}

void test_io_context_pool()
{
    IoContextPool pool(PoolOptions{
        .io_workers = 2,
        .queue_depth = 64,
        .pin_threads = false,
    });
    CHECK(pool.size() == 2);
    pool.start();
    std::atomic<int> owners{0};
    Gate owner0;
    Gate owner1;
    pool.context(0).post([&] {
        if (pool.context(0).in_this_context() &&
            !pool.context(1).in_this_context()) {
            owners.fetch_add(1);
        }
        owner0.signal();
    });
    pool.context(1).post([&] {
        if (pool.context(1).in_this_context() &&
            !pool.context(0).in_this_context()) {
            owners.fetch_add(1);
        }
        owner1.signal();
    });
    CHECK(owner0.wait_ms(3000));
    CHECK(owner1.wait_ms(3000));
    CHECK(owners.load() == 2);

    auto bind = TcpListener::bind(
        pool.context(0), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        pool.stop();
        pool.join();
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    Gate cli;
    std::atomic<int> pinned{0};
    pool.context(0).post([&] {
        pool.context(0).spawn(
            [](TcpListener l, Context self, Context other, std::atomic<int>& pinned,
               Gate& g) -> Task<void> {
                auto acc = co_await l.accept();
                if (!acc) {
                    g.signal();
                    co_return;
                }
                TcpStream s = std::move(acc.value);
                if (self.in_this_context() && !other.in_this_context()) {
                    pinned.fetch_add(1);
                }
                std::array<std::byte, 8> buf{};
                (void)co_await s.recv(MutableBuffer{buf.data(), buf.size()});
                if (self.in_this_context()) {
                    pinned.fetch_add(1);
                }
                g.signal();
            }(std::move(listener), pool.context(0), pool.context(1), pinned, srv));
    });
    pool.context(1).post([&] {
        pool.context(1).spawn([](Context ex, Endpoint p, Gate& g) -> Task<void> {
            auto c = co_await TcpStream::connect(ex, p);
            if (!c) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(c.value);
            std::array<std::byte, 8> msg{};
            msg.fill(std::byte{'a'});
            (void)co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
            g.signal();
        }(pool.context(1), peer, cli));
    });
    CHECK(cli.wait_ms(5000));
    CHECK(srv.wait_ms(5000));
    CHECK(pinned.load() == 2);
    pool.stop();
    pool.join();
    CHECK(owners.load() == 2);
}

void test_reuseport_listeners()
{
    constexpr int kN = 1024;
    IoContextPool pool(PoolOptions{
        .io_workers = 2,
        .queue_depth = 512,
        .pin_threads = true,
    });
    pool.start();
    auto b0 = TcpListener::bind(
        pool.context(0), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = kN, .reuse_port = true});
    CHECK(static_cast<bool>(b0));
    if (!b0) {
        pool.stop();
        pool.join();
        return;
    }
    TcpListener l0 = std::move(b0.value);
    const std::uint16_t port = ipv4_port(l0.local_endpoint());
    auto b1 = TcpListener::bind(
        pool.context(1), Endpoint::ipv4(INADDR_LOOPBACK, port),
        ListenOptions{.backlog = kN, .reuse_port = true});
    CHECK(static_cast<bool>(b1));
    if (!b1) {
        pool.stop();
        pool.join();
        return;
    }
    TcpListener l1 = std::move(b1.value);
    std::atomic<int> accepted0{0};
    std::atomic<int> accepted1{0};
    std::unique_ptr<AcceptStream> stream0;
    std::unique_ptr<AcceptStream> stream1;
    Gate started0, started1, ended0, ended1;
    pool.context(0).post([&] {
        pool.context(0).spawn(
            consume_accepts(l0, stream0, accepted0, started0, ended0));
    });
    pool.context(1).post([&] {
        pool.context(1).spawn(
            consume_accepts(l1, stream1, accepted1, started1, ended1));
    });
    CHECK(started0.wait_ms(3000));
    CHECK(started1.wait_ms(3000));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    std::vector<int> clients;
    clients.reserve(static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            break;
        }
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            break;
        }
        clients.push_back(fd);
    }
    CHECK(wait_pred(30000, [&] {
        return accepted0.load() + accepted1.load() >=
               static_cast<int>(clients.size());
    }));
    const int a = accepted0.load();
    const int b = accepted1.load();
    const double avg = (a + b) / 2.0;
    const double var =
        ((a - avg) * (a - avg) + (b - avg) * (b - avg)) / 2.0;
    std::printf("\n=== reuseport accept_count ===\n");
    std::printf("worker[0]       : %d\n", a);
    std::printf("worker[1]       : %d\n", b);
    std::printf("worker_load_min : %d\n", a < b ? a : b);
    std::printf("worker_load_max : %d\n", a > b ? a : b);
    std::printf("worker_load_avg : %.1f\n", avg);
    std::printf("worker_load_stddev : %.1f\n", std::sqrt(var));
    pool.context(0).post([&] { stream0->cancel(); });
    pool.context(1).post([&] { stream1->cancel(); });
    CHECK(ended0.wait_ms(3000));
    CHECK(ended1.wait_ms(3000));
    for (int fd : clients) {
        int x = fd;
        close_fd(x);
    }
    pool.stop();
    pool.join();
}

void test_sq_full_pending()
{
    Runtime rt(8);
    constexpr int kN = 32;
    std::vector<std::unique_ptr<TimeoutOp>> ops;
    ops.reserve(static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        ops.emplace_back(std::make_unique<TimeoutOp>(5 * 1000 * 1000ULL));
    }
    rt.on_io([&] {
        for (auto& op : ops) {
            rt.ctx->submit(*op);
        }
    });
    bool all = true;
    for (auto& op : ops) {
        if (!wait_done(rt, *op, 5000)) {
            all = false;
        }
    }
    CHECK(all);
}

Task<void> echo_until_eof(TcpStream s)
{
    std::array<std::byte, 64> buf{};
    for (;;) {
        auto r = co_await s.recv(MutableBuffer{buf.data(), buf.size()});
        if (!r || r.value == 0) {
            break;
        }
        auto w = co_await s.send(ConstBuffer{buf.data(), r.value});
        if (!w || w.value == 0) {
            break;
        }
    }
}

struct EchoJoin {
    std::atomic<int> left{1}; /**< 1=accept 循环；每 spawn +1，结束 -1，减到 0 时 signal */
    Gate* gate{nullptr};
};

Task<void> tcp_echo_one(TcpStream s, std::shared_ptr<EchoJoin> join)
{
    co_await echo_until_eof(std::move(s));
    if (join->left.fetch_sub(1) == 1) {
        join->gate->signal();
    }
}

Task<void> tcp_accept_n(IoContext& ctx, TcpListener listener, int n, Gate& gate)
{
    auto join = std::make_shared<EchoJoin>();
    join->gate = &gate;
    for (int i = 0; i < n; ++i) {
        auto acc = co_await listener.accept();
        if (!acc) {
            break;
        }
        // 连接各自一条协程回显，避免 32 路串行把 p99 拉成排队延迟
        join->left.fetch_add(1);
        ctx.spawn(tcp_echo_one(std::move(acc.value), join));
    }
    if (join->left.fetch_sub(1) == 1) {
        gate.signal();
    }
}

Task<void> tcp_ping_n(Context ex, Endpoint peer, int rounds, Gate& gate,
                      std::vector<std::uint64_t>& lat, std::atomic<int>& ok)
{
    auto c = co_await TcpStream::connect(ex, peer);
    if (!c) {
        gate.signal();
        co_return;
    }
    TcpStream s = std::move(c.value);
    std::array<std::byte, 64> msg{};
    std::array<std::byte, 64> buf{};
    msg.fill(std::byte{'p'});
    for (int i = 0; i < rounds; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        auto w = co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
        if (!w) {
            break;
        }
        std::size_t got = 0;
        while (got < msg.size()) {
            auto r = co_await s.recv(MutableBuffer{buf.data() + got, buf.size() - got});
            if (!r || r.value == 0) {
                gate.signal();
                co_return;
            }
            got += r.value;
        }
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        if (lat.size() < lat.capacity()) {
            lat.push_back(static_cast<std::uint64_t>(ns));
        }
        ok.fetch_add(1);
    }
    s.close();
    gate.signal();
}

void test_concurrent_echo()
{
    constexpr int kClients = 64;
    constexpr int kRounds = 400;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = 128});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_accept_n(*rt.ctx, std::move(listener), kClients, srv));
    });
    std::vector<Gate> gates(static_cast<std::size_t>(kClients));
    std::vector<std::vector<std::uint64_t>> lats(static_cast<std::size_t>(kClients));
    std::atomic<int> ok{0};
    for (int i = 0; i < kClients; ++i) {
        lats[static_cast<std::size_t>(i)].reserve(static_cast<std::size_t>(kRounds));
    }
    const CpuSnap cpu0 = cpu_now();
    const auto t0 = std::chrono::steady_clock::now();
    rt.on_io([&] {
        for (int i = 0; i < kClients; ++i) {
            rt.ctx->spawn(tcp_ping_n(rt.ctx->context(), peer, kRounds,
                                     gates[static_cast<std::size_t>(i)],
                                     lats[static_cast<std::size_t>(i)], ok));
        }
    });
    for (auto& g : gates) {
        CHECK(g.wait_ms(120000));
    }
    CHECK(srv.wait_ms(120000));
    CHECK(ok.load() >= kClients * kRounds / 2);
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    const CpuSnap cpu1 = cpu_now();
    std::vector<std::uint64_t> all;
    for (auto& v : lats) {
        all.insert(all.end(), v.begin(), v.end());
    }
    print_bench("concurrent_echo_64x400", sec, ok.load(), 0, std::move(all));
    print_cpu(cpu0, cpu1, sec);
}

void bench_small_messages()
{
    const int sizes[] = {64, 256, 1024};
    for (int sz : sizes) {
        Runtime rt;
        auto bind = TcpListener::bind(
            rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
        CHECK(static_cast<bool>(bind));
        if (!bind) {
            return;
        }
        TcpListener listener = std::move(bind.value);
        Endpoint peer = listener.local_endpoint();
        constexpr int kIters = 20000;
        Gate srv;
        rt.on_io([&] {
            rt.ctx->spawn(
                [](TcpListener l, int nbytes, int iters, Gate& g) -> Task<void> {
                    auto acc = co_await l.accept();
                    if (!acc) {
                        g.signal();
                        co_return;
                    }
                    TcpStream s = std::move(acc.value);
                    std::vector<std::byte> buf(static_cast<std::size_t>(nbytes));
                    for (int i = 0; i < iters; ++i) {
                        std::size_t got = 0;
                        while (got < static_cast<std::size_t>(nbytes)) {
                            auto r = co_await s.recv(
                                MutableBuffer{buf.data() + got, buf.size() - got});
                            if (!r || r.value == 0) {
                                g.signal();
                                co_return;
                            }
                            got += r.value;
                        }
                        (void)co_await s.send_all(
                            ConstBuffer{buf.data(), buf.size()});
                    }
                    g.signal();
                }(std::move(listener), sz, kIters, srv));
        });
        std::vector<std::uint64_t> lat;
        lat.reserve(static_cast<std::size_t>(kIters));
        Gate cli;
        const CpuSnap cpu0 = cpu_now();
        const auto t0 = std::chrono::steady_clock::now();
        rt.on_io([&] {
            rt.ctx->spawn([](Context ex, Endpoint p, int nbytes, int iters,
                             std::vector<std::uint64_t>& lat, Gate& g) -> Task<void> {
                auto c = co_await TcpStream::connect(ex, p);
                if (!c) {
                    g.signal();
                    co_return;
                }
                TcpStream s = std::move(c.value);
                std::vector<std::byte> msg(static_cast<std::size_t>(nbytes),
                                           std::byte{'b'});
                std::vector<std::byte> buf(static_cast<std::size_t>(nbytes));
                for (int i = 0; i < iters; ++i) {
                    const auto t = std::chrono::steady_clock::now();
                    auto w = co_await s.send_all(
                        ConstBuffer{msg.data(), msg.size()});
                    if (!w) {
                        break;
                    }
                    std::size_t got = 0;
                    while (got < msg.size()) {
                        auto r = co_await s.recv(
                            MutableBuffer{buf.data() + got, buf.size() - got});
                        if (!r || r.value == 0) {
                            g.signal();
                            co_return;
                        }
                        got += r.value;
                    }
                    lat.push_back(static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t)
                            .count()));
                }
                g.signal();
                co_return;
            }(rt.ctx->context(), peer, sz, kIters, lat, cli));
        });
        CHECK(cli.wait_ms(120000));
        CHECK(srv.wait_ms(120000));
        const double sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        char name[64];
        std::snprintf(name, sizeof(name), "small_%dB", sz);
        const auto n = static_cast<long long>(lat.size());
        print_bench(name, sec, n, n * sz, std::move(lat));
        print_cpu(cpu0, cpu_now(), sec);
    }
}

void bench_large_transfer()
{
    constexpr std::size_t kBytes = 32 << 20;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_serve_echo(std::move(listener), kBytes, srv));
    });
    Gate cli;
    std::vector<std::byte> msg(kBytes, std::byte{'L'});
    std::vector<std::byte> got;
    const auto t0 = std::chrono::steady_clock::now();
    rt.on_io([&] {
        rt.ctx->spawn(tcp_client_echo(rt.ctx->context(), peer, msg, got, cli));
    });
    CHECK(cli.wait_ms(120000));
    CHECK(srv.wait_ms(120000));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    CHECK(got == msg);
    print_bench("large_32MiB", sec, 1, static_cast<long long>(kBytes), {});
}

void bench_connect_rate()
{
    constexpr int kN = 2000;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = kN});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    rt.on_io([&] {
        rt.ctx->spawn([](TcpListener l, int n, Gate& g) -> Task<void> {
            for (int i = 0; i < n; ++i) {
                auto acc = co_await l.accept();
                if (!acc) {
                    break;
                }
                acc.value.close();
            }
            g.signal();
        }(std::move(listener), kN, srv));
    });
    Gate cli;
    const auto t0 = std::chrono::steady_clock::now();
    rt.on_io([&] {
        rt.ctx->spawn([](Context ex, Endpoint p, int n, Gate& g) -> Task<void> {
            for (int i = 0; i < n; ++i) {
                auto c = co_await TcpStream::connect(ex, p);
                if (c) {
                    c.value.close();
                }
            }
            g.signal();
        }(rt.ctx->context(), peer, kN, cli));
    });
    CHECK(cli.wait_ms(30000));
    CHECK(srv.wait_ms(30000));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    std::printf("\n=== bench connect_rate ===\n");
    std::printf("connections    : %d\n", kN);
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("conn_per_sec   : %.0f\n", sec > 0 ? kN / sec : 0);
    print_rss("");
}

void bench_oneshot_vs_multi()
{
    // 公平对比：两边 recv 容量 = 写入块；64MiB。不要 oneshot 1K vs Ring 4K。
    constexpr int kChunk = 4096;
    constexpr int kWrites = (64 << 20) / kChunk;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    {
        Runtime rt;
        RecvOp* ops = nullptr;
        std::vector<std::byte> buf(static_cast<std::size_t>(kChunk));
        std::atomic<long long> got{0};
        Gate done;
        const CpuSnap cpu0 = cpu_now();
        const auto t0 = std::chrono::steady_clock::now();
        rt.on_io([&] {
            rt.ctx->spawn([](int fd, std::vector<std::byte>& buf, int writes,
                             int chunk, std::atomic<long long>& got, Context ex,
                             Gate& g) -> Task<void> {
                for (int i = 0; i < writes; ++i) {
                    RecvOp op(fd, MutableBuffer{buf.data(), buf.size()});
                    const int res = co_await OpAwaiter(ex, op);
                    if (res <= 0) {
                        break;
                    }
                    got.fetch_add(res);
                }
                g.signal();
                co_return;
            }(fds[0], buf, kWrites, kChunk, got, rt.ctx->context(), done));
        });
        std::vector<char> chunk(static_cast<std::size_t>(kChunk), 'o');
        set_nonblock(fds[1]);
        bool ok = true;
        for (int i = 0; i < kWrites && ok; ++i) {
            ok = write_fully(fds[1], chunk.data(), chunk.size(), 60000);
        }
        ::shutdown(fds[1], SHUT_WR);
        CHECK(done.wait_ms(120000));
        const double sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        print_bench("oneshot_recv_4k", sec, kWrites, got.load(), {});
        print_cpu(cpu0, cpu_now(), sec);
        (void)ops;
    }
    close_fd(fds[0]);
    close_fd(fds[1]);
    make_socketpair(fds);
    {
        BufferPool pool({static_cast<unsigned>(kChunk), 512});
        BufferRing ring(pool, BufferRing::Config{
            .group_id = 7,
            .capacity = 512,
            .reclaim_batch = 32,
            .low_watermark = 64,
        });
        Runtime rt;
        Gate armed;
        rt.on_io([&] {
            rt.backend->register_buffer_ring(ring);
            ring.provide_all();
            armed.signal();
        });
        CHECK(armed.wait_ms(2000));
        RecvSinkStress sink;
        sink.rt = &rt;
        sink.ring = &ring;
        sink.fd = fds[0];
        Gate ended;
        sink.eof_gate = &ended;
        rt.on_io([&] { sink.arm(); });
        std::vector<char> chunk(static_cast<std::size_t>(kChunk), 'm');
        set_nonblock(fds[1]);
        const CpuSnap cpu0 = cpu_now();
        const auto t0 = std::chrono::steady_clock::now();
        bool ok = true;
        for (int i = 0; i < kWrites && ok; ++i) {
            ok = write_fully(fds[1], chunk.data(), chunk.size(), 60000);
        }
        ::shutdown(fds[1], SHUT_WR);
        CHECK(wait_pred(120000, [&] {
            return sink.bytes.load() >= static_cast<long long>(kWrites) * kChunk;
        }));
        const double sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        if (!ended.wait_ms(200)) {
            if (sink.op) {
                rt.ctx->cancel(*sink.op);
            }
            (void)ended.wait_ms(2000);
        }
        print_bench("multishot_recv_4k", sec, sink.shots.load(), sink.bytes.load(),
                    sink.gaps_ns, sink.enobufs.load());
        print_cpu(cpu0, cpu_now(), sec);
    }
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_wakeup_burst()
{
    Runtime rt;
    std::atomic<int> n{0};
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    constexpr int kN = 20000;
    for (int i = 0; i < kN; ++i) {
        rt.ctx->post([&] { n.fetch_add(1); });
    }
    CHECK(wait_pred(30000, [&] { return n.load() == kN; }));
}

void test_sq_full_tiny()
{
    Runtime rt(4);
    constexpr int kN = 64;
    std::vector<std::unique_ptr<TimeoutOp>> ops;
    ops.reserve(static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        ops.emplace_back(std::make_unique<TimeoutOp>(2 * 1000 * 1000ULL));
    }
    rt.on_io([&] {
        for (auto& op : ops) {
            rt.ctx->submit(*op);
        }
    });
    bool all = true;
    for (auto& op : ops) {
        if (!wait_done(rt, *op, 5000)) {
            all = false;
        }
    }
    CHECK(all);
}

void test_accept_multi_burst()
{
    constexpr int kN = 1024;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = kN});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    std::atomic<int> accepted{0};
    std::unique_ptr<AcceptStream> accepts;
    Gate ended;
    Gate started;
    rt.on_io([&] {
        rt.ctx->spawn(consume_accepts(
            listener, accepts, accepted, started, ended));
    });
    CHECK(started.wait_ms(2000));
    sockaddr_in addr{};
    std::memcpy(&addr, peer.native(), sizeof(addr));
    std::vector<int> clients;
    clients.reserve(static_cast<std::size_t>(kN));
    for (int i = 0; i < kN; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0 ||
            ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            if (fd >= 0) {
                ::close(fd);
            }
            break;
        }
        clients.push_back(fd);
    }
    CHECK(wait_pred(30000, [&] {
        return accepted.load() >= static_cast<int>(clients.size());
    }));
    CHECK(static_cast<int>(clients.size()) == kN);
    rt.on_io([&] { accepts->cancel(); });
    CHECK(ended.wait_ms(3000));
    CHECK(rt.barrier());
    for (int fd : clients) {
        int x = fd;
        close_fd(x);
    }
}

void test_send_zc_cancel()
{
    int fds[2]{-1, -1};
    make_socketpair(fds);
    BufferPool pool({kZeroCopySize, 1});
    Runtime rt;
    auto h = pool.try_acquire();
    CHECK(static_cast<bool>(h));
    if (!h) {
        close_fd(fds[0]);
        close_fd(fds[1]);
        return;
    }
    h->resize(kZeroCopySize);
    SendZcOp op(fds[0], std::move(*h));
    rt.on_io([&] { rt.ctx->submit(op); });
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    rt.ctx->cancel(op);
    CHECK(wait_done(rt, op, 3000));
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_chaos_seeded()
{
    constexpr std::uint32_t kSeed = 1;
    std::mt19937 rng{kSeed};
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = 32});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_accept_n(*rt.ctx, std::move(listener), 16, srv));
    });
    std::atomic<int> done{0};
    for (int i = 0; i < 16; ++i) {
        const int nbytes = 1 + static_cast<int>(rng() % 64);
        rt.on_io([&, nbytes] {
            rt.ctx->spawn([](Context ex, Endpoint p, int nbytes,
                             std::atomic<int>& done) -> Task<void> {
                auto c = co_await TcpStream::connect(ex, p);
                if (!c) {
                    done.fetch_add(1);
                    co_return;
                }
                TcpStream s = std::move(c.value);
                std::vector<std::byte> msg(static_cast<std::size_t>(nbytes),
                                           std::byte{'x'});
                (void)co_await s.send_all(ConstBuffer{msg.data(), msg.size()});
                std::array<std::byte, 64> buf{};
                (void)co_await s.recv(MutableBuffer{buf.data(), buf.size()});
                if ((nbytes & 1) != 0) {
                    (void)s.shutdown(Shutdown::Write);
                }
                s.close();
                done.fetch_add(1);
            }(rt.ctx->context(), peer, nbytes, done));
        });
    }
    CHECK(wait_pred(10000, [&] { return done.load() >= 16; }));
    CHECK(srv.wait_ms(10000));
    std::printf("chaos seed=%u done=%d\n", kSeed, done.load());
}

void test_high_concurrency_echo()
{
    constexpr int kClients = 128;
    constexpr int kRounds = 400;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = kClients});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    Gate srv;
    rt.on_io([&] {
        rt.ctx->spawn(tcp_accept_n(*rt.ctx, std::move(listener), kClients, srv));
    });
    std::vector<Gate> gates(static_cast<std::size_t>(kClients));
    std::vector<std::vector<std::uint64_t>> lats(static_cast<std::size_t>(kClients));
    std::atomic<int> ok{0};
    for (int i = 0; i < kClients; ++i) {
        lats[static_cast<std::size_t>(i)].reserve(static_cast<std::size_t>(kRounds));
    }
    const auto t0 = std::chrono::steady_clock::now();
    rt.on_io([&] {
        for (int i = 0; i < kClients; ++i) {
            rt.ctx->spawn(tcp_ping_n(rt.ctx->context(), peer, kRounds,
                                     gates[static_cast<std::size_t>(i)],
                                     lats[static_cast<std::size_t>(i)], ok));
        }
    });
    for (auto& g : gates) {
        CHECK(g.wait_ms(120000));
    }
    CHECK(srv.wait_ms(120000));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    CHECK(ok.load() >= kClients * kRounds / 2);
    std::vector<std::uint64_t> all;
    for (auto& v : lats) {
        all.insert(all.end(), v.begin(), v.end());
    }
    print_bench("high_concurrency_128x400", sec, ok.load(),
                static_cast<long long>(ok.load()) * 64, std::move(all));
}

void bench_send_vs_send_zc()
{
    const std::size_t sizes[] = {4096, 16384, 32768, 65536};
    constexpr int kIters = 256;
    for (std::size_t n : sizes) {
        auto serve = [](TcpListener l, std::size_t n, int iters, Gate& g) -> Task<void> {
            auto acc = co_await l.accept();
            if (!acc) {
                g.signal();
                co_return;
            }
            TcpStream s = std::move(acc.value);
            std::vector<std::byte> buf(n);
            for (int i = 0; i < iters; ++i) {
                std::size_t got = 0;
                while (got < n) {
                    auto r = co_await s.recv(MutableBuffer{buf.data() + got, n - got});
                    if (!r || r.value == 0) {
                        g.signal();
                        co_return;
                    }
                    got += r.value;
                }
                (void)co_await s.send_all(ConstBuffer{buf.data(), n});
            }
            g.signal();
        };
        {
            Runtime rt;
            auto bind = TcpListener::bind(
                rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
            CHECK(static_cast<bool>(bind));
            if (!bind) {
                return;
            }
            TcpListener listener = std::move(bind.value);
            Endpoint peer = listener.local_endpoint();
            Gate srv;
            Gate cli;
            const std::size_t total = n * static_cast<std::size_t>(kIters);
            rt.on_io([&] { rt.ctx->spawn(serve(std::move(listener), n, kIters, srv)); });
            std::vector<std::byte> msg(n, std::byte{'v'});
            const auto t0 = std::chrono::steady_clock::now();
            rt.on_io([&] {
                rt.ctx->spawn([](Context ex, Endpoint p, std::vector<std::byte> msg,
                                 int iters, Gate& g) -> Task<void> {
                    auto c = co_await TcpStream::connect(ex, p);
                    if (!c) {
                        g.signal();
                        co_return;
                    }
                    TcpStream s = std::move(c.value);
                    std::vector<std::byte> buf(msg.size());
                    for (int i = 0; i < iters; ++i) {
                        (void)co_await s.send_all(
                            ConstBuffer{msg.data(), msg.size()});
                        std::size_t off = 0;
                        while (off < buf.size()) {
                            auto r = co_await s.recv(
                                MutableBuffer{buf.data() + off, buf.size() - off});
                            if (!r || r.value == 0) {
                                g.signal();
                                co_return;
                            }
                            off += r.value;
                        }
                    }
                    (void)s.shutdown(Shutdown::Write);
                    g.signal();
                }(rt.ctx->context(), peer, msg, kIters, cli));
            });
            CHECK(cli.wait_ms(120000));
            CHECK(srv.wait_ms(120000));
            const double sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                    .count();
            char name[64];
            std::snprintf(name, sizeof(name), "send_view_%zuB", n);
            print_bench(name, sec, kIters, static_cast<long long>(total), {});
        }
        {
            BufferPool pool({n, 4});
            Runtime rt;
            auto bind = TcpListener::bind(
                rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0));
            CHECK(static_cast<bool>(bind));
            if (!bind) {
                return;
            }
            TcpListener listener = std::move(bind.value);
            Endpoint peer = listener.local_endpoint();
            Gate srv;
            Gate cli;
            const std::size_t total = n * static_cast<std::size_t>(kIters);
            rt.on_io([&] { rt.ctx->spawn(serve(std::move(listener), n, kIters, srv)); });
            const auto t0 = std::chrono::steady_clock::now();
            rt.on_io([&] {
                rt.ctx->spawn([](Context ex, Endpoint p, BufferPool& pool,
                                 std::size_t n, int iters, Gate& g) -> Task<void> {
                    auto c = co_await TcpStream::connect(ex, p);
                    if (!c) {
                        g.signal();
                        co_return;
                    }
                    TcpStream s = std::move(c.value);
                    std::vector<std::byte> buf(n);
                    for (int i = 0; i < iters; ++i) {
                        auto h = pool.try_acquire();
                        if (!h) {
                            break;
                        }
                        h->resize(n);
                        std::memset(h->view().data, 'z', n);
                        (void)co_await s.send(
                            ConstBuffer{h->view().data, h->size()},
                            SendOptions{.zero_copy = true});
                        std::size_t off = 0;
                        while (off < n) {
                            auto r = co_await s.recv(
                                MutableBuffer{buf.data() + off, n - off});
                            if (!r || r.value == 0) {
                                g.signal();
                                co_return;
                            }
                            off += r.value;
                        }
                    }
                    (void)s.shutdown(Shutdown::Write);
                    g.signal();
                }(rt.ctx->context(), peer, pool, n, kIters, cli));
            });
            CHECK(cli.wait_ms(120000));
            CHECK(srv.wait_ms(120000));
            const double sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                    .count();
            char name[64];
            std::snprintf(name, sizeof(name), "send_handle_%zuB", n);
            print_bench(name, sec, kIters, static_cast<long long>(total), {});
        }
    }
}

void bench_connect_accept_multi()
{
    constexpr int kN = 2000;
    Runtime rt;
    auto bind = TcpListener::bind(
        rt.ctx->context(), Endpoint::ipv4(INADDR_LOOPBACK, 0),
        ListenOptions{.backlog = kN});
    CHECK(static_cast<bool>(bind));
    if (!bind) {
        return;
    }
    TcpListener listener = std::move(bind.value);
    Endpoint peer = listener.local_endpoint();
    std::atomic<int> accepted{0};
    std::unique_ptr<AcceptStream> accepts;
    Gate started;
    Gate ended;
    rt.on_io([&] {
        rt.ctx->spawn(consume_accepts(
            listener, accepts, accepted, started, ended));
    });
    CHECK(started.wait_ms(3000));
    sockaddr_in addr{};
    std::memcpy(&addr, peer.native(), sizeof(addr));
    const auto t0 = std::chrono::steady_clock::now();
    std::vector<int> clients;
    clients.reserve(static_cast<std::size_t>(kN));
    int failed = 0;
    for (int i = 0; i < kN; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0 ||
            ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            if (fd >= 0) {
                ::close(fd);
            }
            ++failed;
            continue;
        }
        clients.push_back(fd);
    }
    CHECK(wait_pred(30000, [&] {
        return accepted.load() >= static_cast<int>(clients.size());
    }));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    std::printf("\n=== bench connect_accept_multi ===\n");
    std::printf("connections    : %zu\n", clients.size());
    std::printf("failed         : %d\n", failed);
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("conn_per_sec   : %.0f\n",
                sec > 0 ? static_cast<double>(clients.size()) / sec : 0);
    print_rss("");
    rt.on_io([&] { accepts->cancel(); });
    CHECK(ended.wait_ms(3000));
    for (int fd : clients) {
        int x = fd;
        close_fd(x);
    }
}

void bench_worker_scaling()
{
    // 128 连接样本太少，REUSEPORT 会歪；拉长到秒级，避免 0.2s 噪声当退化
    constexpr int kClients = 1024;
    constexpr int kRounds = 400;
    const std::size_t ns[] = {1, 2, 4};
    double base = 0;
    for (std::size_t workers : ns) {
        IoContextPool pool(PoolOptions{
            .io_workers = workers,
            .queue_depth = 1024,
            .pin_threads = true,
        });
        pool.start();
        auto b0 = TcpListener::bind(
            pool.context(0), Endpoint::ipv4(INADDR_LOOPBACK, 0),
            ListenOptions{.backlog = kClients, .reuse_port = true});
        CHECK(static_cast<bool>(b0));
        if (!b0) {
            pool.stop();
            pool.join();
            return;
        }
        std::vector<TcpListener> ls;
        ls.push_back(std::move(b0.value));
        const std::uint16_t port = ipv4_port(ls[0].local_endpoint());
        for (std::size_t w = 1; w < workers; ++w) {
            auto b = TcpListener::bind(
                pool.context(w), Endpoint::ipv4(INADDR_LOOPBACK, port),
                ListenOptions{.backlog = kClients, .reuse_port = true});
            CHECK(static_cast<bool>(b));
            if (!b) {
                pool.stop();
                pool.join();
                return;
            }
            ls.push_back(std::move(b.value));
        }
        std::vector<std::unique_ptr<std::atomic<int>>> accepted;
        std::vector<std::unique_ptr<AcceptStream>> accept_streams(workers);
        std::vector<Gate> accept_started(workers);
        std::vector<Gate> accept_ended(workers);
        accepted.reserve(workers);
        for (std::size_t w = 0; w < workers; ++w) {
            accepted.push_back(std::make_unique<std::atomic<int>>(0));
        }
        for (std::size_t w = 0; w < workers; ++w) {
            pool.context(w).post([&, w] {
                pool.context(w).spawn(consume_echo_accepts(
                    pool.context(w), ls[w], accept_streams[w], *accepted[w],
                    accept_started[w], accept_ended[w]));
            });
            CHECK(accept_started[w].wait_ms(3000));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        Endpoint peer = Endpoint::ipv4(INADDR_LOOPBACK, port);
        std::vector<Gate> gates(static_cast<std::size_t>(kClients));
        std::vector<std::vector<std::uint64_t>> lats(static_cast<std::size_t>(kClients));
        std::atomic<int> ok{0};
        for (int i = 0; i < kClients; ++i) {
            lats[static_cast<std::size_t>(i)].reserve(static_cast<std::size_t>(kRounds));
        }
        const CpuSnap cpu0 = cpu_now();
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < kClients; ++i) {
            const std::size_t w = static_cast<std::size_t>(i) % workers;
            pool.context(w).post([&, i, w] {
                pool.context(w).spawn(tcp_ping_n(
                    pool.context(w), peer, kRounds, gates[static_cast<std::size_t>(i)],
                    lats[static_cast<std::size_t>(i)], ok));
            });
        }
        for (auto& g : gates) {
            CHECK(g.wait_ms(120000));
        }
        const double sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        const CpuSnap cpu1 = cpu_now();
        std::vector<std::uint64_t> all;
        for (auto& v : lats) {
            all.insert(all.end(), v.begin(), v.end());
        }
        char name[64];
        std::snprintf(name, sizeof(name), "scale_%zu_workers", workers);
        print_bench(name, sec, ok.load(),
                    static_cast<long long>(ok.load()) * 64, std::move(all));
        print_cpu(cpu0, cpu1, sec);
        const double thr = sec > 0 ? static_cast<double>(ok.load()) / sec : 0;
        if (workers == 1) {
            base = thr;
        }
        std::printf("workers         : %zu\n", workers);
        std::printf("accepted        :");
        for (std::size_t w = 0; w < workers; ++w) {
            std::printf(" %d", accepted[w]->load());
        }
        std::printf("\n");
        if (base > 0) {
            std::printf("scaling         : %.2f\n", thr / base);
            std::printf("efficiency      : %.0f%%\n",
                        (thr / (static_cast<double>(workers) * base)) * 100.0);
        }
        for (std::size_t w = 0; w < workers; ++w) {
            pool.context(w).post([&, w] { accept_streams[w]->cancel(); });
        }
        for (std::size_t w = 0; w < workers; ++w) {
            CHECK(accept_ended[w].wait_ms(3000));
        }
        pool.stop();
        pool.join();
    }
}

void burn_spin(unsigned n)
{
    volatile unsigned x = 1;
    for (unsigned i = 0; i < n; ++i) {
        x = x * 1664525u + 1013904223u;
    }
    (void)x;
}

Task<void> yield_mark(IoContext* ctx, std::atomic<int>* seq, int* saw, Gate* done)
{
    seq->store(1, std::memory_order_relaxed);
    co_await ctx->yield();
    *saw = seq->load(std::memory_order_relaxed);
    seq->store(3, std::memory_order_relaxed);
    done->signal();
}

Task<void> yield_n(IoContext* ctx, int n, Gate* done)
{
    for (int i = 0; i < n; ++i) {
        co_await ctx->yield();
    }
    done->signal();
}

Task<void> yield_reenter(IoContext* ctx, std::atomic<int>* ticks, int n, Gate* done)
{
    for (int i = 0; i < n; ++i) {
        co_await ctx->yield();
        ticks->fetch_add(1, std::memory_order_relaxed);
    }
    done->signal();
}

Task<void> burn_until(IoContext* ctx, std::atomic<bool>* stop, unsigned mask,
                      Gate* done, std::atomic<std::uint64_t>* work)
{
    unsigned n = 0;
    while (!stop->load(std::memory_order_relaxed)) {
        burn_spin(256);
        ++n;
        if (work != nullptr) {
            work->fetch_add(1, std::memory_order_relaxed);
        }
        if (mask != 0 && (n & mask) == 0) {
            co_await ctx->yield();
        }
        if (n > 40000000u) {
            break;
        }
    }
    done->signal();
}

Task<void> recv_once(Context ex, RecvOp* op, Gate* done)
{
    (void)co_await OpAwaiter(ex, *op);
    done->signal();
}

void test_yield_basic()
{
    Runtime rt;
    Gate a_done;
    Gate b_done;
    std::atomic<int> seq{0};
    int saw = 0;
    rt.on_io([&] {
        rt.ctx->spawn(yield_mark(rt.ctx.get(), &seq, &saw, &a_done));
        rt.ctx->post([&] {
            seq.store(2, std::memory_order_relaxed);
            b_done.signal();
        });
    });
    CHECK(a_done.wait_ms(2000));
    CHECK(b_done.wait_ms(2000));
    CHECK(saw == 2);
    CHECK(seq.load() == 3);
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.suspends >= 1);
    CHECK(y.suspends == y.resumes);
    CHECK(y.pending == 0);
    print_yield("yield_basic", y);
}

void test_yield_solo()
{
    Runtime rt;
    Gate done;
    rt.on_io([&] { rt.ctx->spawn(yield_n(rt.ctx.get(), 32, &done)); });
    CHECK(done.wait_ms(2000));
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.suspends == 32);
    CHECK(y.resumes == 32);
    CHECK(y.pending == 0);
    print_yield("yield_solo", y);
}

void test_cpu_starvation()
{
    const char msg[] = "y";
    auto run_one = [&](unsigned mask, const char* name, bool cpu_first) {
        Runtime rt;
        int fds[2]{-1, -1};
        make_socketpair(fds);
        char buf[8]{};
        RecvOp recv_op(fds[0],
                       MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
        std::atomic<bool> stop{false};
        Gate cpu_done;
        Gate recv_done;
        rt.on_io([&] {
            if (cpu_first) {
                rt.ctx->spawn(burn_until(rt.ctx.get(), &stop, mask, &cpu_done, nullptr));
                rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
            } else {
                rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
                rt.ctx->spawn(burn_until(rt.ctx.get(), &stop, mask, &cpu_done, nullptr));
            }
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        const auto t_send = std::chrono::steady_clock::now();
        CHECK(write_fully(fds[1], msg, 1, 1000));
        double us = 0;
        if (mask == 0) {
            const bool early = recv_done.wait_ms(80);
            CHECK(!early);
            stop.store(true, std::memory_order_relaxed);
            CHECK(cpu_done.wait_ms(2000));
            CHECK(recv_done.wait_ms(2000));
            us = std::chrono::duration<double, std::micro>(
                     std::chrono::steady_clock::now() - t_send)
                     .count();
        } else {
            CHECK(recv_done.wait_ms(200));
            us = std::chrono::duration<double, std::micro>(
                     std::chrono::steady_clock::now() - t_send)
                     .count();
            stop.store(true, std::memory_order_relaxed);
            CHECK(cpu_done.wait_ms(2000));
        }
        print_yield(name, *rt.ctx);
        std::printf("recv_latency_us: %.1f\n", us);
        close_fd(fds[0]);
        close_fd(fds[1]);
        return us;
    };
    const double starved = run_one(0, "cpu_no_yield", true);
    const double fair_cpu_first = run_one(63, "cpu_yield_then_recv", true);
    const double fair_recv_first = run_one(63, "recv_then_cpu_yield", false);
    CHECK(starved > 50.0);
    CHECK(fair_cpu_first < 5000.0);
    CHECK(fair_recv_first < 5000.0);
}

void test_post_starvation()
{
    Runtime rt;
    std::atomic<bool> stop{false};
    Gate cpu_done;
    rt.on_io([&] {
        rt.ctx->spawn(burn_until(rt.ctx.get(), &stop, 63, &cpu_done, nullptr));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::vector<std::uint64_t> delays;
    delays.reserve(64);
    for (int i = 0; i < 64; ++i) {
        Gate g;
        const auto t0 = std::chrono::steady_clock::now();
        rt.ctx->post([&] {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            delays.push_back(static_cast<std::uint64_t>(ns));
            g.signal();
        });
        CHECK(g.wait_ms(1000));
    }
    stop.store(true, std::memory_order_relaxed);
    CHECK(cpu_done.wait_ms(2000));
    const std::uint64_t p50 = percentile_ns(delays, 0.50);
    const std::uint64_t p99 = percentile_ns(delays, 0.99);
    std::uint64_t mx = 0;
    for (auto d : delays) {
        mx = std::max(mx, d);
    }
    const YieldSnap y = snap_yield(*rt.ctx);
    print_yield("post_starvation", y);
    std::printf("post_delay_p50  : %.3f us\n", p50 / 1000.0);
    std::printf("post_delay_p99  : %.3f us\n", p99 / 1000.0);
    std::printf("post_delay_max  : %.3f us\n", mx / 1000.0);
    CHECK(mx < 200000000ull);
    CHECK(y.suspends == y.resumes);
}

void test_cancel_starvation()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    char buf[16]{};
    RecvOp recv_op(fds[0], MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
    std::atomic<bool> stop{false};
    Gate cpu_done;
    rt.on_io([&] {
        rt.ctx->submit(recv_op);
        rt.ctx->spawn(burn_until(rt.ctx.get(), &stop, 63, &cpu_done, nullptr));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    rt.ctx->cancel(recv_op);
    CHECK(wait_done(rt, recv_op, 2000));
    stop.store(true, std::memory_order_relaxed);
    CHECK(cpu_done.wait_ms(2000));
    CHECK(recv_op.result() == -ECANCELED);
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.pending == 0);
    print_yield("cancel_starvation", y);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_yield_reentrant()
{
    Runtime rt;
    Gate a_done;
    Gate b_done;
    std::atomic<int> ticks{0};
    rt.on_io([&] {
        rt.ctx->spawn(yield_reenter(rt.ctx.get(), &ticks, 24, &a_done));
        rt.ctx->spawn(yield_reenter(rt.ctx.get(), &ticks, 24, &b_done));
    });
    CHECK(a_done.wait_ms(2000));
    CHECK(b_done.wait_ms(2000));
    CHECK(ticks.load() == 48);
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.suspends == y.resumes);
    CHECK(y.pending == 0);
    CHECK(y.pending_max >= 1);
    print_yield("yield_reentrant", y);
}

void test_yield_stop()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    char buf[8]{};
    RecvOp recv_op(fds[0], MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
    std::atomic<bool> stop{false};
    Gate cpu_done;
    Gate y_done;
    rt.on_io([&] {
        rt.ctx->submit(recv_op);
        rt.ctx->spawn(yield_n(rt.ctx.get(), 8, &y_done));
        rt.ctx->spawn(burn_until(rt.ctx.get(), &stop, 63, &cpu_done, nullptr));
    });
    CHECK(y_done.wait_ms(2000));
    rt.ctx->cancel(recv_op);
    CHECK(wait_done(rt, recv_op, 2000));
    stop.store(true, std::memory_order_relaxed);
    CHECK(cpu_done.wait_ms(2000));
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.pending == 0);
    print_yield("yield_stop", y);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_yield_handle_lifetime()
{
    Runtime rt;
    Gate done;
    std::atomic<int> seq{0};
    int saw = 0;
    rt.on_io([&] {
        rt.ctx->spawn(yield_mark(rt.ctx.get(), &seq, &saw, &done));
        rt.ctx->post([&] { seq.store(2, std::memory_order_relaxed); });
    });
    CHECK(done.wait_ms(2000));
    CHECK(saw == 2);
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.suspends == y.resumes);
    CHECK(y.pending == 0);
    print_yield("yield_handle_lifetime", y);
}

Task<void> async_cpu_add(IoContext* ctx, int* out, Gate* done)
{
    *out = co_await ctx->async_cpu([] { return 40 + 2; });
    done->signal();
}

void test_async_cpu()
{
    Runtime rt;
    Gate done;
    int out = 0;
    rt.on_io([&] { rt.ctx->spawn(async_cpu_add(rt.ctx.get(), &out, &done)); });
    CHECK(done.wait_ms(2000));
    CHECK(out == 42);
}

Task<void> async_cpu_sleep(IoContext* ctx, int ms, Gate* done)
{
    co_await ctx->async_cpu([ms] {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    });
    if (done != nullptr) {
        done->signal();
    }
}

Task<void> async_cpu_count(IoContext* ctx, int ms, std::atomic<int>* done)
{
    co_await ctx->async_cpu([ms] {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    });
    done->fetch_add(1, std::memory_order_relaxed);
}

void test_async_cpu_starvation()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    char buf[8]{};
    RecvOp recv_op(fds[0],
                   MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
    Gate cpu_done;
    Gate recv_done;
    const CpuSnap cpu0 = cpu_now();
    const auto t0 = std::chrono::steady_clock::now();
    rt.on_io([&] {
        rt.ctx->spawn(async_cpu_sleep(rt.ctx.get(), 120, &cpu_done));
        rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    const char msg[] = "y";
    const auto t_send = std::chrono::steady_clock::now();
    CHECK(write_fully(fds[1], msg, 1, 1000));
    CHECK(recv_done.wait_ms(200));
    const double recv_us = std::chrono::duration<double, std::micro>(
                               std::chrono::steady_clock::now() - t_send)
                               .count();
    CHECK(cpu_done.wait_ms(2000));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    print_yield("async_cpu_starvation", *rt.ctx);
    std::printf("recv_latency_us: %.1f\n", recv_us);
    std::printf("cpu_job_ms     : 120\n");
    print_cpu(cpu0, cpu_now(), sec);
    CHECK(recv_us < 5000.0);
    CHECK(recv_op.result() == 1);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_async_cpu_stop()
{
    Runtime rt;
    constexpr int kN = 4;
    std::atomic<int> done{0};
    rt.on_io([&] {
        for (int i = 0; i < kN; ++i) {
            rt.ctx->spawn(async_cpu_count(rt.ctx.get(), 50, &done));
        }
    });
    CHECK(wait_pred(1000, [&] {
        return rt.ctx->cpu_executor().inflight() > 0 || done.load() > 0;
    }));
    const auto t0 = std::chrono::steady_clock::now();
    rt.ctx->stop();
    if (rt.io.joinable()) {
        rt.io.join();
    }
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    print_yield("async_cpu_stop", *rt.ctx);
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("cpu_jobs_done  : %d\n", done.load());
    std::printf("cpu_inflight   : %zu\n", rt.ctx->cpu_executor().inflight());
    CHECK(done.load() == kN);
    CHECK(rt.ctx->cpu_executor().inflight() == 0);
    CHECK(sec < 2.0);
}

void test_scheduler_fairness()
{
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    constexpr int kYielders = 80;
    constexpr int kYields = 4;
    std::vector<Gate> ydone(static_cast<std::size_t>(kYielders));
    rt.on_io([&] {
        for (int i = 0; i < kYielders; ++i) {
            rt.ctx->spawn(
                yield_n(rt.ctx.get(), kYields, &ydone[static_cast<std::size_t>(i)]));
        }
    });
    std::vector<std::uint64_t> lat;
    lat.reserve(32);
    const char msg[] = "k";
    char buf[8]{};
    for (int i = 0; i < 16; ++i) {
        RecvOp recv_op(fds[0],
                       MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
        Gate recv_done;
        rt.on_io([&] {
            rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
        });
        const auto ts = std::chrono::steady_clock::now();
        CHECK(write_fully(fds[1], msg, 1, 1000));
        CHECK(recv_done.wait_ms(2000));
        lat.push_back(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - ts)
                .count()));
    }
    for (auto& g : ydone) {
        CHECK(g.wait_ms(5000));
    }
    const std::uint64_t p50 = percentile_ns(lat, 0.50);
    const std::uint64_t p99 = percentile_ns(lat, 0.99);
    const YieldSnap y = snap_yield(*rt.ctx);
    print_yield("scheduler_fairness", y);
    std::printf("yielders       : %d\n", kYielders);
    std::printf("yields_each    : %d\n", kYields);
    std::printf("recv_p50_us    : %.3f\n", p50 / 1000.0);
    std::printf("recv_p99_us    : %.3f\n", p99 / 1000.0);
    CHECK(y.suspends == y.resumes);
    CHECK(y.pending == 0);
    CHECK(p99 < 200000000ull);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void test_executor_cross_thread_stress()
{
    Runtime rt;
    Context ex = rt.ctx->context();
    constexpr int kT = 8;
    constexpr int kN = 4000;
    std::atomic<int> n{0};
    const CpuSnap cpu0 = cpu_now();
    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> ts;
    ts.reserve(static_cast<std::size_t>(kT));
    for (int t = 0; t < kT; ++t) {
        ts.emplace_back([&] {
            for (int i = 0; i < kN; ++i) {
                ex.post([&] { n.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& th : ts) {
        th.join();
    }
    CHECK(wait_pred(30000, [&] { return n.load() == kT * kN; }));
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    const long long ops = static_cast<long long>(kT) * kN;
    std::printf("\n=== executor_cross_thread_stress ===\n");
    std::printf("elapsed_sec    : %.4f\n", sec);
    std::printf("ops            : %lld\n", ops);
    std::printf("throughput     : %.0f ops/s\n", sec > 0 ? ops / sec : 0);
    print_cpu(cpu0, cpu_now(), sec);
    print_rss("");
    CHECK(n.load() == kT * kN);
}

void test_post_overflow()
{
    {
        CrossThreadInbox box(8);
        constexpr int kN = 64;
        std::vector<int> order;
        order.reserve(static_cast<std::size_t>(kN));
        for (int i = 0; i < kN; ++i) {
            box.push([&order, i] { order.push_back(i); });
        }
        box.drain([](TaskFn task) { task(); });
        CHECK(static_cast<int>(order.size()) == kN);
        for (int i = 0; i < kN; ++i) {
            CHECK(order[static_cast<std::size_t>(i)] == i);
        }
        CHECK(box.empty());
        std::printf("\n=== post_overflow_inbox ===\n");
        std::printf("fast_capacity  : 8\n");
        std::printf("posted         : %d\n", kN);
        std::printf("ran            : %zu\n", order.size());
    }
    Runtime rt;
    Gate hold;
    Gate released;
    rt.ctx->post([&] {
        hold.signal();
        (void)released.wait_ms(2000);
    });
    CHECK(hold.wait_ms(2000));
    constexpr int kT = 4;
    constexpr int kN = 512;
    std::atomic<int> n{0};
    std::vector<std::thread> ts;
    ts.reserve(static_cast<std::size_t>(kT));
    for (int t = 0; t < kT; ++t) {
        ts.emplace_back([&] {
            for (int i = 0; i < kN; ++i) {
                rt.ctx->post([&] { n.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& th : ts) {
        th.join();
    }
    released.signal();
    CHECK(wait_pred(10000, [&] { return n.load() == kT * kN; }));
    std::printf("\n=== post_overflow ===\n");
    std::printf("posted         : %d\n", kT * kN);
    std::printf("ran            : %d\n", n.load());
    print_rss("");
}

void test_stop_with_inflight_io()
{
    BufferPool pool({128, 8});
    BufferRing ring(pool, BufferRing::Config{.group_id = 2, .capacity = 8});
    RecvSinkDrop sink;
    int sp_recv[2]{-1, -1};
    int sp_send[2]{-1, -1};
    int sp_multi[2]{-1, -1};
    make_socketpair(sp_recv);
    make_socketpair(sp_send);
    make_socketpair(sp_multi);
    sockaddr_in addr{};
    int listen_fd = make_listen_loopback(addr);
    std::vector<std::byte> send_buf(256 * 1024, std::byte{'s'});
    char recv_buf[16]{};
    RecvOp recv_op(sp_recv[0],
                   MutableBuffer{reinterpret_cast<std::byte*>(recv_buf), sizeof(recv_buf)});
    SendOp send_op(sp_send[0], MutableBuffer{send_buf.data(), send_buf.size()});
    AcceptOp acc(listen_fd);
    RecvMultiOp multi(sp_multi[0], ring, sink);
    Runtime rt;
    Gate y_done;
    Gate cpu_done;
    rt.on_io([&] {
        rt.backend->register_buffer_ring(ring);
        ring.provide_all();
        rt.ctx->submit(recv_op);
        rt.ctx->submit(send_op);
        rt.ctx->submit(acc);
        rt.ctx->submit(multi);
        rt.ctx->spawn(yield_n(rt.ctx.get(), 8, &y_done));
        rt.ctx->spawn(async_cpu_sleep(rt.ctx.get(), 30, &cpu_done));
    });
    CHECK(y_done.wait_ms(2000));
    rt.ctx->cancel(recv_op);
    rt.ctx->cancel(send_op);
    rt.ctx->cancel(acc);
    rt.ctx->cancel(multi);
    const bool recv_ok = wait_done(rt, recv_op, 3000);
    const bool send_ok = wait_done(rt, send_op, 3000);
    const bool acc_ok = wait_done(rt, acc, 3000);
    const bool multi_ok = wait_done(rt, multi, 3000);
    CHECK(recv_ok);
    CHECK(send_ok);
    CHECK(acc_ok);
    CHECK(multi_ok);
    CHECK(cpu_done.wait_ms(2000));
    const YieldSnap y = snap_yield(*rt.ctx);
    CHECK(y.pending == 0);
    print_yield("stop_with_inflight_io", y);
    std::printf("recv_done      : %d\n", recv_ok ? 1 : 0);
    std::printf("send_done      : %d\n", send_ok ? 1 : 0);
    std::printf("accept_done    : %d\n", acc_ok ? 1 : 0);
    std::printf("recv_multi_end : %d\n", sink.end_res.load());
    std::printf("cpu_inflight   : %zu\n", rt.ctx->cpu_executor().inflight());
    print_rss("");
    const auto t0 = std::chrono::steady_clock::now();
    rt.ctx->stop();
    if (rt.io.joinable()) {
        rt.io.join();
    }
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    std::printf("stop_join_sec  : %.4f\n", sec);
    CHECK(sec < 2.0);
    close_fd(sp_recv[0]);
    close_fd(sp_recv[1]);
    close_fd(sp_send[0]);
    close_fd(sp_send[1]);
    close_fd(sp_multi[0]);
    close_fd(sp_multi[1]);
    close_fd(listen_fd);
}

void test_chaos_scheduler()
{
    constexpr std::uint32_t kSeed = 7;
    std::mt19937 rng{kSeed};
    Runtime rt;
    int fds[2]{-1, -1};
    make_socketpair(fds);
    std::atomic<int> posts{0};
    std::atomic<int> cpu_done{0};
    std::atomic<int> yields_done{0};
    int recvs_done = 0;
    constexpr int kYielders = 24;
    constexpr int kCpu = 8;
    constexpr int kPosts = 256;
    constexpr int kRecvs = 8;
    std::array<int, kYielders> yield_counts{};
    std::array<int, kCpu> cpu_delays{};
    std::array<bool, kRecvs> cancel_recv{};
    // 随机计划在单线程预生成，worker 只读取，避免测试自身竞争污染 TSan。
    for (int& count : yield_counts) {
        count = 1 + static_cast<int>(rng() % 4);
    }
    for (int& delay : cpu_delays) {
        delay = 1 + static_cast<int>(rng() % 8);
    }
    for (std::size_t i = 0; i < cancel_recv.size(); ++i) {
        cancel_recv[i] = (rng() & 1u) != 0;
    }
    rt.on_io([&] {
        for (int i = 0; i < kYielders; ++i) {
            const int n = yield_counts[static_cast<std::size_t>(i)];
            rt.ctx->spawn([](IoContext* ctx, int n, std::atomic<int>* done) -> Task<void> {
                for (int i = 0; i < n; ++i) {
                    co_await ctx->yield();
                }
                done->fetch_add(1, std::memory_order_relaxed);
            }(rt.ctx.get(), n, &yields_done));
        }
        for (int i = 0; i < kCpu; ++i) {
            const int ms = cpu_delays[static_cast<std::size_t>(i)];
            rt.ctx->spawn(async_cpu_count(rt.ctx.get(), ms, &cpu_done));
        }
    });
    std::vector<std::thread> ts;
    for (int t = 0; t < 4; ++t) {
        ts.emplace_back([&] {
            for (int i = 0; i < kPosts / 4; ++i) {
                rt.ctx->post([&] { posts.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& th : ts) {
        th.join();
    }
    char rbuf[8]{};
    for (int i = 0; i < kRecvs; ++i) {
        RecvOp op(fds[0],
                  MutableBuffer{reinterpret_cast<std::byte*>(rbuf), sizeof(rbuf)});
        rt.on_io([&] { rt.ctx->submit(op); });
        if (cancel_recv[static_cast<std::size_t>(i)]) {
            rt.ctx->cancel(op);
        } else {
            const char x = 'x';
            CHECK(write_fully(fds[1], &x, 1, 1000));
        }
        CHECK(wait_done(rt, op, 3000));
        ++recvs_done;
    }
    CHECK(wait_pred(5000, [&] { return yields_done.load() == kYielders; }));
    CHECK(wait_pred(5000, [&] { return cpu_done.load() == kCpu; }));
    CHECK(wait_pred(5000, [&] { return posts.load() == kPosts; }));
    const YieldSnap y = snap_yield(*rt.ctx);
    print_yield("chaos_scheduler", y);
    std::printf("seed            : %u\n", kSeed);
    std::printf("posts           : %d\n", posts.load());
    std::printf("cpu_jobs        : %d\n", cpu_done.load());
    std::printf("yielders        : %d\n", yields_done.load());
    std::printf("recvs           : %d\n", recvs_done);
    print_rss("");
    CHECK(y.suspends == y.resumes);
    CHECK(y.pending == 0);
    close_fd(fds[0]);
    close_fd(fds[1]);
}

void bench_yield_checkpoint()
{
    const unsigned masks[] = {7, 15, 31, 63, 127, 255};
    const char msg[] = "k";
    constexpr auto kRun = std::chrono::milliseconds(1000);
    for (unsigned mask : masks) {
        Runtime rt;
        int fds[2]{-1, -1};
        make_socketpair(fds);
        char buf[8]{};
        std::vector<std::uint64_t> lat;
        lat.reserve(4096);
        std::atomic<bool> stop{false};
        std::atomic<std::uint64_t> work{0};
        Gate cpu_done;
        rt.on_io([&] {
            rt.ctx->spawn(
                burn_until(rt.ctx.get(), &stop, mask, &cpu_done, &work));
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        for (int i = 0; i < 8; ++i) {
            RecvOp recv_op(fds[0],
                           MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
            Gate recv_done;
            rt.on_io([&] {
                rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
            });
            CHECK(write_fully(fds[1], msg, 1, 1000));
            CHECK(recv_done.wait_ms(2000));
        }
        work.store(0, std::memory_order_relaxed);
        const CpuSnap cpu0 = cpu_now();
        const auto t0 = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - t0 < kRun) {
            RecvOp recv_op(fds[0],
                           MutableBuffer{reinterpret_cast<std::byte*>(buf), sizeof(buf)});
            Gate recv_done;
            rt.on_io([&] {
                rt.ctx->spawn(recv_once(rt.ctx->context(), &recv_op, &recv_done));
            });
            const auto ts = std::chrono::steady_clock::now();
            CHECK(write_fully(fds[1], msg, 1, 1000));
            CHECK(recv_done.wait_ms(2000));
            lat.push_back(static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - ts)
                    .count()));
        }
        const double sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
                .count();
        const CpuSnap cpu1 = cpu_now();
        const std::uint64_t cpu_ops = work.load(std::memory_order_relaxed);
        stop.store(true, std::memory_order_relaxed);
        CHECK(cpu_done.wait_ms(2000));
        const std::uint64_t p50 = percentile_ns(lat, 0.50);
        const std::uint64_t p99 = percentile_ns(lat, 0.99);
        char name[64];
        std::snprintf(name, sizeof(name), "yield_checkpoint_%u", mask + 1);
        print_yield(name, *rt.ctx);
        std::printf("checkpoint     : %u\n", mask + 1);
        std::printf("elapsed_sec    : %.4f\n", sec);
        std::printf("cpu_work_ops   : %llu\n",
                    static_cast<unsigned long long>(cpu_ops));
        std::printf("cpu_work_ops/s : %.0f\n",
                    sec > 0 ? static_cast<double>(cpu_ops) / sec : 0);
        print_cpu(cpu0, cpu1, sec);
        std::printf("p50_us         : %.3f\n", p50 / 1000.0);
        std::printf("p99_us         : %.3f\n", p99 / 1000.0);
        close_fd(fds[0]);
        close_fd(fds[1]);
    }
}

}  // namespace

struct TestCase {
    const char* name;
    const char* group;
    void (*run)();
};

int main(int argc, char** argv)
{
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);

    const TestCase cases[] = {
        {"pool_basic", "correctness", test_pool_basic}, {"post_wakeup_stop", "correctness", test_post_wakeup_stop},
        {"send_recv_oneshot", "correctness", test_send_recv_oneshot}, {"registered_recv", "correctness", test_registered_recv},
        {"recv_multi_and_reclaim", "correctness", test_recv_multi_and_reclaim}, {"recv_multi_enobufs_corner", "corner", test_recv_multi_enobufs_corner},
        {"recv_multi_enobufs_refill", "corner", test_recv_multi_enobufs_refill}, {"connect_accept", "correctness", test_connect_accept},
        {"send_zc", "correctness", test_send_zc}, {"timeout", "correctness", test_timeout}, {"cancel_recv", "corner", test_cancel_recv},
        {"yield_basic", "scheduler", test_yield_basic}, {"yield_solo", "scheduler", test_yield_solo},
        {"cpu_starvation", "scheduler", test_cpu_starvation}, {"post_starvation", "scheduler", test_post_starvation},
        {"cancel_starvation", "scheduler", test_cancel_starvation}, {"yield_reentrant", "scheduler", test_yield_reentrant},
        {"yield_stop", "scheduler", test_yield_stop}, {"yield_handle_lifetime", "scheduler", test_yield_handle_lifetime},
        {"async_cpu", "scheduler", test_async_cpu}, {"async_cpu_starvation", "scheduler", test_async_cpu_starvation},
        {"async_cpu_stop", "scheduler", test_async_cpu_stop}, {"scheduler_fairness", "scheduler", test_scheduler_fairness},
        {"executor_cross_thread_stress", "stress", test_executor_cross_thread_stress}, {"post_overflow", "corner", test_post_overflow},
        {"stop_with_inflight_io", "corner", test_stop_with_inflight_io}, {"accept_multi", "correctness", test_accept_multi},
        {"tcp_echo_basic", "correctness", test_tcp_echo_basic}, {"tcp_partial_sizes", "corner", test_tcp_partial_sizes},
        {"tcp_eof_half_close", "corner", test_tcp_eof_half_close}, {"tcp_econnrefused", "corner", test_tcp_econnrefused},
        {"tcp_rst", "corner", test_tcp_rst}, {"tcp_send_handle", "correctness", test_tcp_send_handle},
        {"tcp_recv_multi_api", "correctness", test_tcp_recv_multi_api}, {"tcp_accept_multi_api", "correctness", test_tcp_accept_multi_api},
        {"tcp_bidirectional", "correctness", test_tcp_bidirectional}, {"tcp_epipe", "corner", test_tcp_epipe},
        {"send_strategy_threshold", "correctness", test_send_strategy_threshold}, {"send_pipeline", "correctness", test_send_pipeline},
        {"send_pipeline_close_lifecycle", "corner", test_send_pipeline_close_lifecycle},
        {"batch_public_api", "correctness", test_batch_public_api}, {"io_context_pool", "correctness", test_io_context_pool},
        {"reuseport_listeners", "stress", test_reuseport_listeners},
        {"sq_full_pending", "corner", test_sq_full_pending}, {"concurrent_echo", "stress", test_concurrent_echo},
        {"stress_recv_multi", "stress", test_stress_recv_multi}, {"wakeup_burst", "stress", test_wakeup_burst},
        {"sq_full_tiny", "corner", test_sq_full_tiny}, {"accept_multi_burst", "stress", test_accept_multi_burst},
        {"send_zc_cancel", "corner", test_send_zc_cancel}, {"chaos_seeded", "stress", test_chaos_seeded},
        {"chaos_scheduler", "stress", test_chaos_scheduler}, {"high_concurrency_echo", "stress", test_high_concurrency_echo},
        {"small_messages", "benchmark", bench_small_messages}, {"large_transfer", "benchmark", bench_large_transfer},
        {"connect_rate", "benchmark", bench_connect_rate}, {"oneshot_vs_multi", "benchmark", bench_oneshot_vs_multi},
        {"send_vs_send_zc", "benchmark", bench_send_vs_send_zc}, {"connect_accept_multi", "benchmark", bench_connect_accept_multi},
        {"worker_scaling", "benchmark", bench_worker_scaling}, {"yield_checkpoint", "benchmark", bench_yield_checkpoint},
    };

    std::vector<std::string> groups;
    std::optional<std::string> exact_test;
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        if (arg == "--group" && index + 1 < argc) groups.emplace_back(argv[++index]);
        else if (arg == "--test" && index + 1 < argc) exact_test = argv[++index];
        else {
            std::fprintf(stderr, "usage: %s [--group NAME]... [--test NAME]\n", argv[0]);
            return 2;
        }
    }

    const bool skip_bench = std::getenv("SHARDIO_SKIP_BENCH") != nullptr;
    std::size_t executed = 0;
    for (const auto& test : cases) {
        if (skip_bench && std::strcmp(test.group, "benchmark") == 0) continue;
        if (exact_test && *exact_test != test.name) continue;
        if (!groups.empty() && std::find(groups.begin(), groups.end(), test.group) == groups.end()) continue;
        std::printf("[%s] %s\n", test.group, test.name);
        try {
            test.run();
        } catch (const std::exception& error) {
            std::fprintf(stderr, "FAIL %s: exception: %s\n", test.name, error.what());
            ++g_fails;
        }
        ++executed;
    }
    if (executed == 0) {
        std::fprintf(stderr, "no tests selected\n");
        return 2;
    }
    if (g_fails != 0) {
        std::fprintf(stderr, "\n%d check(s) failed\n", g_fails);
        return 1;
    }
    std::printf("\nall checks passed\n");
    return 0;
}
