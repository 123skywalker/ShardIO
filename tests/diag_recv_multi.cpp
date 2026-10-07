/* diag_recv_multi.cpp, RecvMulti vs oneshot 公平对比（不进 e2e） */

#ifndef SHARDIO_RING_STATS
#error "diag_recv_multi 需要 -DSHARDIO_RING_STATS（见 scripts/run_diag_recv_multi.sh）"
#endif

#include "backend/io_operation.hpp"
#include "backend/uring_backend.hpp"
#include "core/io_context.hpp"
#include "core/op_awaiter.hpp"
#include "core/task.hpp"
#include "memory/buffer_pool.hpp"
#include "memory/buffer_ring.hpp"

#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <memory>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

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
        return cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return ready; });
    }
};

struct Runtime {
    UringBackend* backend{nullptr};
    std::unique_ptr<IoContext> ctx;
    std::thread io;

    Runtime()
    {
        auto b = std::make_unique<UringBackend>(1024);
        backend = b.get();
        ctx = std::make_unique<IoContext>(std::move(b));
        io = std::thread([this] { ctx->run(); });
    }

    ~Runtime()
    {
        ctx->stop();
        if (io.joinable()) {
            io.join();
        }
    }
};

void close_fd(int& fd)
{
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

void make_pair(int fds[2])
{
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        throw std::runtime_error("socketpair");
    }
    int snd = 256 * 1024;
    ::setsockopt(fds[0], SOL_SOCKET, SO_RCVBUF, &snd, sizeof(snd));
    ::setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
    const int fl = ::fcntl(fds[1], F_GETFL, 0);
    ::fcntl(fds[1], F_SETFL, fl | O_NONBLOCK);
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
            if (::poll(&pfd, 1, static_cast<int>(left)) <= 0) {
                return false;
            }
            continue;
        }
        return false;
    }
    return true;
}

bool wait_pred(int ms, const std::function<bool()>& pred)
{
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

double ru_ms(const timeval& t)
{
    return static_cast<double>(t.tv_sec) * 1000.0 +
           static_cast<double>(t.tv_usec) / 1000.0;
}

struct CpuSnap {
    double user_ms{0};
    double sys_ms{0};
};

CpuSnap cpu_now()
{
    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    return {ru_ms(ru.ru_utime), ru_ms(ru.ru_stime)};
}

void print_row(const char* name, unsigned cap, long long bytes, int shots,
               int sqe, int enobufs, std::uint64_t flush, std::uint64_t adv,
               double sec, CpuSnap a, CpuSnap b)
{
    const double mib = sec > 0
        ? (static_cast<double>(bytes) / (1024.0 * 1024.0)) / sec
        : 0;
    const double avg = shots > 0 ? static_cast<double>(bytes) / shots : 0;
    const double batch =
        flush > 0 ? static_cast<double>(adv) / static_cast<double>(flush) : 0;
    const double cpb = bytes > 0
        ? ((b.user_ms + b.sys_ms - a.user_ms - a.sys_ms) * 1e6) /
              static_cast<double>(bytes)
        : 0;
    std::printf(
        "%-22s cap=%4u bytes=%lld shots=%d avg=%.1f sqe=%d enobufs=%d "
        "flush=%llu adv=%llu batch=%.1f sec=%.4f MiB/s=%.2f user=%.1f sys=%.1f "
        "us/byte=%.4f\n",
        name, cap, bytes, shots, avg, sqe, enobufs,
        static_cast<unsigned long long>(flush),
        static_cast<unsigned long long>(adv), batch, sec, mib,
        b.user_ms - a.user_ms, b.sys_ms - a.sys_ms, cpb);
}

struct MultiSink final : MultiShotSink<Buffer> {
    Runtime* rt{nullptr};
    BufferRing* ring{nullptr};
    int fd{-1};
    std::atomic<int> shots{0};
    std::atomic<long long> bytes{0};
    std::atomic<int> enobufs{0};
    std::atomic<int> submits{0};
    Gate* eof{nullptr};
    std::unique_ptr<RecvMultiOp> op;

    void arm()
    {
        op = std::make_unique<RecvMultiOp>(fd, *ring, *this);
        rt->ctx->submit(*op);
        submits.fetch_add(1);
    }

    void on_shot(Buffer value) noexcept override
    {
        bytes.fetch_add(static_cast<long long>(value.size()));
        shots.fetch_add(1);
        (void)value;
    }

    void on_end(int result) noexcept override
    {
        if (result == -ENOBUFS) {
            enobufs.fetch_add(1);
            rt->ctx->post([this] {
                ring->flush_reclaims();
                if (ring->kernel_available() > 0) {
                    arm();
                }
            });
            return;
        }
        if (eof != nullptr) {
            eof->signal();
        }
    }
};

bool run_oneshot(unsigned cap, int writes)
{
    int fds[2]{-1, -1};
    make_pair(fds);
    Runtime rt;
    std::vector<std::byte> buf(cap);
    std::atomic<long long> got{0};
    std::atomic<int> shots{0};
    Gate done;
    rt.ctx->post([&] {
        rt.ctx->spawn([](int fd, std::vector<std::byte>& buf, int writes,
                         std::atomic<long long>& got, std::atomic<int>& shots,
                         Context ex, Gate& g) -> Task<void> {
            for (int i = 0; i < writes; ++i) {
                RecvOp op(fd, MutableBuffer{buf.data(), buf.size()});
                const int res = co_await OpAwaiter(ex, op);
                if (res <= 0) {
                    break;
                }
                got.fetch_add(res);
                shots.fetch_add(1);
            }
            g.signal();
            co_return;
        }(fds[0], buf, writes, got, shots, rt.ctx->context(), done));
    });
    std::vector<char> chunk(cap, 'o');
    const CpuSnap a = cpu_now();
    const auto t0 = std::chrono::steady_clock::now();
    bool ok = true;
    for (int i = 0; i < writes && ok; ++i) {
        ok = write_fully(fds[1], chunk.data(), chunk.size(), 60000);
    }
    ::shutdown(fds[1], SHUT_WR);
    if (!done.wait_ms(120000) || !ok) {
        std::printf("FAIL oneshot cap=%u\n", cap);
        close_fd(fds[0]);
        close_fd(fds[1]);
        return false;
    }
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const CpuSnap b = cpu_now();
    char name[64];
    std::snprintf(name, sizeof(name), "oneshot");
    print_row(name, cap, got.load(), shots.load(), shots.load(), 0, 0, 0, sec, a,
              b);
    close_fd(fds[0]);
    close_fd(fds[1]);
    return true;
}

bool run_multi(unsigned cap, int writes, unsigned ring_n, std::size_t batch,
               std::size_t watermark, const char* tag)
{
    int fds[2]{-1, -1};
    make_pair(fds);
    BufferPool pool({cap, ring_n});
    BufferRing ring(pool, BufferRing::Config{
        .group_id = 1,
        .capacity = ring_n,
        .reclaim_batch = batch,
        .low_watermark = watermark,
    });
    Runtime rt;
    Gate armed;
    rt.ctx->post([&] {
        rt.backend->register_buffer_ring(ring);
        ring.provide_all();
        armed.signal();
    });
    if (!armed.wait_ms(2000)) {
        return false;
    }
    MultiSink sink;
    sink.rt = &rt;
    sink.ring = &ring;
    sink.fd = fds[0];
    Gate eof;
    sink.eof = &eof;
    rt.ctx->post([&] { sink.arm(); });
    std::vector<char> chunk(cap, 'm');
    const long long want = static_cast<long long>(writes) * cap;
    const CpuSnap a = cpu_now();
    const auto t0 = std::chrono::steady_clock::now();
    bool ok = true;
    for (int i = 0; i < writes && ok; ++i) {
        ok = write_fully(fds[1], chunk.data(), chunk.size(), 60000);
    }
    ::shutdown(fds[1], SHUT_WR);
    if (!ok || !wait_pred(120000, [&] { return sink.bytes.load() >= want; })) {
        std::printf("FAIL multi %s\n", tag);
        close_fd(fds[0]);
        close_fd(fds[1]);
        return false;
    }
    const double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const CpuSnap b = cpu_now();
    if (!eof.wait_ms(200)) {
        if (sink.op) {
            rt.ctx->cancel(*sink.op);
        }
        (void)eof.wait_ms(2000);
    }
    print_row(tag, cap, sink.bytes.load(), sink.shots.load(), sink.submits.load(),
              sink.enobufs.load(), ring.flush_count(), ring.buffers_advanced(), sec,
              a, b);
    rt.ctx->post([&] { sink.op.reset(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    close_fd(fds[0]);
    close_fd(fds[1]);
    return true;
}

}  // namespace

int main()
{
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("# RecvMulti 公平 A/B（write 块 = recv 槽；64MiB）\n");
    std::printf("# us/byte = (user+sys) ms * 1e6 / bytes，含写线程\n");

    constexpr int kBytes = 64 << 20;
    const unsigned caps[] = {1024, 4096};
    bool ok = true;
    for (unsigned cap : caps) {
        const int writes = kBytes / static_cast<int>(cap);
        std::printf("\n-- cap=%u writes=%d --\n", cap, writes);
        ok = run_oneshot(cap, writes) && ok;
        ok = run_multi(cap, writes, 512, 32, 64, "multi ring=512 batch=32") && ok;
        ok = run_multi(cap, writes, 512, 1, 64, "multi ring=512 batch=1") && ok;
        ok = run_multi(cap, writes, 1024, 32, 128, "multi ring=1024 batch=32") &&
             ok;
    }
    return ok ? 0 : 1;
}
