/* tcp_listener.cpp, 同步 bind/listen + oneshot / multishot accept */

#include "net/tcp_listener.hpp"

#include "backend/io_operation.hpp"
#include "core/op_awaiter.hpp"
#include "net/error.hpp"
#include "net/multi_stream.hpp"

#include <cerrno>
#include <stdexcept>
#include <netinet/tcp.h>
#include <unistd.h>
#include <utility>

TcpListener::TcpListener(int fd, Context context) noexcept
    : fd_(fd)
    , context_(context)
{
}

TcpListener::~TcpListener()
{
    close();
}

TcpListener::TcpListener(TcpListener&& other) noexcept
    : fd_(other.fd_)
    , context_(other.context_)
{
    other.fd_ = -1;
}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept
{
    if (this != &other) {
        close();
        fd_ = other.fd_;
        context_ = other.context_;
        other.fd_ = -1;
    }
    return *this;
}

IoResult<TcpListener> TcpListener::bind(
    Context context, Endpoint endpoint, ListenOptions options)
{
    if (endpoint.size() == 0 || endpoint.family() == AF_UNSPEC) {
        return {{}, net_errno(EINVAL)};
    }

    const int fd = ::socket(
        endpoint.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return {{}, net_errno(errno)};
    }

    const int yes = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (options.reuse_port) {
        // 每 IoContext 一个 Listener 时让内核把 SYN 分到各 listen fd
        (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    }

    if (::bind(fd, endpoint.native(), endpoint.size()) != 0) {
        const int e = errno;
        ::close(fd);
        return {{}, net_errno(e)};
    }
    if (::listen(fd, options.backlog) != 0) {
        const int e = errno;
        ::close(fd);
        return {{}, net_errno(e)};
    }
    return {TcpListener{fd, context}, {}};
}

Task<IoResult<TcpStream>> TcpListener::accept(AcceptOptions options)
{
    if (fd_ < 0) {
        co_return IoResult<TcpStream>{{}, net_errno(EBADF)};
    }
    AcceptOp op(fd_);
    const int res = co_await OpAwaiter(context_, op);
    if (res < 0) {
        co_return IoResult<TcpStream>{{}, net_uring(res)};
    }
    if (options.no_delay) {
        const int yes = 1;
        (void)::setsockopt(res, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    }
    // 新连接钉在 Listener 的 Context，整个生命周期不迁移。
    co_return IoResult<TcpStream>{TcpStream{res, context_}, {}};
}

AcceptStream TcpListener::accept_multi(AcceptMultiOptions options)
{
    return AcceptStream::start(context_, fd_, options.queue_capacity,
                               options.no_delay);
}

Endpoint TcpListener::local_endpoint() const
{
    if (fd_ < 0) {
        return {};
    }
    sockaddr_storage ss{};
    socklen_t len = sizeof(ss);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&ss), &len) != 0) {
        return {};
    }
    return Endpoint(reinterpret_cast<const sockaddr*>(&ss), len);
}

void TcpListener::close() noexcept
{
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    // 与 TcpStream 相同：不自动 cancel 仍在飞的 AcceptMultiOp
}

bool TcpListener::valid() const noexcept
{
    return fd_ >= 0;
}

int TcpListener::native_handle() const noexcept
{
    return fd_;
}

Context TcpListener::context() const noexcept
{
    return context_;
}
