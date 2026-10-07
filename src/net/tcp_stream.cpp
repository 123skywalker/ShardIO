/* tcp_stream.cpp, fd 所有权 + Connect/Recv/Send + OpAwaiter */

#include "net/tcp_stream.hpp"

#include "backend/io_operation.hpp"
#include "core/op_awaiter.hpp"
#include "memory/buffer_ring.hpp"
#include "net/error.hpp"
#include "net/multi_stream.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <netinet/tcp.h>
#include <unistd.h>
#include <utility>

TcpStream::TcpStream(int fd, Context context) noexcept
    : fd_(fd)
    , context_(context)
{
}

TcpStream::~TcpStream()
{
    close();
}

TcpStream::TcpStream(TcpStream&& other) noexcept
    : fd_(other.fd_)
    , context_(other.context_)
    , send_pipeline_(std::move(other.send_pipeline_))
{
    // in-flight RecvMulti 跟 fd 一起走，源变成空流
    other.fd_ = -1;
}

TcpStream& TcpStream::operator=(TcpStream&& other) noexcept
{
    if (this != &other) {
        close();
        fd_ = other.fd_;
        context_ = other.context_;
        send_pipeline_ = std::move(other.send_pipeline_);
        other.fd_ = -1;
    }
    return *this;
}

Task<IoResult<TcpStream>> TcpStream::connect(
    Context context, Endpoint peer, ConnectOptions options)
{
    if (peer.size() == 0 || peer.family() == AF_UNSPEC) {
        co_return IoResult<TcpStream>{{}, net_errno(EINVAL)};
    }

    const int fd = ::socket(
        peer.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        co_return IoResult<TcpStream>{{}, net_errno(errno)};
    }

    sockaddr_storage ss{};
    // ConnectOp 自备一份地址：peer 实参不一定活过 CQE
    std::memcpy(&ss, peer.native(), static_cast<std::size_t>(peer.size()));

    ConnectOp op(fd, ss);
    const int res = co_await OpAwaiter(context, op);
    if (res < 0) {
        ::close(fd);
        co_return IoResult<TcpStream>{{}, net_uring(res)};
    }
    if (options.no_delay) {
        const int yes = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    }
    co_return IoResult<TcpStream>{TcpStream{fd, context}, {}};
}

Task<IoResult<std::size_t>> TcpStream::recv(MutableBuffer buffer,
                                            RecvOptions options)
{
    if (fd_ < 0) {
        co_return IoResult<std::size_t>{0, net_errno(EBADF)};
    }
    const BufferId index = options.fixed_buffer
        ? buffer.fixed_index : kUnregisteredBuffer;
    RecvOp op(fd_, buffer, index, options.flags);
    const int res = co_await OpAwaiter(context_, op);
    if (res < 0) {
        co_return IoResult<std::size_t>{0, net_uring(res)};
    }
    // res==0 且无 error：对端 FIN / EOF
    co_return IoResult<std::size_t>{static_cast<std::size_t>(res), {}};
}

RecvStream TcpStream::recv_multi(RecvMultiOptions options)
{
    return RecvStream::start(context_, fd_, context_.buffer_ring(),
                             options.queue_capacity);
}

Task<IoResult<std::size_t>> TcpStream::send(ConstBuffer buffer,
                                            SendOptions options)
{
    if (fd_ < 0) {
        co_return IoResult<std::size_t>{0, net_errno(EBADF)};
    }
    MutableBuffer view{
        const_cast<std::byte*>(buffer.data),
        buffer.size,
    };
    const int flags = options.flags | MSG_NOSIGNAL;
    if (options.zero_copy) {
        // zero-copy 必须等 NOTIF 后恢复，调用方缓冲区才可安全复用。
        SendZcOp op(fd_, view, buffer.fixed_index, flags);
        const int res = co_await OpAwaiter(context_, op);
        if (res < 0) {
            co_return IoResult<std::size_t>{0, net_uring(res)};
        }
        co_return IoResult<std::size_t>{static_cast<std::size_t>(res), {}};
    }
    SendOp op(fd_, view, buffer.fixed_index, flags);
    const int res = co_await OpAwaiter(context_, op);
    if (res < 0) {
        co_return IoResult<std::size_t>{0, net_uring(res)};
    }
    co_return IoResult<std::size_t>{static_cast<std::size_t>(res), {}};
}

Task<IoResult<void>> TcpStream::send_all(ConstBuffer buffer, SendOptions options)
{
    if (fd_ < 0) {
        co_return IoResult<void>{net_errno(EBADF)};
    }
    std::size_t sent = 0;
    while (sent < buffer.size) {
        // 允许短写：每轮从剩余区间再发
        ConstBuffer slice{buffer.data + sent, buffer.size - sent};
        IoResult<std::size_t> one = co_await send(slice, options);
        if (!one) {
            co_return IoResult<void>{one.error};
        }
        if (one.value == 0) {
            co_return IoResult<void>{net_errno(EPIPE)};
        }
        sent += one.value;
    }
    co_return IoResult<void>{{}};
}

std::error_code TcpStream::shutdown(Shutdown how) noexcept
{
    if (fd_ < 0) {
        return net_errno(EBADF);
    }
    const int native_how = how == Shutdown::Read ? SHUT_RD
                         : how == Shutdown::Write ? SHUT_WR : SHUT_RDWR;
    if (::shutdown(fd_, native_how) != 0) {
        return net_errno(errno);
    }
    return {};
}

void TcpStream::close() noexcept
{
    if (fd_ < 0) {
        return;
    }
    if (send_pipeline_) {
        // Pipeline 先停止并取消，fd 延迟到 CQE/ZC NOTIF 全部退休后关闭。
        SendPipeline::close_connection(send_pipeline_);
        send_pipeline_.reset();
    } else {
        ::close(fd_);
    }
    fd_ = -1;
}

bool TcpStream::valid() const noexcept
{
    return fd_ >= 0;
}

int TcpStream::native_handle() const noexcept
{
    return fd_;
}

Context TcpStream::context() const noexcept
{
    return context_;
}
