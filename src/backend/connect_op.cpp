/* connect_op.cpp, ConnectOp 实现 */

#include "backend/io_operation.hpp"

#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>

ConnectOp::ConnectOp(int fd, const sockaddr_storage& address)
    : fd_(fd)
    , address_(address)
{
}

void ConnectOp::prepare(io_uring_sqe& sqe)
{
    socklen_t len = sizeof(address_);
    if (address_.ss_family == AF_INET) {
        len = sizeof(sockaddr_in);
    } else if (address_.ss_family == AF_INET6) {
        len = sizeof(sockaddr_in6);
    }
    io_uring_prep_connect(
        &sqe, fd_,
        reinterpret_cast<sockaddr*>(&address_),
        len);
}

void ConnectOp::complete(int result, std::uint32_t) noexcept
{
    finish(result);
}
