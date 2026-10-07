/* accept_op.cpp, AcceptOp 实现 */

#include "backend/io_operation.hpp"

#include <liburing.h>

AcceptOp::AcceptOp(int listen_fd)
    : listen_fd_(listen_fd)
{
}

void AcceptOp::prepare(io_uring_sqe& sqe)
{
    io_uring_prep_accept(&sqe, listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
}

void AcceptOp::complete(int result, std::uint32_t) noexcept
{
    finish(result);
}
