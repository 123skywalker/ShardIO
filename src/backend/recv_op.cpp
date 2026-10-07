/* recv_op.cpp, RecvOp 实现 */

#include "backend/io_operation.hpp"

#include <liburing.h>

RecvOp::RecvOp(int fd, MutableBuffer buffer, std::uint32_t buf_index, int flags)
    : fd_(fd)
    , buffer_(buffer)
    , buf_index_(buf_index)
    , flags_(flags)
{
}

void RecvOp::prepare(io_uring_sqe& sqe)
{
    if (buf_index_ == kUnregisteredBuffer) {
        io_uring_prep_recv(&sqe, fd_, buffer_.data, buffer_.size, flags_);
        return;
    }
    // 文件 offset 对 socket 无意义；buf_index 对应 register 时的 iovec 下标
    io_uring_prep_read_fixed(
        &sqe, fd_, buffer_.data, buffer_.size, 0, static_cast<int>(buf_index_));
}

void RecvOp::complete(int result, std::uint32_t) noexcept
{
    finish(result);
}
