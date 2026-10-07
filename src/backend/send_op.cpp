/* send_op.cpp, SendOp 实现 */

#include "backend/io_operation.hpp"

#include <liburing.h>

SendOp::SendOp(int fd, MutableBuffer buffer, std::uint32_t buf_index, int flags)
    : fd_(fd)
    , buffer_(buffer)
    , offset_(0)
    , buf_index_(buf_index)
    , flags_(flags)
{
}

void SendOp::prepare(io_uring_sqe& sqe)
{
    void* data = buffer_.data + offset_;
    const std::size_t len = buffer_.size - offset_;
    if (buf_index_ == kUnregisteredBuffer) {
        io_uring_prep_send(&sqe, fd_, data, len, flags_);
        return;
    }
    // 指针仍须落在该 index 对应的 iovec 内（短写只前移 data，不换块）
    io_uring_prep_write_fixed(
        &sqe, fd_, data, len, 0, static_cast<int>(buf_index_));
}

void SendOp::complete(int result, std::uint32_t) noexcept
{
    if (result > 0) {
        offset_ += static_cast<std::size_t>(result);
    }
    finish(result);
}
