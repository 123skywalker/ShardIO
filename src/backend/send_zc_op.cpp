/* send_zc_op.cpp, 上层选择的 zerocopy send；等 NOTIF 才 finish / 还 Handle */

#include "backend/io_operation.hpp"

#include <liburing.h>
#include <utility>

SendZcOp::SendZcOp(int fd, BufferHandle buffer, std::uint32_t buf_index, int flags)
    : fd_(fd)
    , owned_(std::move(buffer))
    , buffer_(owned_.view())
    , buf_index_(buf_index)
    , flags_(flags)
{
}

SendZcOp::SendZcOp(int fd, MutableBuffer buffer, std::uint32_t buf_index, int flags)
    : fd_(fd)
    , buffer_(buffer)
    , buf_index_(buf_index)
    , flags_(flags)
{
}

void SendZcOp::prepare(io_uring_sqe& sqe)
{
    const unsigned zc_flags = 0;
    if (buf_index_ == kUnregisteredBuffer) {
        io_uring_prep_send_zc(
            &sqe, fd_, buffer_.data, buffer_.size, flags_, zc_flags);
        return;
    }
    // 与 register_buffers 的 iovec 下标对应；缓冲须活到 NOTIF
    io_uring_prep_send_zc_fixed(
        &sqe, fd_, buffer_.data, buffer_.size, flags_, zc_flags,
        static_cast<unsigned>(buf_index_));
}

void SendZcOp::complete(int result, std::uint32_t flags) noexcept
{
    if ((flags & IORING_CQE_F_NOTIF) != 0) {
        // 内核不再引用缓冲，此时才能还 Handle
        owned_ = BufferHandle{};
        finish(have_send_result_ ? send_result_ : result);
        return;
    }
    have_send_result_ = true;
    send_result_ = result;
    if ((flags & IORING_CQE_F_MORE) != 0 && result >= 0) {
        // 还有 NOTIF，先不 resume，避免上层提前回收
        return;
    }
    owned_ = BufferHandle{};
    finish(result);
}
