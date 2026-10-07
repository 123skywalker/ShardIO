/* accept_multi_op.cpp, Multishot accept：同步交给 Sink */

#include "backend/io_operation.hpp"

#include <liburing.h>


AcceptMultiOp::AcceptMultiOp(int listen_fd, MultiShotSink<AcceptedFd>& sink)
    : listen_fd_(listen_fd)
    , sink_(&sink)
{
}

void AcceptMultiOp::prepare(io_uring_sqe& sqe)
{
    io_uring_prep_multishot_accept(
        &sqe, listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
}

void AcceptMultiOp::complete(int result, std::uint32_t flags) noexcept
{
    if (result >= 0) {
        // 未 move/release 则 on_shot 返回时 AcceptedFd 会 close，避免泄漏
        sink_->on_shot(AcceptedFd(result));
    }
    const bool more = (flags & IORING_CQE_F_MORE) != 0 && result >= 0;
    if (!more) {
        // 成功收尾传 0，避免把最后一个 fd 当成 errno
        sink_->on_end(result < 0 ? result : 0);
        finish(result < 0 ? result : 0);
    }
}
