/* recv_multi_op.cpp, RecvMultiOp：multishot recv，同步交给 Sink */

#include "backend/io_operation.hpp"
#include "memory/buffer_ring.hpp"

#include <liburing.h>
#include <utility>

RecvMultiOp::RecvMultiOp(int fd, BufferRing& ring,
                         MultiShotSink<Buffer>& sink)
    : fd_(fd)
    , ring_(&ring)
    , sink_(&sink)
{
}

void RecvMultiOp::prepare(io_uring_sqe& sqe)
{
    io_uring_prep_recv_multishot(&sqe, fd_, nullptr, 0, 0);
    sqe.flags |= IOSQE_BUFFER_SELECT;
    sqe.buf_group = ring_->group_id();
}

void RecvMultiOp::complete(int result, std::uint32_t flags) noexcept
{
    if ((flags & IORING_CQE_F_BUFFER) != 0) {
        const auto bid = static_cast<BufferBid>(flags >> IORING_CQE_BUFFER_SHIFT);
        Buffer buffer = ring_->checkout(bid);
        if (result > 0) {
            buffer.resize(static_cast<std::size_t>(result));
            sink_->on_shot(std::move(buffer));
        }
        // result<=0：Handle 析构 reclaim，不交给 Sink
    }
    // MORE 且读到数据：还在听。EOF(res==0)、错误、或 MORE=0：结束
    const bool more = (flags & IORING_CQE_F_MORE) != 0 && result > 0;
    if (!more) {
        sink_->on_end(result < 0 ? result : 0);
        finish(result < 0 ? result : 0);
    }
}
