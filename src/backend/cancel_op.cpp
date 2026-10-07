/* cancel_op.cpp, CancelOp 实现 */

#include "backend/io_operation.hpp"

#include <liburing.h>

CancelOp::CancelOp(IoOperation& target, int flags)
    : target_(&target)
    , cancel_flags_(flags)
{
    cancel_request_ = true;
}

void CancelOp::prepare(io_uring_sqe& sqe)
{
    io_uring_prep_cancel(&sqe, target_, cancel_flags_);
}

void CancelOp::complete(int result, std::uint32_t) noexcept
{
    finish(result);
}
