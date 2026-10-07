/* timeout_op.cpp, TimeoutOp 实现 */

#include "backend/io_operation.hpp"

#include <cstdint>
#include <liburing.h>

TimeoutOp::TimeoutOp(std::uint64_t timeout_ns, unsigned flags)
    : timeout_ns_(timeout_ns)
    , timeout_flags_(flags)
{
}

void TimeoutOp::prepare(io_uring_sqe& sqe)
{
    ts_.tv_sec = static_cast<__kernel_time64_t>(timeout_ns_ / 1000000000ULL);
    ts_.tv_nsec = static_cast<long long>(timeout_ns_ % 1000000000ULL);
    io_uring_prep_timeout(&sqe, &ts_, 0, timeout_flags_);
}

void TimeoutOp::complete(int result, std::uint32_t) noexcept
{
    finish(result);
}
