/* io_operation.cpp, IoOperation 完成语义 */

#include "backend/io_operation.hpp"

void IoOperation::set_waiter(std::coroutine_handle<> waiter) noexcept
{
    waiter_ = waiter;
}

void IoOperation::mark_submitted() noexcept
{
    submitted_ = true;
}

bool IoOperation::is_submitted() const noexcept
{
    return submitted_;
}

bool IoOperation::is_completed() const noexcept
{
    return completed_;
}

bool IoOperation::is_cancel_request() const noexcept
{
    return cancel_request_;
}

int IoOperation::result() const noexcept
{
    return result_;
}

void IoOperation::finish(int result) noexcept
{
    if (completed_) {
        return;
    }
    completed_ = true;
    result_ = result;
    notify();
}

void IoOperation::notify() noexcept
{
    // 不清 completed_：同一颗 Op 还要收后续 CQE
    if (waiter_) {
        auto h = waiter_;
        waiter_ = {};
        h.resume();
    }
}
