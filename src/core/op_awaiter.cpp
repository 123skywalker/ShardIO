/* op_awaiter.cpp, 把 oneshot IoOperation 接到协程：submit 后挂起，CQE 再 resume */

#include "core/op_awaiter.hpp"

OpAwaiter::OpAwaiter(Context executor, IoOperation& op) noexcept
    : executor_(executor)
    , op_(&op)
{
}

bool OpAwaiter::await_ready() const noexcept
{
    return false;
}

void OpAwaiter::await_suspend(std::coroutine_handle<> h) noexcept
{
    // 先挂 waiter：提交路径若同步 complete，也能 resume
    op_->set_waiter(h);
    // 入 SQ 或 pending_ops_；返回 void = 始终挂起，不要按 submit 成败 resume
    executor_.submit(*op_);
}

int OpAwaiter::await_resume() noexcept
{
    // 负 errno 原样返回，由 TcpStream 填 IoResult
    return op_->result();
}
