/* yield_awaiter.cpp, 无条件 yield：只 defer，不在 suspend 里 resume 别人 */

#include "core/yield_awaiter.hpp"
#include "core/io_context.hpp"

YieldAwaiter::YieldAwaiter(IoContext* ctx) noexcept : ctx_(ctx) {}

void YieldAwaiter::await_suspend(std::coroutine_handle<> h)
{
    // 只入队；这里 resume 别人会回不到 EventLoop 收 CQ
    ctx_->defer(h);
}
