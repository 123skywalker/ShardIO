/* yield_awaiter.hpp, 无条件协作式让出：挂起当前协程，把控制权交还 EventLoop */

#pragma once

#include <coroutine>

class IoContext;

/**
 * @brief `co_await IoContext::yield()` 的 awaiter。
 * @details 不替代 `OpAwaiter`，也不改 `finish()` 的 inline resume。
 *          `await_ready()` 恒为 false：每次检查点都入 `pending_handles_`。
 *          `await_suspend` 只入队，不 resume 别人。frame 仍由 `spawned_` 持有。
 */
class YieldAwaiter {
public:
    /**
     * @brief 绑定所属 Context。
     * @param ctx 正在 `run()` 的 `IoContext`。
     */
    explicit YieldAwaiter(IoContext* ctx) noexcept;

    /**
     * @brief 始终挂起。
     * @return 恒为 false。
     */
    bool await_ready() const noexcept
    {
        return false;
    }

    /**
     * @brief 把当前协程推进 `pending_handles_`。
     * @param h 当前帧；禁止在这里 `h.resume()`。
     */
    void await_suspend(std::coroutine_handle<> h);

    /**
     * @brief 恢复后无返回值。
     */
    void await_resume() const noexcept {}

private:
    IoContext* ctx_{nullptr}; /**< 入队与计数用。 */
};
