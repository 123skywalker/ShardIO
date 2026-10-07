/* cpu_awaiter.hpp, co_await async_cpu：Worker 执行 fn，post 回原 IoContext 再 resume */

#pragma once

#include "core/context.hpp"

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

class Context;

/**
 * @brief `IoContext::async_cpu(fn)` 的 awaiter。
 * @tparam F 可调用对象；`fn()` 的返回值即 `await_resume` 类型（void 则无值）。
 * @details 由 `io_context.hpp` 在 `IoContext` 定义之后包含，以便调用 `post` / `cpu_executor`。
 */
template <class F>
class CpuAwaiter {
public:
    using result_type = std::invoke_result_t<F&>;

    /**
     * @brief 绑定提交方 Context 与待执行函数。
     * @param ctx 完成后 `post(resume)` 的目标。
     * @param fn 在 `CpuExecutor` Worker 上调用。
     */
    CpuAwaiter(Context context, F fn) noexcept
        : context_(context)
        , fn_(std::move(fn))
    {
    }

    /**
     * @brief 始终要上 CPU 线程。
     * @return 恒为 false。
     */
    bool await_ready() const noexcept
    {
        return false;
    }

    /**
     * @brief 把作业交给 `CpuExecutor`；算完后 `post` resume。
     * @param h 当前协程。
     */
    void await_suspend(std::coroutine_handle<> h);

    /**
     * @brief 取出 `fn()` 的返回值；若 Worker 上抛错则在此重抛。
     */
    result_type await_resume();

private:
    Context context_{};       /**< 完成后续跑的轻量执行域句柄。 */
    F fn_;                    /**< CPU 作业。 */
    std::exception_ptr ep_{}; /**< Worker 捕获的异常。 */
    std::conditional_t<std::is_void_v<result_type>, char,
                       std::optional<result_type>>
        result_{}; /**< 非 void 时保存返回值。 */
};

template <class F>
void CpuAwaiter<F>::await_suspend(std::coroutine_handle<> h)
{
    // awaiter 活在协程帧上，Worker 跑完再 post，resume 时 this 仍有效
    context_.submit_cpu([this, h] {
        try {
            if constexpr (std::is_void_v<result_type>) {
                fn_();
            } else {
                result_.emplace(fn_());
            }
        } catch (...) {
            ep_ = std::current_exception();
        }
        context_.post([h] { h.resume(); });
    });
}

template <class F>
typename CpuAwaiter<F>::result_type CpuAwaiter<F>::await_resume()
{
    if (ep_) {
        std::rethrow_exception(ep_);
    }
    if constexpr (std::is_void_v<result_type>) {
        return;
    } else {
        return std::move(*result_);
    }
}
