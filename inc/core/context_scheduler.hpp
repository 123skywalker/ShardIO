/* context_scheduler.hpp, 单 IoContext 本地协作式调度：spawn / ready / yield */

#pragma once

#include "core/context.hpp"
#include "core/task.hpp"

#include <cstddef>
#include <cstdint>
#include <coroutine>
#include <vector>

/**
 * @brief 绑定一颗 `IoContext` 的无锁协作调度器。
 * @details 只跑在 owner 线程。不管 SQ/CQ、Cancel、BufferRing，也不做跨 Context steal。
 *          Ready 与 yielded handle 均为 FIFO + head 下标：本轮只消费进入时的 snapshot。
 */
class ContextScheduler {
public:
    static constexpr std::size_t kReadyBudget = 64;     /**< 每圈最多执行的 `post`/`spawn` 回调。 */
    static constexpr std::size_t kDeferredBudget = 64;  /**< 每圈最多 resume 的 yield handle。 */

    /**
     * @brief 托管根协程：所有权留下，首次 `resume` 进 Ready。
     * @param task 有效的 lazy `Task<void>`。
     */
    void spawn(Task<void> task);

    /**
     * @brief 把回调追加到 Ready 尾部；不立即执行。
     * @param fn owner 线程稍后跑的短任务。
     */
    void post(TaskFn fn);

    /**
     * @brief `yield()` 挂起：handle 进 deferred 队列。
     * @param h 当前协程；frame 仍由 `spawned_` 持有。
     */
    void defer(std::coroutine_handle<> h);

    /**
     * @brief 执行进入本函数时 Ready 快照中至多 `budget` 个回调。
     * @param budget 本圈上限，通常 `kReadyBudget`。
     */
    void run_ready(std::size_t budget);

    /**
     * @brief 执行进入本函数时 deferred 快照中至多 `budget` 个 handle。
     * @param budget 本圈上限，通常 `kDeferredBudget`。
     */
    void run_deferred(std::size_t budget);

    /**
     * @brief Ready 或 deferred 是否还有未消费项。
     */
    bool has_runnable() const noexcept;

    /**
     * @brief 丢掉已 `done()` 的根 Task。
     */
    void reap();

    /**
     * @brief `defer` 次数。
     */
    std::uint64_t yield_suspends() const noexcept;

    /**
     * @brief deferred `resume` 次数。
     */
    std::uint64_t yield_resumes() const noexcept;

    /**
     * @brief 尚未 resume 的 yielded handle 数。
     */
    std::size_t pending_handles_size() const noexcept;

    /**
     * @brief `pending_handles_size` 历史高水位。
     */
    std::size_t pending_handles_max() const noexcept;

private:
    /**
     * @brief 丢掉 `[0, head)` 已消费槽，避免向量无限涨。
     * @param q 队列。
     * @param head 已消费下标。
     */
    template <typename T>
    static void compact(std::vector<T>& q, std::size_t& head);

    std::vector<Task<void>> spawned_;                 /**< 根协程所有权。 */
    std::vector<TaskFn> ready_;                       /**< spawn 启动与 post 回调。 */
    std::size_t ready_head_{0};                       /**< 下一笔 Ready。 */
    std::vector<std::coroutine_handle<>> deferred_;   /**< `yield()` 的 handle。 */
    std::size_t deferred_head_{0};                    /**< 下一笔 deferred。 */
    std::uint64_t yield_suspends_{0};                 /**< defer 计数。 */
    std::uint64_t yield_resumes_{0};                  /**< resume 计数。 */
    std::size_t pending_handles_max_{0};              /**< deferred 高水位。 */
};
