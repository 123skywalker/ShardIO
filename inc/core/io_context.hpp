/* io_context.hpp, I/O 执行域：ring 提交/收割 + 驱动 ContextScheduler */

#pragma once

#include "backend/io_backend.hpp"
#include "backend/io_operation.hpp"
#include "core/context_scheduler.hpp"
#include "core/cpu_executor.hpp"
#include "core/context.hpp"
#include "core/task.hpp"
#include "core/yield_awaiter.hpp"
#include "memory/buffer_pool.hpp"
#include "memory/buffer_ring.hpp"
#include "runtime/cross_thread_inbox.hpp"

#include <array>
#include <atomic>
#include <coroutine>
#include <cstddef>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

template <class F>
class CpuAwaiter;

/**
 * @brief `IoContext` 内部缓冲区资源配置。
 */
struct IoBufferConfig {
    std::size_t provided_buffers{1024}; /**< multishot 接收可循环使用的块数。 */
    std::size_t buffer_size{4096}; /**< 每个 provided buffer 的字节数。 */
};

/**
 * @brief 单个 I/O 线程：SQ/CQ、Cancel、Wakeup、BufferRing flush，并驱动本地 Scheduler。
 * @details 1:1 绑定 `IoBackend`。连接不迁移。长 CPU 走 `async_cpu`，不在本线程算。
 */
class IoContext {
public:
    /**
     * @brief 接管 Backend；可选共用外部 `CpuExecutor`。
     * @param backend 非空，由本对象独占。
     * @param cpu 池共享的 CPU 执行器；空则本对象自建一条 Worker。
     */
    explicit IoContext(std::unique_ptr<IoBackend> backend,
                       CpuExecutor* cpu = nullptr,
                       IoBufferConfig buffers = {});

    /**
     * @brief 停自建的 `CpuExecutor`（若有）。调用前应 `stop()` 并退出 `run()`。
     */
    ~IoContext();

    IoContext(const IoContext&) = delete;
    IoContext& operator=(const IoContext&) = delete;
    IoContext(IoContext&&) = delete;
    IoContext& operator=(IoContext&&) = delete;

    /**
     * @brief 在当前线程跑 EventLoop 直至 `stop()` 且队列排空。
     */
    void run();

    /**
     * @brief 请求退出并 wakeup `wait()`。
     */
    void stop();

    /**
     * @brief 投递短回调：同线程进 Scheduler Ready，跨线程进 inbox（满则 overflow）。
     * @param task owner 线程执行；永不在 `post` 调用栈上立刻跑。
     */
    void post(TaskFn task);

    /**
     * @brief 已在 owner 则立刻执行，否则 `post`。
     * @param task 极短控制逻辑。
     */
    void dispatch(TaskFn task);

    /**
     * @brief 绑定本 Context 的轻量句柄。
     */
    Context context() noexcept;

    /**
     * @brief 当前线程是否为 owner。
     */
    bool in_this_context() const noexcept;

    /**
     * @brief 接手一颗 Op：入 SQ 或 `pending_ops_`。
     * @param op 完成前须存活。须在 owner 线程调用。
     */
    void submit(IoOperation& op);

    /**
     * @brief 根协程交给 `ContextScheduler` 延迟 start。
     * @param task 顶层任务。
     */
    void spawn(Task<void> task);

    /**
     * @brief 取消可能仍 in-flight 的 Op。
     * @param op 目标业务 Op。跨线程只 `post`。
     */
    void cancel(IoOperation& op);

    /**
     * @brief 无条件让出当前协程。
     */
    YieldAwaiter yield() noexcept;

    /**
     * @brief 把 `fn` 放到 `CpuExecutor`，完成后 resume 本协程。
     * @tparam F `fn()` 可调用。
     * @param fn CPU-heavy 或耗时未知的计算。
     * @return 可 `co_await`；结果类型为 `fn()` 的返回值。
     */
    template <class F>
    CpuAwaiter<std::decay_t<F>> async_cpu(F&& fn);

    /**
     * @brief 本 Context 使用的 CPU 执行器。
     */
    CpuExecutor& cpu_executor() noexcept;

    /**
     * @brief 取得本执行域自动注册的 provided-buffer ring。
     * @return 与当前 `IoContext` 同寿命的缓冲区环。
     */
    BufferRing& buffer_ring() noexcept;

    /**
     * @brief `yield` 入队次数。
     */
    std::uint64_t yield_suspends() const noexcept;

    /**
     * @brief yielded handle 被 resume 的次数。
     */
    std::uint64_t yield_resumes() const noexcept;

    /**
     * @brief 尚未 resume 的 yielded handle 数。
     */
    std::size_t pending_handles_size() const noexcept;

    /**
     * @brief yielded 队列高水位。
     */
    std::size_t pending_handles_max() const noexcept;

private:
    friend class YieldAwaiter;

    /**
     * @brief `yield` 把 handle 交给 Scheduler。
     * @param h 当前协程。
     */
    void defer(std::coroutine_handle<> h);

    /**
     * @brief MPSC + overflow 抽到 Scheduler Ready。
     */
    void drain_inbox();

    /**
     * @brief 有 runnable 则 poll CQ，否则 wait；对每笔 CQE `complete`（inline resume）。
     */
    void process_completions();

    /**
     * @brief 合并 wakeup。
     */
    void wakeup();

    /**
     * @brief 尝试入 SQ，失败则 flush 再试一次。
     * @param op 业务或 CancelOp。
     * @return 拿到 SQE 为 true。
     */
    bool try_submit(IoOperation& op);

    /**
     * @brief SQ 满留下的业务 Op 再提交。
     */
    void drain_pending_ops();

    /**
     * @brief SQ 满留下的 CancelOp 再提交。
     */
    void drain_pending_cancels();

    std::unique_ptr<IoBackend> backend_;  /**< 1:1 ring。 */
    BufferPool buffer_pool_;              /**< 本 Context 独占的固定地址缓冲池。 */
    std::unique_ptr<BufferRing> buffer_ring_; /**< 自动注册的 provided-buffer ring。 */
    std::unique_ptr<CpuExecutor> owned_cpu_; /**< 未注入时自建。 */
    CpuExecutor* cpu_{nullptr};           /**< 实际使用的 CPU 池。 */

    ContextScheduler scheduler_;          /**< spawn / post / yield。 */
    CrossThreadInbox inbox_;              /**< 跨线程 post：MPSC + overflow deque。 */

    std::vector<IoOperation*> pending_ops_;  /**< SQ 满尚未入 SQ 的业务 Op。 */
    std::vector<std::unique_ptr<CancelOp>> pending_cancels_;  /**< SQ 满的 CancelOp。 */
    std::vector<std::unique_ptr<CancelOp>> inflight_cancels_;  /**< 已 submit 等自身 CQE。 */
    std::array<Completion, 64> completion_buf_{};  /**< 批量收 CQ。 */

    std::atomic<bool> stop_requested_{false};  /**< 请求退出。 */
    std::atomic<std::thread::id> owner_thread_{}; /**< run() 发布的 owner 线程编号。 */
    std::atomic<bool> wakeup_pending_{false};  /**< 合并 eventfd wakeup。 */
};

#include "core/cpu_awaiter.hpp"

template <class F>
CpuAwaiter<std::decay_t<F>> IoContext::async_cpu(F&& fn)
{
    return CpuAwaiter<std::decay_t<F>>{context(), std::forward<F>(fn)};
}
