/* cross_thread_inbox.hpp, 跨线程 post：bounded MPSC 快路径 + mutex deque 溢出 */

#pragma once

#include "core/context.hpp"
#include "runtime/mpsc_queue.hpp"

#include <atomic>
#include <deque>
#include <mutex>
#include <span>
#include <utility>

/**
 * @brief 跨线程投递：正常走无锁环，满了进 overflow，不再 busy-spin。
 * @details `overflowing_` 置位后后来的 producer 都进 overflow，避免 A 进 overflow、B 钻回
 *          已腾空的 fast 而乱序。Consumer 抽完 overflow 再清 flag，并再抽一轮吃掉竞态。
 */
class CrossThreadInbox {
public:
    /**
     * @brief 构造 Fast Path 环。
     * @param fast_capacity MPSC 槽位数，向上取 2 的幂。
     */
    explicit CrossThreadInbox(std::size_t fast_capacity = 256);

    /**
     * @brief 跨线程入队；永不在此执行 `task`。
     * @param task 稍后在 owner 线程运行。
     */
    void push(TaskFn task);

    /**
     * @brief 先抽 fast 再抽 overflow，再重复一轮以关闭「清 flag 时又 push」的窗口。
     * @tparam Fn `void(TaskFn)`。
     * @param out 收到任务后入 Scheduler Ready。
     */
    template <typename Fn>
    void drain(Fn&& out);

    /**
     * @brief 快路径为空且未处于 overflow 模式。
     * @details overflow 里若有竞态残留，producer 已 `wakeup`，下一圈 `drain` 能拿到。
     */
    bool empty() const noexcept;

private:
    /**
     * @brief 把 overflow 全部交给 `out`，然后 `overflowing_=false`。
     * @tparam Fn `void(TaskFn)`。
     * @param out 与 `drain` 相同。
     */
    template <typename Fn>
    void steal_overflow(Fn&& out);

    MPSCQueue<TaskFn> fast_;              /**< 无锁快路径。 */
    std::mutex overflow_mu_;              /**< 只保护 overflow。 */
    std::deque<TaskFn> overflow_;         /**< 环满后的冷路径。 */
    std::atomic<bool> overflowing_{false}; /**< 一旦溢出，后续都进 overflow。 */
};

template <typename Fn>
void CrossThreadInbox::steal_overflow(Fn&& out)
{
    std::lock_guard<std::mutex> lock(overflow_mu_);
    while (!overflow_.empty()) {
        out(std::move(overflow_.front()));
        overflow_.pop_front();
    }
    overflowing_.store(false, std::memory_order_release);  // 允许 producer 再走 try_push
}

template <typename Fn>
void CrossThreadInbox::drain(Fn&& out)
{
    TaskFn batch[64];
    auto drain_fast = [&] {
        for (;;) {
            const std::size_t n = fast_.drain(std::span<TaskFn>(batch, 64));
            if (n == 0) {
                break;
            }
            for (std::size_t i = 0; i < n; ++i) {
                out(std::move(batch[i]));
            }
        }
    };
    // 第二轮吃掉「清 overflowing_ 时 producer 又 push」的窗口
    drain_fast();
    steal_overflow(out);
    drain_fast();
    steal_overflow(out);
}
