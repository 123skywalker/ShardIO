/* cross_thread_inbox.cpp, try_push 失败则 overflow，不 spin */

#include "runtime/cross_thread_inbox.hpp"

CrossThreadInbox::CrossThreadInbox(std::size_t fast_capacity) : fast_(fast_capacity) {}

void CrossThreadInbox::push(TaskFn task)
{
    // 已溢出则全部进 deque，避免 A 在 overflow、B 又钻回已腾空的 fast
    if (overflowing_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(overflow_mu_);
        overflow_.push_back(std::move(task));
        overflowing_.store(true, std::memory_order_release);
        return;
    }
    if (fast_.try_push(std::move(task))) {
        return;
    }
    overflowing_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(overflow_mu_);
    overflow_.push_back(std::move(task));
}

bool CrossThreadInbox::empty() const noexcept
{
    // 竞态残留靠 producer wakeup，下一圈 drain 再拿
    return fast_.empty() && !overflowing_.load(std::memory_order_acquire);
}
