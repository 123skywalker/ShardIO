/* context_scheduler.cpp, FIFO + head：本圈只跑进入时的 snapshot */

#include "core/context_scheduler.hpp"

#include <cstddef>
#include <utility>

template <typename T>
void ContextScheduler::compact(std::vector<T>& q, std::size_t& head)
{
    if (head == 0) {
        return;
    }
    if (head == q.size()) {
        q.clear();
        head = 0;
        return;
    }
    q.erase(q.begin(), q.begin() + static_cast<std::ptrdiff_t>(head));
    head = 0;
}

void ContextScheduler::spawn(Task<void> task)
{
    spawned_.push_back(std::move(task));
    // 只拷贝 handle：spawned_ 扩容会搬 Task，不能抓 Task&
    const std::coroutine_handle<> h = spawned_.back().handle();
    ready_.push_back([h] { h.resume(); });
}

void ContextScheduler::post(TaskFn fn)
{
    ready_.push_back(std::move(fn));
}

void ContextScheduler::defer(std::coroutine_handle<> h)
{
    ++yield_suspends_;
    deferred_.push_back(h);
    // 水位按尚未 resume 的条数，不含已跑过的 head 空洞
    const std::size_t n = deferred_.size() - deferred_head_;
    if (n > pending_handles_max_) {
        pending_handles_max_ = n;
    }
}

void ContextScheduler::run_ready(std::size_t budget)
{
    // snap 钉死本圈尾巴；执行中新 post/spawn 追加在 snap 之后，下一圈再跑
    const std::size_t snap = ready_.size();
    std::size_t n = 0;
    while (n < budget && ready_head_ < snap) {
        TaskFn fn = std::move(ready_[ready_head_]);
        ++ready_head_;
        ++n;
        fn();
    }
    compact(ready_, ready_head_);
}

void ContextScheduler::run_deferred(std::size_t budget)
{
    const std::size_t snap = deferred_.size();
    std::size_t n = 0;
    while (n < budget && deferred_head_ < snap) {
        const std::coroutine_handle<> h = deferred_[deferred_head_];
        ++deferred_head_;
        ++n;
        ++yield_resumes_;
        h.resume();
    }
    compact(deferred_, deferred_head_);
}

bool ContextScheduler::has_runnable() const noexcept
{
    return ready_head_ < ready_.size() || deferred_head_ < deferred_.size();
}

void ContextScheduler::reap()
{
    auto it = spawned_.begin();
    while (it != spawned_.end()) {
        if (it->done()) {
            it = spawned_.erase(it);
        } else {
            ++it;
        }
    }
}

std::uint64_t ContextScheduler::yield_suspends() const noexcept
{
    return yield_suspends_;
}

std::uint64_t ContextScheduler::yield_resumes() const noexcept
{
    return yield_resumes_;
}

std::size_t ContextScheduler::pending_handles_size() const noexcept
{
    return deferred_.size() - deferred_head_;
}

std::size_t ContextScheduler::pending_handles_max() const noexcept
{
    return pending_handles_max_;
}
