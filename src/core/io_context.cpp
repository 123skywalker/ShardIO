/* io_context.cpp, I/O 域：提交/收割/Cancel，调度全部交给 ContextScheduler */

#include "core/io_context.hpp"
#include "backend/uring_backend.hpp"

#include <algorithm>
#include <cerrno>
#include <utility>

IoContext::IoContext(std::unique_ptr<IoBackend> backend, CpuExecutor* cpu,
                     IoBufferConfig buffers)
    : backend_(std::move(backend))
    , buffer_pool_(BufferPool::Config{buffers.buffer_size, buffers.provided_buffers})
    , cpu_(cpu)
{
    // 单测 / 单独 Runtime 没有池级 CpuExecutor 时自建一条 Worker
    if (cpu_ == nullptr) {
        owned_cpu_ = std::make_unique<CpuExecutor>(1);
        cpu_ = owned_cpu_.get();
    }
    if (buffers.provided_buffers != 0) {
        buffer_ring_ = std::make_unique<BufferRing>(
            buffer_pool_, BufferRing::Config{0, buffers.provided_buffers, 16});
        // 当前正式后端为 io_uring；注册只发生在构造冷路径，不进入 I/O 热路径。
        auto& uring = static_cast<UringBackend&>(*backend_);
        uring.register_buffers(buffer_pool_);
        uring.register_buffer_ring(*buffer_ring_);
        buffer_ring_->provide_all();
    }
}

IoContext::~IoContext()
{
    // 先停自建 CPU，避免 Worker 还往已析构的 inbox post
    if (owned_cpu_) {
        owned_cpu_->stop();
        owned_cpu_->join();
    }
    if (buffer_ring_) {
        auto& uring = static_cast<UringBackend&>(*backend_);
        uring.unregister_buffer_ring(*buffer_ring_);
        uring.unregister_buffers();
    }
}

void IoContext::run()
{
    owner_thread_.store(std::this_thread::get_id(), std::memory_order_release);
    for (;;) {
        drain_inbox();
        drain_pending_cancels();
        drain_pending_ops();
        backend_->flush();
        process_completions();
        // ENOBUFS 重挂要在跑 ready 前公布，否则下圈才看到空槽
        backend_->flush_reclaims();
        wakeup_pending_.store(false, std::memory_order_release);
        drain_inbox();
        // I/O inline resume 已在 complete 里跑完；这里只跑 post/spawn，再跑 yield
        scheduler_.run_ready(ContextScheduler::kReadyBudget);
        scheduler_.run_deferred(ContextScheduler::kDeferredBudget);
        scheduler_.reap();
        if (!stop_requested_.load(std::memory_order_acquire)) {
            continue;
        }
        // 非阻塞收尾：ready/yield/inbox 空且 CPU 作业已 post 回来才拆 spawned_
        if (!scheduler_.has_runnable() && inbox_.empty() && cpu_->inflight() == 0) {
            break;
        }
    }
}

void IoContext::stop()
{
    stop_requested_.store(true, std::memory_order_release);
    wakeup();
}

void IoContext::post(TaskFn task)
{
    if (in_this_context()) {
        // 同线程也不立刻跑，保证 post 永不在调用栈上执行
        scheduler_.post(std::move(task));
        return;
    }
    inbox_.push(std::move(task));
    wakeup();
}

void IoContext::dispatch(TaskFn task)
{
    if (in_this_context()) {
        task();
        return;
    }
    post(std::move(task));
}

Context IoContext::context() noexcept
{
    return Context{this};
}

BufferRing& IoContext::buffer_ring() noexcept
{
    return *buffer_ring_;
}

bool IoContext::in_this_context() const noexcept
{
    const std::thread::id owner = owner_thread_.load(std::memory_order_acquire);
    return owner != std::thread::id{} && owner == std::this_thread::get_id();
}

void IoContext::submit(IoOperation& op)
{
    if (!try_submit(op)) {
        pending_ops_.push_back(&op);
    }
}

void IoContext::spawn(Task<void> task)
{
    if (!task.valid()) {
        return;
    }
    scheduler_.spawn(std::move(task));
}

void IoContext::drain_pending_ops()
{
    if (pending_ops_.empty()) {
        return;
    }
    std::vector<IoOperation*> still;
    still.reserve(pending_ops_.size());
    for (IoOperation* op : pending_ops_) {
        if (op->is_completed()) {
            continue;
        }
        if (!try_submit(*op)) {
            still.push_back(op);
        }
    }
    pending_ops_ = std::move(still);
}

void IoContext::cancel(IoOperation& op)
{
    if (!in_this_context()) {
        post([this, p = &op] { cancel(*p); });
        return;
    }
    if (op.is_completed()) {
        return;
    }
    if (!op.is_submitted()) {
        // 还没进内核，本地 complete 即可，finish 会 inline resume waiter
        op.complete(-ECANCELED, 0);
        return;
    }
    auto req = std::make_unique<CancelOp>(op);
    if (try_submit(*req)) {
        inflight_cancels_.push_back(std::move(req));
        return;
    }
    pending_cancels_.push_back(std::move(req));
}

void IoContext::drain_inbox()
{
    inbox_.drain([this](TaskFn task) { scheduler_.post(std::move(task)); });
}

void IoContext::process_completions()
{
    // 有现成工作或正在 stop 时不阻塞 wait_cqe
    const bool busy = scheduler_.has_runnable() || !inbox_.empty() ||
                      stop_requested_.load(std::memory_order_relaxed);
    const std::size_t n = busy ? backend_->poll(completion_buf_)
                               : backend_->wait(completion_buf_);
    for (std::size_t i = 0; i < n; ++i) {
        IoOperation* op = completion_buf_[i].operation;
        if (op == nullptr) {
            continue;
        }
        // complete 可能 inline resume 并析构业务 Op；CancelOp 仍在 inflight_cancels_
        const bool is_cancel = op->is_cancel_request();
        op->complete(completion_buf_[i].result, completion_buf_[i].flags);
        if (!is_cancel || inflight_cancels_.empty()) {
            continue;
        }
        auto it = std::find_if(
            inflight_cancels_.begin(),
            inflight_cancels_.end(),
            [p = op](const std::unique_ptr<CancelOp>& c) { return c.get() == p; });
        if (it != inflight_cancels_.end()) {
            inflight_cancels_.erase(it);
        }
    }
}

YieldAwaiter IoContext::yield() noexcept
{
    return YieldAwaiter{this};
}

void IoContext::defer(std::coroutine_handle<> h)
{
    scheduler_.defer(h);
}

CpuExecutor& IoContext::cpu_executor() noexcept
{
    return *cpu_;
}

std::uint64_t IoContext::yield_suspends() const noexcept
{
    return scheduler_.yield_suspends();
}

std::uint64_t IoContext::yield_resumes() const noexcept
{
    return scheduler_.yield_resumes();
}

std::size_t IoContext::pending_handles_size() const noexcept
{
    return scheduler_.pending_handles_size();
}

std::size_t IoContext::pending_handles_max() const noexcept
{
    return scheduler_.pending_handles_max();
}

void IoContext::wakeup()
{
    // 一圈只打一次门铃，避免 post 风暴刷 eventfd
    if (!wakeup_pending_.exchange(true, std::memory_order_acq_rel)) {
        backend_->wakeup();
    }
}

bool IoContext::try_submit(IoOperation& op)
{
    if (backend_->submit(op)) {
        op.mark_submitted();
        return true;
    }
    backend_->flush();
    if (backend_->submit(op)) {
        op.mark_submitted();
        return true;
    }
    return false;
}

void IoContext::drain_pending_cancels()
{
    if (pending_cancels_.empty()) {
        return;
    }
    std::vector<std::unique_ptr<CancelOp>> still;
    still.reserve(pending_cancels_.size());
    for (auto& req : pending_cancels_) {
        if (try_submit(*req)) {
            inflight_cancels_.push_back(std::move(req));
        } else {
            still.push_back(std::move(req));
        }
    }
    pending_cancels_ = std::move(still);
}
