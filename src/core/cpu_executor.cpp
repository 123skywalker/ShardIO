/* cpu_executor.cpp, mutex+queue+cv：满载不 spin，stop 后先把已入队作业跑完 */

#include "core/cpu_executor.hpp"

#include <utility>

CpuExecutor::CpuExecutor(std::size_t workers)
{
    if (workers == 0) {
        workers = 1;
    }
    threads_.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
        threads_.emplace_back([this] { worker_main(); });
    }
}

CpuExecutor::~CpuExecutor()
{
    stop();
    join();
}

void CpuExecutor::submit(TaskFn fn)
{
    // 先记账再入队，IoContext::stop 用 inflight 判断 CPU 作业是否已 post 回来
    inflight_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(mu_);
        jobs_.push(std::move(fn));
    }
    cv_.notify_one();
}

void CpuExecutor::stop()
{
    stop_.store(true, std::memory_order_release);
    cv_.notify_all();
}

void CpuExecutor::join()
{
    for (std::thread& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    threads_.clear();
}

std::size_t CpuExecutor::inflight() const noexcept
{
    return inflight_.load(std::memory_order_relaxed);
}

void CpuExecutor::worker_main()
{
    for (;;) {
        TaskFn fn;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this] {
                return stop_.load(std::memory_order_acquire) || !jobs_.empty();
            });
            // stop 且队列空才退出；还有作业则先跑完再走
            if (jobs_.empty()) {
                return;
            }
            fn = std::move(jobs_.front());
            jobs_.pop();
        }
        fn();
        inflight_.fetch_sub(1, std::memory_order_relaxed);
    }
}
