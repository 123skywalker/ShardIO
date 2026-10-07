/* io_context_pool.cpp, 多 IoContext 的创建、绑核、启停、Round Robin */

#include "runtime/io_context_pool.hpp"

#include "backend/uring_backend.hpp"
#include "core/cpu_executor.hpp"
#include "core/io_context.hpp"

#include <pthread.h>
#include <sched.h>
#include <utility>

IoContextPool::IoContextPool(PoolOptions options)
    : options_(options)
    , cpu_(std::make_unique<CpuExecutor>(
          options_.cpu_workers == 0 ? options_.io_workers : options_.cpu_workers))
{
    workers_.reserve(options_.io_workers);
    for (std::size_t i = 0; i < options_.io_workers; ++i) {
        Worker w;
        w.id = i;
        w.cpu_id = i;
        // 各核独占 ring，CPU 域全池共用
        w.context = std::make_unique<IoContext>(
            std::make_unique<UringBackend>(static_cast<unsigned>(options_.queue_depth)),
            cpu_.get(),
            IoBufferConfig{options_.provided_buffers, options_.buffer_size});
        workers_.push_back(std::move(w));
    }
}

IoContextPool::~IoContextPool()
{
    stop();
    join();
    // I/O 线程已退出后再停 CPU，避免 Worker 往已拆的 Context post
    cpu_->stop();
    cpu_->join();
}

void IoContextPool::start()
{
    for (Worker& w : workers_) {
        // workers_ 此后不再扩容，&w 对线程寿命稳定
        w.thread = std::thread([this, &w] { run_worker(w); });
    }
}

void IoContextPool::stop() noexcept
{
    for (Worker& w : workers_) {
        // stop 里含 wakeup，把可能堵在 wait_cqe 的线程拉回来
        w.context->stop();
    }
}

void IoContextPool::join()
{
    for (Worker& w : workers_) {
        if (w.thread.joinable()) {
            w.thread.join();
        }
    }
}

Context IoContextPool::next_context() noexcept
{
    // 只给新连接分片；relaxed 即可，不是发布同步。REUSEPORT 路径不要走这里
    const std::size_t i =
        next_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    return workers_[i].context->context();
}

Context IoContextPool::context(std::size_t index) noexcept
{
    return workers_[index].context->context();
}

std::size_t IoContextPool::size() const noexcept
{
    return workers_.size();
}

void IoContextPool::run_worker(Worker& worker)
{
    if (options_.pin_threads) {
        pin_current_thread(worker.cpu_id);
    }
    worker.context->run();
}

void IoContextPool::pin_current_thread(std::size_t cpu_id)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(cpu_id), &set);
    // 绑核失败仍继续跑：测试机核数可能少于 first_cpu+worker_count
    (void)pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}
