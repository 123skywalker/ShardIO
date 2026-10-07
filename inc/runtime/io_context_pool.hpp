/* io_context_pool.hpp, 多 IoContext 分片：创建/绑核/启停/选 Context，不进 I/O 热路径 */

#pragma once

#include "core/context.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

class CpuExecutor;
class IoContext;

/**
 * @brief `IoContextPool` 的线程、ring 与缓冲区配置。
 */
struct PoolOptions {
    std::size_t io_workers{1}; /**< I/O Context 与 I/O 工作线程数量。 */
    std::size_t cpu_workers{0}; /**< CPU 工作线程数量；0 表示与 I/O Worker 数相同。 */
    std::size_t queue_depth{256}; /**< 每个 io_uring 的提交队列深度。 */
    bool pin_threads{true}; /**< 是否按 Worker 编号绑定逻辑 CPU。 */
    std::size_t provided_buffers{1024}; /**< 每个 Context 的 provided buffer 数量。 */
    std::size_t buffer_size{4096}; /**< 每个 provided buffer 的字节数。 */
};

/**
 * @brief 多个固定 CPU 的 `IoContext` 的组织和分片层。
 * @details 1 Core = 1 Thread = 1 IoContext = 1 UringBackend。
 *          热路径（recv/send/CQE/resume）不经过本对象；BufferDomain 由每个 Context 自动管理。
 *          多核 listen 使用 `ListenOptions::reuse_port` 与 `context(i)`。
 */
class IoContextPool {
public:
    /**
     * @brief 创建全部 `UringBackend` + `IoContext`，不启动线程。
     * @param options I/O/CPU Worker、ring 与缓冲区配置。
     */
    explicit IoContextPool(PoolOptions options = {});

    /**
     * @brief `stop()` + `join()`，避免 joinable `std::thread` 析构。
     */
    ~IoContextPool();

    /** @brief 禁止复制拥有线程与 ring 的运行时。 */
    IoContextPool(const IoContextPool&) = delete;
    /** @brief 禁止复制赋值拥有线程与 ring 的运行时。 */
    IoContextPool& operator=(const IoContextPool&) = delete;

    /**
     * @brief 启动全部 Worker 线程（只调一次）。
     */
    void start();

    /**
     * @brief 请求每个 `IoContext::stop()`（含 wakeup）。
     */
    void stop() noexcept;

    /**
     * @brief 等待全部 Worker 退出。
     */
    void join();

    /**
     * @brief Round Robin 选一个 Context（单 Listener 交接用）。
     * @details REUSEPORT 每核 Listener 应使用 `context(i)`，不要走本函数。
     */
    Context next_context() noexcept;

    /**
     * @brief 指定 Worker 的 Context。
     * @param index 须 `< size()`。
     */
    Context context(std::size_t index) noexcept;

    /**
     * @brief Worker 数量。
     */
    std::size_t size() const noexcept;

private:
    struct Worker {
        std::size_t id{0};                     /**< Pool 内编号。 */
        std::size_t cpu_id{0};                 /**< 目标逻辑 CPU。 */
        std::unique_ptr<IoContext> context;    /**< 本核独占。 */
        std::thread thread;                    /**< `context->run()`。 */
    };

    /**
     * @brief Worker 入口：可选绑核后 `run()`。
     * @param worker 本线程对应的槽。
     */
    void run_worker(Worker& worker);

    /**
     * @brief `pthread_setaffinity_np` 到 `cpu_id`。
     * @param cpu_id 逻辑 CPU。
     */
    static void pin_current_thread(std::size_t cpu_id);

    PoolOptions options_{};                /**< 构造配置副本。 */
    std::unique_ptr<CpuExecutor> cpu_;     /**< 全池共用 CPU 域。 */
    std::vector<Worker> workers_;          /**< 固定分片，start 后不再扩容。 */
    std::atomic<std::size_t> next_{0};     /**< Round Robin 计数，relaxed。 */
};
