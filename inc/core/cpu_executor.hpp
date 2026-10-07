/* cpu_executor.hpp, CPU 执行域：Worker 跑纯计算，完成后由调用方 post 回 IoContext */

#pragma once

#include "core/context.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

/**
 * @brief 与 I/O 线程分离的 CPU Worker 池。
 * @details 不碰 ring / Connection。`submit` 的回调在 Worker 上跑完后，应由回调自己
 *          `origin.post(resume)` 回到提交时的 `IoContext`。
 */
class CpuExecutor {
public:
    /**
     * @brief 创建 `workers` 条线程，构造后即可 `submit`。
     * @param workers Worker 数，至少按 1 用。
     */
    explicit CpuExecutor(std::size_t workers);

    /**
     * @brief `stop` + `join`。
     */
    ~CpuExecutor();

    CpuExecutor(const CpuExecutor&) = delete;
    CpuExecutor& operator=(const CpuExecutor&) = delete;

    /**
     * @brief 入队一个 CPU 作业。
     * @param fn Worker 上执行的函数；通常内部再 `post` 回 IoContext。
     */
    void submit(TaskFn fn);

    /**
     * @brief 拒绝新作业，唤醒 Worker 把队列掏空后退出。
     */
    void stop();

    /**
     * @brief 等待全部 Worker 线程结束。
     */
    void join();

    /**
     * @brief 已 submit 尚未跑完的作业数。
     */
    std::size_t inflight() const noexcept;

private:
    /**
     * @brief Worker 循环：取作业、执行、`inflight_--`。
     */
    void worker_main();

    std::mutex mu_;                       /**< 保护 `jobs_`。 */
    std::condition_variable cv_;          /**< 有作业或 stop。 */
    std::queue<TaskFn> jobs_;             /**< CPU 作业 FIFO。 */
    std::vector<std::thread> threads_;    /**< Worker。 */
    std::atomic<bool> stop_{false};       /**< 停止接新活并让空闲 Worker 退出。 */
    std::atomic<std::size_t> inflight_{0}; /**< 已入队未执行完。 */
};
