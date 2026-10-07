/* context.hpp, 绑定单个 IoContext 的轻量执行域句柄 */

#pragma once

#include <functional>
#include <type_traits>
#include <utility>

class IoContext;
class IoOperation;
class BufferRing;
class OpAwaiter;
class TcpStream;
class TcpListener;
class RecvStream;
class AcceptStream;
class Batch;
template <class F>
class CpuAwaiter;
class SendPipeline;
namespace shardio_detail {
class SendPipelineState;
}
template <typename T>
class Task;
class YieldAwaiter;

/**
 * @brief 可投递到 `IoContext` 的无参任务。
 * @details 跨线程经 inbox 传递，在 owner 线程执行。不要长时间占用 I/O 线程。
 */
using TaskFn = std::function<void()>;

/**
 * @brief 描述任务应在哪个 `IoContext` 上执行的轻量句柄。
 * @details 本类型只保存一个指针，可复制且不拥有线程或 io_uring；普通用户不接触底层操作。
 */
class Context {
public:
    /**
     * @brief 构造空执行域句柄，仅用于延迟赋值。
     */
    Context() noexcept = default;

    /**
     * @brief 向绑定的 `IoContext` 投递任务（任意线程安全，内部走 `IoContext::post`）。
     * @param task 将在 owner 线程执行的回调。
     */
    void post(TaskFn task) const;

    /**
     * @brief 已在 owner 线程则立即执行，否则 `post`。
     * @param task 待执行的回调。
     */
    void dispatch(TaskFn task) const;

    /**
     * @brief 把一个根协程交给所属执行域运行。
     * @param task 待启动的顶层协程任务。
     */
    void spawn(Task<void> task) const;

    /**
     * @brief 创建一次协作式让出 awaiter，使当前协程回到本地就绪队列尾部。
     * @return 可直接 `co_await` 的让出对象。
     */
    YieldAwaiter yield() const noexcept;

    /**
     * @brief 把耗时计算移交 CPU 工作线程，并在当前执行域恢复协程。
     * @tparam F 无参可调用对象类型。
     * @param fn 将在 CPU 工作线程执行的函数。
     * @return 可直接 `co_await` 的 CPU 计算对象。
     */
    template <class F>
    CpuAwaiter<std::decay_t<F>> async_cpu(F&& fn) const;

    /**
     * @brief 创建一个显式批量提交对象。
     * @return 绑定当前执行域的批次；默认 recv/send 路径不受影响。
     */
    Batch batch() const;

    /**
     * @brief 当前线程是否为绑定 `IoContext` 的 owner 线程。
     */
    bool in_this_context() const noexcept;

private:
    friend class IoContext;
    friend class OpAwaiter;
    friend class TcpStream;
    friend class TcpListener;
    friend class RecvStream;
    friend class AcceptStream;
    template <class F>
    friend class CpuAwaiter;
    friend class SendPipeline;
    friend class shardio_detail::SendPipelineState;

    /**
     * @brief 由 `IoContext` 创建指向自身的轻量句柄。
     * @param context 所属执行域，必须比返回的 Context 活得更久。
     */
    explicit Context(IoContext* context) noexcept;

    /**
     * @brief 把内部 I/O 操作交给所属执行域。
     * @param op 必须存活至完成事件返回的内部操作。
     */
    void submit(IoOperation& op) const;

    /**
     * @brief 请求取消一个仍在执行的内部 I/O 操作。
     * @param op 需要取消的内部操作。
     */
    void cancel(IoOperation& op) const;

    /**
     * @brief 把包装后的 CPU 作业送入所属 CPU 执行器。
     * @param task 在 CPU 工作线程执行的作业。
     */
    void submit_cpu(TaskFn task) const;

    /**
     * @brief 取得所属执行域自动管理的 provided-buffer ring。
     * @return 与执行域同寿命的内部缓冲区环。
     */
    BufferRing& buffer_ring() const noexcept;

    IoContext* context_{nullptr};  /**< 目标 I/O 执行域，不持有所有权。 */
};

#include "core/cpu_awaiter.hpp"
#include "core/yield_awaiter.hpp"

template <class F>
CpuAwaiter<std::decay_t<F>> Context::async_cpu(F&& fn) const
{
    return CpuAwaiter<std::decay_t<F>>{*this, std::forward<F>(fn)};
}

#include "net/batch.hpp"
