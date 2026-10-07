/* op_awaiter.hpp, 极薄：把 oneshot IoOperation 接到 C++20 协程，不做成 Task */

#pragma once

#include "backend/io_operation.hpp"
#include "core/context.hpp"

#include <coroutine>

/**
 * @brief 通用 oneshot Op 的 awaiter：submit 后挂起，CQE `complete` 再 resume。
 * @details RecvOp/SendOp/ConnectOp/AcceptOp 不必变成 `Task`。`TcpStream::recv` 等在协程帧里
 *          构造 Op，再 `co_await OpAwaiter`。入 SQ 或 `pending_ops_` 后必须挂起，不得因
 *          SQ 满而立刻 `await_resume`。须在所属 I/O 线程 await（与 `Context::submit` 相同）。
 */
class OpAwaiter {
public:
    /**
     * @brief 绑定一次提交。
     * @param executor 目标 IoContext 的句柄；`submit` 转调它。
     * @param op 已构造、尚未完成的 Op；须活到 `complete`（通常在调用方协程帧上）。
     */
    OpAwaiter(Context executor, IoOperation& op) noexcept;

    /**
     * @brief 总是需要等 CQE，不在 suspend 前完成。
     * @return 恒为 false。
     */
    bool await_ready() const noexcept;

    /**
     * @brief 登记 waiter，再 `Context::submit`（SQ 或 pending）。始终挂起。
     * @param h 当前协程；由 `IoOperation::finish` → `notify` resume。
     */
    void await_suspend(std::coroutine_handle<> h) noexcept;

    /**
     * @brief 返回 `op.result()`（字节数 / 新 fd / 0 / `-errno`）。
     * @details 不把负 errno 转成异常；由 `TcpStream` 填进 `IoResult::error`。
     */
    int await_resume() noexcept;

private:
    Context executor_{};         /**< 提交目标，不持有 IoContext。 */
    IoOperation* op_{nullptr};    /**< 非拥有；所有权在调用方协程帧。 */
};
