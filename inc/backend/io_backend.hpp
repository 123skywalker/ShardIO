/* io_backend.hpp, ShardIO I/O 后端抽象接口与完成事件声明 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

class IoOperation;

/**
 * @brief 一次 I/O 完成事件，由 Backend 从内核 CQE 解码而来。
 * @details 上层（如 IoContext）只消费本结构，不直接接触 `io_uring_cqe`。
 */
struct Completion {
    /**
     * @brief 该完成事件对应的异步操作对象。
     * @details 通常来自 SQE user_data，即提交时绑定的 `IoOperation*`。
     */
    IoOperation* operation{nullptr};

    /**
     * @brief 内核返回的原始结果，对应 `cqe->res`。
     * @details 正数为字节数或新 fd，0 表示成功/EOF（依操作而定），负数为 `-errno`。
     */
    int result{0};

    /**
     * @brief 内核返回的原始标志，对应 `cqe->flags`。
     * @details 用于识别 `IORING_CQE_F_MORE` 等 Multishot 续命标志。
     */
    std::uint32_t flags{0};
};

/**
 * @brief ShardIO 与内核 I/O 接口之间的适配层抽象。
 * @details 第一版具体实现为 `UringBackend`；保留本接口以便后续接入 Mock/Transfer 等后端。
 *          `IoContext` 负责调度，本类只负责提交 SQE、回收 Completion，以及跨线程唤醒。
 */
class IoBackend {
public:
    /**
     * @brief 虚析构，保证通过基类指针释放具体后端时能正确回收 ring / wakeup fd 等资源。
     */
    virtual ~IoBackend() = default;

    /**
     * @brief 将一个 `IoOperation` 编译成 SQE（只入队准备，默认不立刻 syscall submit）。
     * @param op 待提交的异步操作，其地址会写入 SQE user_data 以便 CQE 回指。
     * @return 成功取得 SQE 并完成 prepare 时返回 true；环满或无法取 SQE 时返回 false。
     */
    virtual bool submit(IoOperation& op) = 0;

    /**
     * @brief 把当前已准备好的 SQE 一次性提交给内核（SQ Batching 的提交点）。
     * @return 本次实际提交的 SQE 数量。
     */
    virtual std::size_t flush() = 0;

    /**
     * @brief 阻塞直到内核交出至少一笔 CQE，再尽量批量写入 `output`。
     * @param output 调用方提供的完成事件缓冲区；写入个数不超过其 size。
     * @return 实际写入的业务 Completion 数量。门铃由具体后端消化时可以为 0。
     */
    virtual std::size_t wait(std::span<Completion> output) = 0;

    /**
     * @brief 非阻塞检查当前 CQ 是否已有完成事件。
     * @param output 调用方提供的完成事件缓冲区。
     * @return 实际写入数量；若当前无 CQE 则立即返回 0（与 `wait()` 相对）。
     */
    virtual std::size_t poll(std::span<Completion> output) = 0;

    /**
     * @brief 唤醒正在 `wait()` 中阻塞的 I/O 线程。任意线程可调用；不得向调用方抛异常。
     * @details 供其他线程在 `Context::post()`（`core/context.hpp`）之后使用，以便 IoContext 回来处理 inbox / stop。
     *          不向调用方暴露 eventfd 等实现细节。
     */
    virtual void wakeup() noexcept = 0;

    /**
     * @brief 把已注册 `BufferRing` 的 pending reclaim 公布给内核（`advance`）。
     * @details 默认空。不负责 reserve / 改 Buffer 状态。调用点：CQ 整批 `complete` 之后、`wait` 前。
     */
    virtual void flush_reclaims() noexcept {}
};
