/* uring_backend.hpp, ShardIO 基于 Linux io_uring 的 IoBackend */

#pragma once

#include "backend/io_backend.hpp"
#include "backend/io_operation.hpp"

#include <cstddef>
#include <cstdint>
#include <liburing.h>
#include <span>
#include <vector>

class BufferPool;
class BufferRing;

/**
 * @brief Backend 内部门铃标签，仅作 SQE user_data；不进入 IoContext Completion。
 * @details `prepare` 对 eventfd 挂 `poll_multishot`。`complete` 不应被 IoContext 调用。
 */
class WakeupOp final : public IoOperation {
public:
    WakeupOp() = default;

    /**
     * @brief eventfd 创建成功后绑定真实 fd（构造时尚无有效 fd）。
     * @param wakeup_fd Backend 持有的 eventfd，本对象不负责关闭。
     */
    void bind(int wakeup_fd) noexcept;

    /**
     * @brief 使用 `io_uring_prep_poll_multishot` 把 wakeup fd 挂进 SQE。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 门铃由 Backend `handle_wakeup_cqe` 消化；本函数留空。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int wakeup_fd_{-1};  /**< 绑定后的 eventfd；无效为 -1。 */
};

/**
 * @brief ShardIO 与 `io_uring` 之间的适配层：SQE 编码、CQE 解码、内部门铃。
 * @details 与一块 ring、一个 I/O 线程 1:1。不负责协程调度、业务 Buffer 或 TCP。
 */
class UringBackend final : public IoBackend {
public:
    /**
     * @brief 初始化 ring、创建 eventfd，并提交第一笔 eventfd multishot poll。
     * @param queue_depth SQ 容量（如 256/512/1024），不是最大连接数。
     */
    explicit UringBackend(unsigned queue_depth = 256);

    /**
     * @brief 关闭 eventfd 并 `io_uring_queue_exit`。
     */
    ~UringBackend() override;

    UringBackend(const UringBackend&) = delete;
    UringBackend& operator=(const UringBackend&) = delete;
    UringBackend(UringBackend&&) = delete;
    UringBackend& operator=(UringBackend&&) = delete;

    /**
     * @brief 为业务 Op 取 SQE 并 `prepare`，不立刻 `io_uring_submit`。
     * @param op 待编码操作，地址写入 user_data，完成前须保持存活。
     * @return 取得 SQE 为 true；环满为 false。
     */
    bool submit(IoOperation& op) override;

    /**
     * @brief 把当前 pending SQE 提交给内核，并更新 pending 计数。
     * @return 实际提交数量；`io_uring_submit` 失败应抛错，不能假装提交了 0 个。
     */
    std::size_t flush() override;

    /**
     * @brief 阻塞等到至少一笔 CQE，再批量 drain。进入 `wait_cqe` 前必须先确保门铃已 armed。
     * @param out 业务 Completion 输出；门铃由 Backend 内部消费，不写入本缓冲。
     * @return 业务 Completion 个数；仅被门铃叫醒时可以为 0。
     */
    std::size_t wait(std::span<Completion> out) override;

    /**
     * @brief 非阻塞 peek CQ。不 ensure / 不重挂门铃。
     * @param out 业务 Completion 输出。
     * @return 写入个数；无 CQE 立即返回 0。
     */
    std::size_t poll(std::span<Completion> out) override;

    /**
     * @brief 任意线程可调用：只 `eventfd_write`，不碰 ring，不向调用线程 throw。
     */
    void wakeup() noexcept override;


    /**
     * @brief 把整池 `storage_` 按块注册进 ring（iovec[i] 对应 id=i）。须在无 in-flight fixed I/O 时调用。
     * @param pool 地址已固定的 BufferPool；成功后 `pool.is_registered()` 为真。
     */
    void register_buffers(BufferPool& pool);

    /**
     * @brief 撤销 `register_buffers`。无注册时为空操作。析构前也会调用。
     */
    void unregister_buffers();

    /**
     * @brief 为 `BufferRing::group_id()` 创建并绑定内核 buf ring。`ring.capacity()` 须为 2 的幂。
     * @details 只 setup/bind，不 `provide_all`。挂货由 `BufferRing` 负责。
     */
    void register_buffer_ring(BufferRing& ring);

    /**
     * @brief 内核 unregister 后 `unbind_kernel`（归还独占 id）。未注册则为空操作。
     */
    void unregister_buffer_ring(BufferRing& ring);

private:
    /**
     * @brief 对每个已注册 `BufferRing` 调用 `flush_reclaims`。
     */
    void flush_reclaims() noexcept override;

    /**
     * @brief 创建 eventfd、bind `WakeupOp`，再 `arm_wakeup`。
     */
    void setup_wakeup();

    /**
     * @brief 先 `flush_all_pending`，再单独 prepare+submit 门铃；确认提交成功后才 `wakeup_armed_ = true`。
     */
    void arm_wakeup();

    /**
     * @brief 非阻塞读空 eventfd counter（成功继续、EINTR 重试、EAGAIN 结束）。
     */
    void drain_wakeup_fd();

    /**
     * @brief 检查 poll `res`、drain fd、无 MORE 或 `res < 0` 则 `wakeup_armed_ = false`。不在此重挂。
     */
    void handle_wakeup_cqe(int result, std::uint32_t flags);

    /**
     * @brief 门铃则内部消费并返回 false；业务则填 `out` 并返回 true。须 `cqe_seen`。
     */
    bool decode_cqe(io_uring_cqe* cqe, Completion& out);

    /**
     * @brief 直到 pending 清零或失败；供 `arm_wakeup` 在单独提交门铃前使用。
     */
    void flush_all_pending();

    io_uring ring_{};  /**< 本 Context 的 io_uring，仅 owner 线程操作。 */

    unsigned queue_depth_{0};  /**< 构造时的 SQ 容量，供 init / 诊断。 */

    std::size_t pending_submissions_{0};  /**< 已 prepare、尚未确认提交给内核的 SQE 数。 */

    int wakeup_fd_{-1};  /**< 跨线程门铃 eventfd；无效为 -1。 */

    WakeupOp wakeup_op_{};  /**< 稳定 user_data，寿命与 Backend 相同。 */

    bool wakeup_armed_{false};  /**< true 当且仅当 Kernel 中已有有效的 wakeup multishot poll。 */

    BufferPool* registered_pool_{nullptr};  /**< 当前已 register 的池，unregister 时回写 index。 */

    /**
     * @brief 已注册的 provided buffer ring。
     */
    struct BufRingReg {
        std::uint16_t group_id{0};  /**< 与 `BufferRing::group_id` 相同。 */
        io_uring_buf_ring* br{nullptr};  /**< liburing 分配的 buf ring。 */
        unsigned entries{0};  /**< ring 容量，2 的幂。 */
        BufferRing* user_ring{nullptr};  /**< 解绑时清空其 `br_`，避免悬空。 */
    };

    std::vector<BufRingReg> buf_rings_;  /**< 按 group 注册的 provided rings。 */

    BufRingReg* find_buf_ring(std::uint16_t group_id) noexcept;
};
