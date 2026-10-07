/* buffer_ring.hpp, provided-buffer 生命周期 */

#pragma once

#include "memory/buffer.hpp"
#include "memory/buffer_handle.hpp"
#include "memory/buffer_pool.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

struct io_uring_buf_ring;

/**
 * @brief 独占一批 Pool id，在 Provided / CheckedOut / pending reclaim 之间循环。
 * @details 不分配物理内存。Backend 只 setup/teardown 内核 buf ring；本类在绑定后直接
 *          `io_uring_buf_ring_add` / `advance`。bid 是本 Ring 下标，不是 Pool id。
 *          第一版约定 `Buffer` 仅在 owner I/O 线程析构。
 */
class BufferRing {
public:
    /**
     * @brief 组号、独占块数与 reclaim 压控。
     */
    struct Config {
        std::uint16_t group_id{0};     /**< SQE `buf_group` / 内核 buf ring 组号。 */
        std::size_t capacity{0};       /**< 独占块数，须为 2 的幂（同时作为内核 entries）。 */
        std::size_t reclaim_batch{16}; /**< pending 达到此数量则 `flush_reclaims`。 */
        std::size_t low_watermark{32}; /**< `kernel_available_` 低至此则立即 flush，避免 ENOBUFS。 */
    };

    /**
     * @brief 从 Pool `reserve(capacity)` 独占 id；此时尚未挂进内核。
     * @param pool 提供物理块的池，须比本对象活得长（或先 unbind）。
     * @param config `capacity` 为 0 以外时须能一次 reserve 成功，否则抛 `logic_error`。
     */
    BufferRing(BufferPool& pool, Config config);

    /**
     * @brief `unbind_kernel`：flush pending、归还独占 id。不负责 `io_uring_free_buf_ring`。
     */
    ~BufferRing();

    /**
     * @brief 禁止拷贝：独占 id 与内核 ring 绑定不可复制。
     */
    BufferRing(const BufferRing&) = delete;

    /**
     * @brief 禁止拷贝赋值。
     */
    BufferRing& operator=(const BufferRing&) = delete;

    /**
     * @brief 本 Ring 的 `buf_group`，供 RecvMulti SQE 填写。
     */
    std::uint16_t group_id() const noexcept;

    /**
     * @brief 独占块数 / 内核 buf ring entries。
     */
    std::size_t capacity() const noexcept;

    /**
     * @brief 当前内核 buf ring 上仍可见、可被 BUFFER_SELECT 选中的块数（计数）。
     */
    std::size_t kernel_available() const noexcept;

    /**
     * @brief 已 checkout 归还、尚未 `advance` 回内核的 bid 个数。
     */
    std::size_t pending_reclaims() const noexcept;

    /**
     * @brief 把 ring bid 映射为底层 BufferPool 的固定缓冲区下标。
     * @param bid ring 内核返回的缓冲区编号。
     * @return 对应 `io_uring_register_buffers` 的物理下标。
     */
    BufferId fixed_index(BufferBid bid) const noexcept;

    /**
     * @brief CQE 带 `F_BUFFER` 时接管该 bid：计数减一，交给 Sink。
     * @param bid 内核返回的 Buffer ID，即 `reserved_ids_` 下标。
     * @return 合法 bid 为拥有句柄；越界为空句柄（不 reclaim）。
     */
    Buffer checkout(BufferBid bid) noexcept;

    /**
     * @brief 把 pending 全部 `add` + 一次 `advance`，增加 `kernel_available_`。
     * @details 未 bind 或 pending 为空则为空操作。热路径无 syscall。
     */
    void flush_reclaims() noexcept;

    /**
     * @brief pending ≥ batch 或内核可见 ≤ 低水位时调用 `flush_reclaims`。
     */
    void flush_if_needed() noexcept;

    /**
     * @brief 把已 reserve、尚未挂货的块一次性 add+advance。
     * @details 须先 `bind_kernel`。只成功一次；之后靠 reclaim/flush 循环。未 bind 则忽略。
     */
    void provide_all() noexcept;

#if defined(SHARDIO_RING_STATS)
    /**
     * @brief `flush_reclaims` 实际执行次数（pending 空的调用不计）。诊断宏开启时才有。
     */
    std::uint64_t flush_count() const noexcept;

    /**
     * @brief 经 `flush_reclaims` 公布的块数之和（不含 `provide_all`）。
     */
    std::uint64_t buffers_advanced() const noexcept;
#endif

private:
    friend class UringBackend;
    friend class Buffer;

    /**
     * @brief Handle 析构：bid 入 pending，再按压控决定是否立刻 flush。
     * @param bid 先前 checkout 的编号。
     */
    void reclaim(BufferBid bid) noexcept;

    /**
     * @brief Backend `register_buffer_ring` 成功后绑定共享 buf ring 内存。
     * @param br liburing 分配的 ring，非拥有。
     * @param mask 一般为 `entries - 1`。
     */
    void bind_kernel(io_uring_buf_ring* br, int mask) noexcept;

    /**
     * @brief 先 flush pending，断开 `br_`，再 `Pool::release_reserved`。
     * @details 内核 unregister 由 Backend 负责。调用后本对象不再持有独占 id。
     */
    void unbind_kernel() noexcept;

    BufferPool* pool_{nullptr};  /**< 物理 Slot 来源；非拥有。 */
    Config config_{};            /**< 组号、容量与压控参数。 */
    std::vector<BufferId> reserved_ids_;  /**< `bid -> Pool id`；解绑后清空。 */
    std::vector<BufferBid> pending_reclaims_;  /**< 待 add 回内核的 bid；构造时 reserve 避免 realloc。 */
    io_uring_buf_ring* br_{nullptr};  /**< 已 bind 的内核 buf ring；未注册为 nullptr。 */
    int mask_{0};                    /**< `io_uring_buf_ring_add` 的 ring mask。 */
    std::size_t kernel_available_{0};  /**< 内核侧仍可选中的块数（生产路径用计数，不用状态机）。 */
    bool provided_{false};           /**< 已 `provide_all`；防止同一批 bid 重复 add。 */
#if defined(SHARDIO_RING_STATS)
    std::uint64_t flush_count_{0};      /**< 非空 flush_reclaims 次数。 */
    std::uint64_t buffers_advanced_{0}; /**< flush 公布的块数累计。 */
#endif
};
