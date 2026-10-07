/* buffer_pool.hpp, 物理内存 + 空闲 id */

#pragma once

#include "memory/buffer.hpp"
#include "memory/buffer_handle.hpp"
#include "memory/id_free_list.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

/**
 * @brief 一次申请并切成固定块；只维护 Free id。
 * @details 不知道 provided-buffer、内核可见性或 pending reclaim。
 *          oneshot 走 `try_acquire`；BufferRing 走 `reserve` 独占一批 id。
 *          热路径不 malloc。每 IoContext 通常一块 Pool。
 */
class BufferPool {
public:
    /**
     * @brief 构造参数：块大小与块数。不含是否 register_buffers。
     */
    struct Config {
        std::size_t buffer_size{0};   /**< 每块字节数；0 表示空池。 */
        std::size_t buffer_count{0};  /**< 块数，也是 BufferId 范围 `[0, count)`。 */
    };

    /**
     * @brief 分配 `count * size` 连续存储，切 Slot，并把全部 id 推入空闲表。
     * @param config 块大小与数量；任一项为 0 则空池（无 storage）。
     */
    explicit BufferPool(Config config);

    /**
     * @brief 释放整段 storage。调用前应已归还全部 Handle / Ring 独占 id。
     */
    ~BufferPool();

    /** @brief 禁止拷贝：storage 地址须稳定且唯一。 */
    BufferPool(const BufferPool&) = delete;
    /** @brief 禁止拷贝赋值。 */
    BufferPool& operator=(const BufferPool&) = delete;

    /**
     * @brief 从空闲表借一块给 oneshot Recv/Send/SendZc。
     * @return 成功为拥有句柄；无空闲为 `nullopt`，不隐式 malloc。
     */
    std::optional<BufferHandle> try_acquire();

    /**
     * @brief 从空闲表拿走一批 id，交给某个 BufferRing 独占。
     * @param count 需要的块数。中途不足则把已取出的全部放回并返回空 vector。
     * @return 长度为 `count` 的 id 列表，或失败时空列表。
     */
    std::vector<BufferId> reserve(std::size_t count);

    /**
     * @brief Ring 解绑后把独占 id 逐个 `release` 回空闲表。
     * @param ids `reserve` 得到的编号，可为空。
     */
    void release_reserved(std::span<const BufferId> ids) noexcept;

    /**
     * @brief 按物理编号取 Slot（读写 data 指针与 capacity）。
     * @param id 须 `< capacity()`；越界为未定义。
     */
    BufferSlot& slot(BufferId id) noexcept;

    /**
     * @brief 只读 Slot。
     * @param id 须 `< capacity()`。
     */
    const BufferSlot& slot(BufferId id) const noexcept;

    /**
     * @brief 构造时的块数（含已被 Ring reserve 的）。
     */
    std::size_t capacity() const noexcept;

    /**
     * @brief 当前仍在空闲表、可 `try_acquire` 的块数（近似，relaxed）。
     */
    std::size_t available() const noexcept;

    /**
     * @brief 单块字节数。
     */
    std::size_t buffer_size() const noexcept;

    /**
     * @brief 标记整池已 `io_uring_register_buffers`；此后 `Handle::id()` 可作 fixed index。
     */
    void mark_registered() noexcept;

    /**
     * @brief 清除固定注册标记（unregister 之后）。
     */
    void mark_unregistered() noexcept;

    /**
     * @brief 是否处于固定注册状态。
     */
    bool is_registered() const noexcept;

private:
    friend class BufferHandle;

    /**
     * @brief oneshot Handle 析构：id 推回空闲表并增加 `available_count_`。
     * @param id 合法物理编号；越界忽略。
     */
    void release(BufferId id) noexcept;

    Config config_{};  /**< 构造参数副本，供 capacity / buffer_size 查询。 */
    std::unique_ptr<std::byte[]> storage_;  /**< 整池连续存储；空池为空。 */
    std::vector<BufferSlot> slots_;  /**< `slots_[id]` 指向 storage 切块，地址稳定。 */
    IdFreeList free_ids_;  /**< 可 try_acquire 的 id；Ring reserve 的不在此。 */
    std::atomic<std::size_t> available_count_{0};  /**< 空闲个数快照，非精确锁。 */
    bool registered_{false};  /**< 已 `register_buffers`；fixed_index == BufferId。 */
};
