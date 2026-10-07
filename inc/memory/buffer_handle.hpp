/* buffer_handle.hpp, oneshot BufferHandle 与 provided Buffer */

#pragma once

#include "memory/buffer.hpp"

#include <cstddef>
#include <cstdint>

class BufferPool;
class BufferRing;

/**
 * @brief oneshot 借出句柄：析构一定调用 `BufferPool::release`，不碰 BufferRing。
 * @details 由 `try_acquire` 构造。对象很小（指针 + id + 长度），无堆分配。
 *          Recv/Send 的 Op 不持有本对象；调用方须保证 Handle 活过对应 I/O 完成。
 */
class BufferHandle {
public:
    /**
     * @brief 空句柄，不拥有任何块；析构为空操作。
     */
    BufferHandle() noexcept = default;

    /**
     * @brief 若仍拥有块则归还 Pool。
     */
    ~BufferHandle();

    /**
     * @brief 转移所有权；源变为空句柄，避免双重 release。
     * @param other 被掏空的源句柄。
     */
    BufferHandle(BufferHandle&& other) noexcept;

    /**
     * @brief 先归还自身持有块，再接管 `other`。
     * @param other 被掏空的源句柄。
     * @return `*this`。
     */
    BufferHandle& operator=(BufferHandle&& other) noexcept;

    /** @brief 禁止拷贝，避免同一块被 release 两次。 */
    BufferHandle(const BufferHandle&) = delete;
    /** @brief 禁止拷贝赋值。 */
    BufferHandle& operator=(const BufferHandle&) = delete;

    /**
     * @brief 可写视图：Slot 地址 + 当前 `size()`。
     * @return 空句柄时 `{nullptr, 0}`。
     */
    MutableBuffer view() noexcept;

    /**
     * @brief 只读视图，供 Send 使用。
     * @return 空句柄时 `{nullptr, 0}`。
     */
    ConstBuffer view() const noexcept;

    /**
     * @brief 设置当前有效长度（不写回 Slot）。
     * @param size 目标长度；超过 capacity 则钳到 capacity。空句柄忽略。
     */
    void resize(std::size_t size) noexcept;

    /**
     * @brief 当前有效字节数（`resize` 或默认 0）。
     */
    std::size_t size() const noexcept;

    /**
     * @brief 物理块容量；空句柄为 0。
     */
    std::size_t capacity() const noexcept;

    /**
     * @brief Pool 内物理编号；已 register_buffers 时可作 fixed `buf_index`。
     */
    BufferId id() const noexcept;

    /**
     * @brief 所属 Pool 是否已 `register_buffers`（`id()` 可作 fixed `buf_index`）。
     */
    bool registered() const noexcept;

    /**
     * @brief 是否仍拥有一块有效 Slot。
     */
    explicit operator bool() const noexcept;

private:
    friend class BufferPool;

    /**
     * @brief 由 Pool `try_acquire` 构造的拥有句柄。
     * @param pool 归还目标，非空。
     * @param id 物理块编号。
     * @param slot 对应 Slot 指针，与 `id` 一致。
     */
    BufferHandle(BufferPool* pool, BufferId id, BufferSlot* slot) noexcept;

    /**
     * @brief 若仍拥有则 `Pool::release(id_)` 并清空指针。
     */
    void return_owned() noexcept;

    BufferPool* pool_{nullptr};  /**< 归还目标；空句柄为 nullptr。 */
    BufferSlot* slot_{nullptr};  /**< 物理块描述；随 pool_ 清空。 */
    BufferId id_{0};             /**< 归还时交给 Pool 的编号。 */
    std::uint32_t size_{0};      /**< 当前有效长度，不写回 Slot。 */
};

/**
 * @brief RecvMulti checkout 句柄：析构一定 `BufferRing::reclaim(bid)`。
 * @details 须在所属 I/O 线程析构（热路径无锁）。跨线程析构未实现。无堆分配。
 */
class Buffer {
public:
    /**
     * @brief 空句柄，析构不 reclaim。
     */
    Buffer() noexcept = default;

    /**
     * @brief 若仍拥有则把 bid 送入 Ring 的 pending reclaim。
     */
    ~Buffer();

    /**
     * @brief 转移所有权；源变为空，避免双重 reclaim。
     * @param other 被掏空的源句柄。
     */
    Buffer(Buffer&& other) noexcept;

    /**
     * @brief 先 reclaim 自身，再接管 `other`。
     * @param other 被掏空的源句柄。
     * @return `*this`。
     */
    Buffer& operator=(Buffer&& other) noexcept;

    /** @brief 禁止拷贝，避免同一 bid 被 reclaim 两次。 */
    Buffer(const Buffer&) = delete;
    /** @brief 禁止拷贝赋值。 */
    Buffer& operator=(const Buffer&) = delete;

    /**
     * @brief 可写视图：收到数据所在地址 + `size()`。
     * @return 空句柄时 `{nullptr, 0}`。
     */
    MutableBuffer view() noexcept;

    /**
     * @brief 只读视图。
     * @return 空句柄时 `{nullptr, 0}`。
     */
    ConstBuffer view() const noexcept;

    /**
     * @brief 按 CQE `res` 设置有效长度；超过 capacity 则钳制。
     * @param size 本笔收到的字节数。
     */
    void resize(std::size_t size) noexcept;

    /**
     * @brief 当前有效字节数。
     */
    std::size_t size() const noexcept;

    /**
     * @brief 物理块容量；空句柄为 0。
     */
    std::size_t capacity() const noexcept;

    /**
     * @brief 内核 CQE 带回的 bid（Ring 下标，不是 Pool id）。
     */
    BufferBid bid() const noexcept;

    /**
     * @brief 是否仍对应一次尚未 reclaim 的 checkout。
     */
    explicit operator bool() const noexcept;

private:
    friend class BufferRing;

    /**
     * @brief 由 Ring `checkout` 构造。
     * @param ring 归还目标，非空。
     * @param bid Ring 内下标。
     * @param slot `reserved_ids_[bid]` 对应的物理 Slot。
     */
    Buffer(BufferRing* ring, BufferBid bid, BufferSlot* slot) noexcept;

    /**
     * @brief 若仍拥有则 `Ring::reclaim(bid_)` 并清空指针。
     */
    void return_owned() noexcept;

    BufferRing* ring_{nullptr};  /**< 归还目标；空句柄为 nullptr。须在 I/O 线程使用。 */
    BufferSlot* slot_{nullptr};  /**< 本 bid 对应物理块。 */
    BufferBid bid_{0};           /**< 析构时推进 pending reclaim 的编号。 */
    std::uint32_t size_{0};      /**< 本笔有效长度，不写回 Slot。 */
};
