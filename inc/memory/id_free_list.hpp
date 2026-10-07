/* id_free_list.hpp, 有界无锁空闲 Buffer id 表（复用 MPSCQueue） */

#pragma once

#include "runtime/mpsc_queue.hpp"

#include <cstddef>
#include <cstdint>

/**
 * @brief 有界无锁空闲 id 表：多线程 `try_push`（归还），单线程 `try_pop`（领取）。
 * @details 供每 `IoContext` 一块的 `BufferPool` 使用。Owner 上 `try_acquire` / `reserve`，
 *          任意线程 oneshot Handle 析构时 `release`。内部复用 inbox 同款 `MPSCQueue<uint32_t>`。
 */
class IdFreeList {
public:
    /**
     * @brief 构造容量向上取整为不小于 2 的 2 的幂的空环，由 Pool 再填入全部 id。
     * @param capacity 期望槽位数，通常等于 `buffer_count`。
     */
    explicit IdFreeList(std::size_t capacity);

    /**
     * @brief 释放内部环。须在无并发 push/pop 后调用。
     */
    ~IdFreeList() = default;

    /** @brief 禁止拷贝：环内序号与并发协议不可复制。 */
    IdFreeList(const IdFreeList&) = delete;
    /** @brief 禁止拷贝赋值。 */
    IdFreeList& operator=(const IdFreeList&) = delete;
    /** @brief 禁止移动：Pool 成员地址须稳定。 */
    IdFreeList(IdFreeList&&) = delete;
    /** @brief 禁止移动赋值。 */
    IdFreeList& operator=(IdFreeList&&) = delete;

    /**
     * @brief 归还一个空闲 id（任意线程）。
     * @param id Pool 内合法 buffer 编号。
     * @return 入队成功 true；环满 false（调用方应重试或视为逻辑错误）。
     */
    bool try_push(std::uint32_t id) noexcept;

    /**
     * @brief 领取一个空闲 id（仅 Pool 所在 owner 线程）。
     * @param id 成功时写入弹出的编号。
     * @return 成功 true；无已发布空闲 false。
     */
    bool try_pop(std::uint32_t& id) noexcept;

    /**
     * @brief 消费者视角是否无已发布 id。
     */
    bool empty() const noexcept;

    /**
     * @brief 实际环容量（2 的幂，可能大于构造时的期望值）。
     */
    std::size_t capacity() const noexcept;

private:
    MPSCQueue<std::uint32_t> ids_;  /**< 空闲 buffer id；push 任意线程，pop 仅 owner。 */
};
