/* buffer.hpp, BufferId / BufferBid / BufferSlot / 非拥有视图 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

/**
 * @brief BufferPool 内部物理块编号。
 * @details 与 `io_uring_register_buffers` 的 iovec 下标约定相同（`fixed_index == BufferId`）。
 *          不是 BufferRing 交给内核的 bid。
 */
using BufferId = std::uint32_t;

/**
 * @brief BufferRing / io_uring provided-buffer 使用的 bid。
 * @details 值为 Ring 内 `reserved_ids_` 的下标（0 .. capacity-1），不要与 Pool 的 BufferId 等同。
 */
using BufferBid = std::uint16_t;

/**
 * @brief 未走 `io_uring_register_buffers` 时，Recv/Send/SendZc 的 `buf_index` 哨兵。
 * @details 有效时使用 `prep_*_fixed`；本值为全 1，表示走普通 recv/send。
 */
inline constexpr BufferId kUnregisteredBuffer = ~BufferId{0};

/**
 * @brief 一块固定地址物理内存的描述，不含 Free / CheckedOut 等运行时占用状态。
 * @details 地址在 Pool 构造时切好，之后不变。有效载荷长度由 Handle 的 `size_` 记录，不写回本结构。
 */
struct BufferSlot {
    std::byte* data{nullptr};   /**< payload 起始地址，Pool 初始化后不变。 */
    std::uint32_t capacity{0};  /**< 本块最大字节数，构造后不变。 */
};

/**
 * @brief 可写非拥有视图：地址 + 当前有效长度。
 * @details 供 RecvOp / 调用方填写 SQE。不负责归还内存；寿命须短于对应 Handle 或稳定存储。
 */
struct MutableBuffer {
    std::byte* data{nullptr};  /**< 可写起始地址；空视图为 nullptr。 */
    std::size_t size{0};       /**< 当前有效字节数，不超过所属 Slot 的 capacity。 */
    BufferId fixed_index{kUnregisteredBuffer}; /**< registered buffer 下标；普通视图为哨兵。 */

    /**
     * @brief 从连续可写字节区间构造视图。
     * @param bytes 由调用方持有并保证存活到异步操作完成的区间。
     */
    MutableBuffer(std::span<std::byte> bytes) noexcept
        : data(bytes.data()), size(bytes.size()) {}

    /** @brief 构造空视图。 */
    MutableBuffer() noexcept = default;

    /**
     * @brief 从地址和长度构造视图。
     * @param first 可写区间首地址。
     * @param length 可写区间字节数。
     */
    MutableBuffer(std::byte* first, std::size_t length,
                  BufferId index = kUnregisteredBuffer) noexcept
        : data(first), size(length), fixed_index(index) {}
};

/**
 * @brief 只读非拥有视图，供 Send / SendZc 填写 SQE。
 * @details 不拥有内存，不延长缓冲寿命。
 */
struct ConstBuffer {
    const std::byte* data{nullptr};  /**< 只读起始地址；空视图为 nullptr。 */
    std::size_t size{0};             /**< 待发送或已有效的字节数。 */
    BufferId fixed_index{kUnregisteredBuffer}; /**< registered buffer 下标；普通视图为哨兵。 */

    /**
     * @brief 从连续只读字节区间构造视图。
     * @param bytes 由调用方持有并保证存活到异步操作完成的区间。
     */
    ConstBuffer(std::span<const std::byte> bytes) noexcept
        : data(bytes.data()), size(bytes.size()) {}

    /** @brief 构造空视图。 */
    ConstBuffer() noexcept = default;

    /**
     * @brief 从地址和长度构造视图。
     * @param first 只读区间首地址。
     * @param length 只读区间字节数。
     */
    ConstBuffer(const std::byte* first, std::size_t length,
                BufferId index = kUnregisteredBuffer) noexcept
        : data(first), size(length), fixed_index(index) {}
};
