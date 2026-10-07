/* queue_sink.hpp, MultiShotSink 的库内实现：有界环，on_shot 入队、on_end 收尾 */

#pragma once

#include "backend/io_operation.hpp"

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

/**
 * @brief Multishot 的默认 Sink：把每笔载荷排进有界环，结束码单独保存。
 * @details 在 I/O 线程 `on_shot` / `on_end` 里只做搬移，不 `wait`、不分配。
 *          环在构造时 `resize`，之后 `on_shot` 满则丢弃（`T` 析构：Handle reclaim / fd close）。
 *          单生产者（I/O 线程）单消费者（同一线程或外部自行同步）。
 * @tparam T `Buffer` 或 `AcceptedFd`。
 */
template <typename T>
class QueueSink final : public MultiShotSink<T> {
public:
    /**
     * @brief 预分配 `capacity` 个槽，热路径不再扩容。
     * @param capacity 环大小，必须是 2 的幂。
     */
    explicit QueueSink(std::size_t capacity)
        : slots_(capacity)
        , cap_(capacity)
        , mask_(capacity - 1)
    {
        if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
            throw std::logic_error("QueueSink: capacity must be power of two");
        }
    }

    /**
     * @brief 一笔成功载荷入环；满则丢弃（由 `value` 析构回收资源）。
     */
    void on_shot(T value) noexcept override
    {
        if (ended_ || count_ == cap_) {
            return;
        }
        const std::size_t i = (head_ + count_) & mask_;
        slots_[i] = std::move(value);
        ++count_;
    }

    /**
     * @brief 记录结束原因；之后的 `on_shot` 忽略。
     * @param result 0 正常结束，负数为 `-errno`。
     */
    void on_end(int result) noexcept override
    {
        if (ended_) {
            return;
        }
        ended_ = true;
        result_ = result;
    }

    /**
     * @brief 取出最早一笔；空环返回 false。
     * @param out 接收所有权。
     */
    bool try_pop(T& out) noexcept
    {
        if (count_ == 0) {
            return false;
        }
        out = std::move(*slots_[head_]);
        slots_[head_].reset();
        head_ = (head_ + 1) & mask_;
        --count_;
        return true;
    }

    /**
     * @brief 当前未取出的笔数。
     */
    std::size_t size() const noexcept
    {
        return count_;
    }

    /**
     * @brief 是否已经 `on_end`。
     */
    bool ended() const noexcept
    {
        return ended_;
    }

    /**
     * @brief `on_end` 写入的结果；未结束时无意义。
     */
    int result() const noexcept
    {
        return result_;
    }

private:
    std::vector<std::optional<T>> slots_;  /**< 预分配环槽，避免 on_shot 里 realloc。 */
    std::size_t cap_{0};                   /**< 环容量，2 的幂。 */
    std::size_t mask_{0};                  /**< cap_ - 1，下标用 `& mask_`。 */
    std::size_t head_{0};                  /**< 下一笔 try_pop 的下标。 */
    std::size_t count_{0};                 /**< 已入队未取出数量。 */
    int result_{0};                        /**< on_end 的 result。 */
    bool ended_{false};                    /**< 是否已收尾。 */
};
