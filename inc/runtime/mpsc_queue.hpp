/* mpsc_queue.hpp, 有界无锁 MPSC 队列（Vyukov 环形缓冲） */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>

/**
 * @brief 有界 MPSC 队列：多个生产者可并发 `try_push`，仅一个消费者 `drain`。
 * @details 供 `IoContext::inbox_` 使用。内部是带 per-slot sequence 的环形缓冲
 *          （Vyukov 有界环的 MPSC 裁剪：入队 CAS `tail_`，出队单线程更新 `head_`）。
 *          不能只用 head/tail 计数：生产者占号与写槽之间会有空洞。
 *          不负责 wakeup；满时 `try_push` 返回 false，由调用方处理。
 *
 * @tparam T 可移动类型（如 `TaskFn`）。出队后槽内留下 moved-from 对象，下次入队再赋值。
 */
template <class T>
class MPSCQueue {
public:
    /**
     * @brief 构造环形缓冲，容量向上取整为不小于 2 的 2 的幂。
     * @param capacity 期望槽位数。
     */
    explicit MPSCQueue(std::size_t capacity = 256);

    /**
     * @brief 释放缓冲。必须在无并发 `try_push`/`drain` 后调用。
     */
    ~MPSCQueue();

    MPSCQueue(const MPSCQueue&) = delete;
    MPSCQueue& operator=(const MPSCQueue&) = delete;
    MPSCQueue(MPSCQueue&&) = delete;
    MPSCQueue& operator=(MPSCQueue&&) = delete;

    /**
     * @brief 生产者尝试入队（任意线程）。
     * @param value 成功才移入槽位；失败时调用方对象保持有效，便于重试。
     * @return 成功 true；环满 false，不阻塞。
     */
    bool try_push(T&& value);

    /**
     * @brief 单消费者批量出队（仅 IoContext owner 线程）。
     * @param output 输出缓冲，写入数不超过其 size。
     * @return 实际取出个数；无已发布元素时返回 0。
     */
    std::size_t drain(std::span<T> output);

    /**
     * @brief 消费者视角是否为空（看 `head_` 槽是否已发布）。
     */
    bool empty() const noexcept;

    /**
     * @brief 实际槽位数（2 的幂）。
     */
    std::size_t capacity() const noexcept;

private:
    /**
     * @brief 把 n 向上取整为 2 的幂，且至少为 2。
     */
    static std::size_t round_pow2(std::size_t n) noexcept;

    /**
     * @brief 环形上一格：seq 表示该槽目前属于哪一代 ticket。
     * @details 初始 `seq == 下标`。生产者在 `seq == pos` 时写入，再 `seq = pos+1` 发布。
     *          消费者在 `seq == pos+1` 时取走，再 `seq = pos+capacity` 交给下一圈。
     */
    struct Slot {
        std::atomic<std::size_t> seq{0};  /**< 与全局 ticket 对齐的代次，acquire/release 同步数据。 */
        T value{};                        /**< 槽内元素；未发布时内容无意义。 */
    };

    std::size_t capacity_{0};  /**< 槽位数，必为 2 的幂。 */
    std::size_t mask_{0};      /**< capacity_ - 1，用与运算代替取模。 */

    std::unique_ptr<Slot[]> slots_;  /**< 环形槽位，与 capacity_ 等长。 */

    alignas(64) std::atomic<std::size_t> head_{0};  /**< 消费者下一 ticket，仅单消费者存储。 */

    alignas(64) std::atomic<std::size_t> tail_{0};  /**< 生产者下一 ticket，多生产者 CAS 抢号。 */
};

template <class T>
std::size_t MPSCQueue<T>::round_pow2(std::size_t n) noexcept
{
    if (n < 2) {
        return 2;
    }
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if constexpr (sizeof(std::size_t) > 4) {
        n |= n >> 32;
    }
    return n + 1;
}

template <class T>
MPSCQueue<T>::MPSCQueue(std::size_t capacity)
    : capacity_(round_pow2(capacity))
    , mask_(capacity_ - 1)
    , slots_(std::make_unique<Slot[]>(capacity_))
{
    for (std::size_t i = 0; i < capacity_; ++i) {
        slots_[i].seq.store(i, std::memory_order_relaxed);
    }
}

template <class T>
MPSCQueue<T>::~MPSCQueue()
{
    T dump{};
    std::span<T> one{&dump, 1};
    while (drain(one) != 0) {
    }
}

template <class T>
bool MPSCQueue<T>::try_push(T&& value)
{
    std::size_t pos = tail_.load(std::memory_order_relaxed);

    for (;;) {
        Slot& cell = slots_[pos & mask_];
        const std::size_t seq = cell.seq.load(std::memory_order_acquire);
        // 有符号差：0 表示轮到本 ticket 写；<0 满；>0 号已被别人抢走，重读 tail。
        const std::intptr_t dif = static_cast<std::intptr_t>(seq) -
                                  static_cast<std::intptr_t>(pos);

        if (dif == 0) {
            if (tail_.compare_exchange_weak(
                    pos,
                    pos + 1,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                cell.value = std::move(value);
                cell.seq.store(pos + 1, std::memory_order_release);
                return true;
            }
            // CAS 失败时 pos 已更新为当前 tail，继续循环。 
        } else if (dif < 0) {
            // 未 move value，调用方仍可持有原对象重试
            return false;
        } else {
            pos = tail_.load(std::memory_order_relaxed);
        }
    }
}

template <class T>
std::size_t MPSCQueue<T>::drain(std::span<T> output)
{
    std::size_t n = 0;
    std::size_t pos = head_.load(std::memory_order_relaxed);

    while (n < output.size()) {
        Slot& cell = slots_[pos & mask_];
        const std::size_t seq = cell.seq.load(std::memory_order_acquire);
        // seq == pos+1 才是生产者已 release 的数据；否则空或尚未发布。 
        const std::intptr_t dif = static_cast<std::intptr_t>(seq) -
                                  static_cast<std::intptr_t>(pos + 1);
        if (dif != 0) {
            break;
        }

        output[n++] = std::move(cell.value);
        cell.seq.store(pos + capacity_, std::memory_order_release);
        ++pos;
        head_.store(pos, std::memory_order_relaxed);
    }
    return n;
}

template <class T>
bool MPSCQueue<T>::empty() const noexcept
{
    const std::size_t pos = head_.load(std::memory_order_relaxed);
    const std::size_t seq = slots_[pos & mask_].seq.load(std::memory_order_acquire);
    return seq != pos + 1;
}

template <class T>
std::size_t MPSCQueue<T>::capacity() const noexcept
{
    return capacity_;
}
