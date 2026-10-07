/* buffer_pool.cpp, BufferPool：storage + Free ids */

#include "memory/buffer_pool.hpp"

#include <stdexcept>
#include <thread>

BufferPool::BufferPool(Config config)
    : config_(config)
    // 空池也要合法环：MPSC 容量至少为 2
    , free_ids_(config.buffer_count == 0 ? 2 : config.buffer_count)
{
    const std::size_t count = config.buffer_count;
    const std::size_t size = config.buffer_size;
    if (count == 0 || size == 0) {
        return;
    }
    storage_ = std::make_unique<std::byte[]>(count * size);
    slots_.resize(count);
    for (std::size_t i = 0; i < count; ++i) {
        slots_[i].data = storage_.get() + i * size;
        slots_[i].capacity = static_cast<std::uint32_t>(size);
        if (!free_ids_.try_push(static_cast<BufferId>(i))) {
            throw std::logic_error("BufferPool: free_ids try_push failed during init");
        }
    }
    available_count_.store(count, std::memory_order_relaxed);
}

BufferPool::~BufferPool() = default;

std::optional<BufferHandle> BufferPool::try_acquire()
{
    BufferId id = 0;
    if (!free_ids_.try_pop(id) || id >= slots_.size()) {
        return std::nullopt;
    }
    available_count_.fetch_sub(1, std::memory_order_relaxed);
    return BufferHandle(this, id, &slots_[id]);
}

std::vector<BufferId> BufferPool::reserve(std::size_t count)
{
    std::vector<BufferId> ids;
    ids.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        BufferId id = 0;
        // 中途失败必须全部放回，避免 Ring 拿到短列表却以为独占了 count 块
        if (!free_ids_.try_pop(id) || id >= slots_.size()) {
            release_reserved(ids);
            return {};
        }
        ids.push_back(id);
        available_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    return ids;
}

void BufferPool::release_reserved(std::span<const BufferId> ids) noexcept
{
    for (BufferId id : ids) {
        release(id);
    }
}

BufferSlot& BufferPool::slot(BufferId id) noexcept
{
    return slots_[id];
}

const BufferSlot& BufferPool::slot(BufferId id) const noexcept
{
    return slots_[id];
}

std::size_t BufferPool::capacity() const noexcept
{
    return config_.buffer_count;
}

std::size_t BufferPool::available() const noexcept
{
    return available_count_.load(std::memory_order_relaxed);
}

std::size_t BufferPool::buffer_size() const noexcept
{
    return config_.buffer_size;
}

void BufferPool::mark_registered() noexcept
{
    registered_ = true;
}

void BufferPool::mark_unregistered() noexcept
{
    registered_ = false;
}

bool BufferPool::is_registered() const noexcept
{
    return registered_;
}

void BufferPool::release(BufferId id) noexcept
{
    if (id >= slots_.size()) {
        return;
    }
    // 环理论上不会满（每个 id 至多一份）；满则让出 CPU 再试，避免析构失败丢 id
    while (!free_ids_.try_push(id)) {
        std::this_thread::yield();
    }
    available_count_.fetch_add(1, std::memory_order_relaxed);
}
