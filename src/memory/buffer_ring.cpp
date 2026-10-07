/* buffer_ring.cpp, BufferRing：Provided ↔ CheckedOut ↔ pending reclaim */

#include "memory/buffer_ring.hpp"

#include <liburing.h>
#include <stdexcept>

BufferRing::BufferRing(BufferPool& pool, Config config)
    : pool_(&pool)
    , config_(config)
{
    // 预留满容量，reclaim 热路径不得 realloc
    pending_reclaims_.reserve(config.capacity == 0 ? 1 : config.capacity);
    reserved_ids_ = pool.reserve(config.capacity);
    if (config.capacity != 0 && reserved_ids_.size() != config.capacity) {
        throw std::logic_error("BufferRing: pool has too few free buffers");
    }
}

BufferRing::~BufferRing()
{
    unbind_kernel();
}

std::uint16_t BufferRing::group_id() const noexcept
{
    return config_.group_id;
}

std::size_t BufferRing::capacity() const noexcept
{
    return config_.capacity;
}

std::size_t BufferRing::kernel_available() const noexcept
{
    return kernel_available_;
}

std::size_t BufferRing::pending_reclaims() const noexcept
{
    return pending_reclaims_.size();
}

BufferId BufferRing::fixed_index(BufferBid bid) const noexcept
{
    return reserved_ids_[bid];
}

Buffer BufferRing::checkout(BufferBid bid) noexcept
{
    if (static_cast<std::size_t>(bid) >= reserved_ids_.size()) {
        return {};
    }
    // 内核已消耗该块；计数只表达可见性，不维护每 bid 状态机
    if (kernel_available_ > 0) {
        --kernel_available_;
    }
    const BufferId id = reserved_ids_[bid];
    return Buffer(this, bid, &pool_->slot(id));
}

void BufferRing::flush_reclaims() noexcept
{
    const std::size_t n = pending_reclaims_.size();
    if (n == 0 || br_ == nullptr) {
        return;
    }
    unsigned offset = 0;
    for (BufferBid bid : pending_reclaims_) {
        const BufferId id = reserved_ids_[bid];
        BufferSlot& s = pool_->slot(id);
        io_uring_buf_ring_add(
            br_,
            s.data,
            s.capacity,
            bid,
            mask_,
            static_cast<int>(offset));
        ++offset;
    }
    // 一次 advance 公布本批，无 syscall
    io_uring_buf_ring_advance(br_, static_cast<int>(n));
    kernel_available_ += n;
#if defined(SHARDIO_RING_STATS)
    ++flush_count_;
    buffers_advanced_ += n;
#endif
    pending_reclaims_.clear();
}

#if defined(SHARDIO_RING_STATS)
std::uint64_t BufferRing::flush_count() const noexcept
{
    return flush_count_;
}

std::uint64_t BufferRing::buffers_advanced() const noexcept
{
    return buffers_advanced_;
}
#endif

void BufferRing::flush_if_needed() noexcept
{
    if (pending_reclaims_.empty()) {
        return;
    }
    if (pending_reclaims_.size() >= config_.reclaim_batch ||
        kernel_available_ <= config_.low_watermark) {
        flush_reclaims();
    }
}

void BufferRing::provide_all() noexcept
{
    if (br_ == nullptr || provided_ || reserved_ids_.empty()) {
        return;
    }
    unsigned offset = 0;
    // bid 用 Ring 下标，内核 CQE 带回的就是这个值
    for (BufferBid bid = 0; bid < reserved_ids_.size(); ++bid) {
        BufferSlot& s = pool_->slot(reserved_ids_[bid]);
        io_uring_buf_ring_add(
            br_, s.data, s.capacity, bid, mask_, static_cast<int>(offset));
        ++offset;
    }
    io_uring_buf_ring_advance(br_, static_cast<int>(offset));
    kernel_available_ = offset;
    provided_ = true;
}

void BufferRing::reclaim(BufferBid bid) noexcept
{
    pending_reclaims_.push_back(bid);
    flush_if_needed();
}

void BufferRing::bind_kernel(io_uring_buf_ring* br, int mask) noexcept
{
    br_ = br;
    mask_ = mask;
}

void BufferRing::unbind_kernel() noexcept
{
    if (br_ != nullptr) {
        flush_reclaims();
        br_ = nullptr;
        mask_ = 0;
        kernel_available_ = 0;
        provided_ = false;
    }
    if (!reserved_ids_.empty()) {
        pool_->release_reserved(reserved_ids_);
        reserved_ids_.clear();
    }
}
