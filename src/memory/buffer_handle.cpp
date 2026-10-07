/* buffer_handle.cpp, BufferHandle / Buffer RAII */

#include "memory/buffer_handle.hpp"
#include "memory/buffer_pool.hpp"
#include "memory/buffer_ring.hpp"

BufferHandle::BufferHandle(BufferPool* pool, BufferId id, BufferSlot* slot) noexcept
    : pool_(pool)
    , slot_(slot)
    , id_(id)
    , size_(0)
{
}

void BufferHandle::return_owned() noexcept
{
    if (pool_ == nullptr) {
        return;
    }
    pool_->release(id_);
    pool_ = nullptr;
    slot_ = nullptr;
}

BufferHandle::~BufferHandle()
{
    return_owned();
}

BufferHandle::BufferHandle(BufferHandle&& other) noexcept
    : pool_(other.pool_)
    , slot_(other.slot_)
    , id_(other.id_)
    , size_(other.size_)
{
    // 掏空源，析构时不再 release
    other.pool_ = nullptr;
    other.slot_ = nullptr;
}

BufferHandle& BufferHandle::operator=(BufferHandle&& other) noexcept
{
    if (this == &other) {
        return *this;
    }
    return_owned();
    pool_ = other.pool_;
    slot_ = other.slot_;
    id_ = other.id_;
    size_ = other.size_;
    other.pool_ = nullptr;
    other.slot_ = nullptr;
    return *this;
}

MutableBuffer BufferHandle::view() noexcept
{
    return slot_ == nullptr ? MutableBuffer{} : MutableBuffer{slot_->data, size_};
}

ConstBuffer BufferHandle::view() const noexcept
{
    return slot_ == nullptr ? ConstBuffer{}
                            : ConstBuffer{slot_->data, size_};
}

void BufferHandle::resize(std::size_t size) noexcept
{
    if (slot_ == nullptr) {
        return;
    }
    if (size > slot_->capacity) {
        size = slot_->capacity;
    }
    size_ = static_cast<std::uint32_t>(size);
}

std::size_t BufferHandle::size() const noexcept
{
    return size_;
}

std::size_t BufferHandle::capacity() const noexcept
{
    return slot_ == nullptr ? 0 : slot_->capacity;
}

BufferId BufferHandle::id() const noexcept
{
    return id_;
}

bool BufferHandle::registered() const noexcept
{
    return pool_ != nullptr && pool_->is_registered();
}

BufferHandle::operator bool() const noexcept
{
    return pool_ != nullptr && slot_ != nullptr;
}

Buffer::Buffer(BufferRing* ring, BufferBid bid,
                                           BufferSlot* slot) noexcept
    : ring_(ring)
    , slot_(slot)
    , bid_(bid)
    , size_(0)
{
}

void Buffer::return_owned() noexcept
{
    if (ring_ == nullptr) {
        return;
    }
    // 热路径：只推进 Ring pending，由 batch/低水位再 advance
    ring_->reclaim(bid_);
    ring_ = nullptr;
    slot_ = nullptr;
}

Buffer::~Buffer()
{
    return_owned();
}

Buffer::Buffer(Buffer&& other) noexcept
    : ring_(other.ring_)
    , slot_(other.slot_)
    , bid_(other.bid_)
    , size_(other.size_)
{
    other.ring_ = nullptr;
    other.slot_ = nullptr;
}

Buffer& Buffer::operator=(
    Buffer&& other) noexcept
{
    if (this == &other) {
        return *this;
    }
    return_owned();
    ring_ = other.ring_;
    slot_ = other.slot_;
    bid_ = other.bid_;
    size_ = other.size_;
    other.ring_ = nullptr;
    other.slot_ = nullptr;
    return *this;
}

MutableBuffer Buffer::view() noexcept
{
    return slot_ == nullptr
        ? MutableBuffer{}
        : MutableBuffer{slot_->data, size_, ring_->fixed_index(bid_)};
}

ConstBuffer Buffer::view() const noexcept
{
    return slot_ == nullptr
        ? ConstBuffer{}
        : ConstBuffer{slot_->data, size_, ring_->fixed_index(bid_)};
}

void Buffer::resize(std::size_t size) noexcept
{
    if (slot_ == nullptr) {
        return;
    }
    if (size > slot_->capacity) {
        size = slot_->capacity;
    }
    size_ = static_cast<std::uint32_t>(size);
}

std::size_t Buffer::size() const noexcept
{
    return size_;
}

std::size_t Buffer::capacity() const noexcept
{
    return slot_ == nullptr ? 0 : slot_->capacity;
}

BufferBid Buffer::bid() const noexcept
{
    return bid_;
}

Buffer::operator bool() const noexcept
{
    return ring_ != nullptr && slot_ != nullptr;
}
