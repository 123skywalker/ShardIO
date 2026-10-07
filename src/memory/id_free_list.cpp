/* id_free_list.cpp, IdFreeList：转发到 MPSCQueue<uint32_t> */

#include "memory/id_free_list.hpp"

#include <span>
#include <utility>

IdFreeList::IdFreeList(std::size_t capacity)
    : ids_(capacity)
{
}

bool IdFreeList::try_push(std::uint32_t id) noexcept
{
    return ids_.try_push(std::move(id));
}

bool IdFreeList::try_pop(std::uint32_t& id) noexcept
{
    std::uint32_t out = 0;
    // 单消费者一次只领一个 id，复用 drain 的发布协议（与 inbox 相同）
    if (ids_.drain(std::span<std::uint32_t>(&out, 1)) == 0) {
        return false;
    }
    id = out;
    return true;
}

bool IdFreeList::empty() const noexcept
{
    return ids_.empty();
}

std::size_t IdFreeList::capacity() const noexcept
{
    return ids_.capacity();
}
