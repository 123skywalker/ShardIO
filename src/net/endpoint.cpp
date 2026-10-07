/* endpoint.cpp, sockaddr 存储：主机序 API，内部网络序 */

#include "net/endpoint.hpp"

#include <cstring>

Endpoint::Endpoint(const sockaddr* addr, socklen_t length)
{
    if (addr == nullptr || length == 0 ||
        length > static_cast<socklen_t>(sizeof(storage_))) {
        return;
    }
    std::memcpy(&storage_, addr, static_cast<std::size_t>(length));
    length_ = length;
}

Endpoint Endpoint::ipv4(in_addr_t ipv4, std::uint16_t port)
{
    Endpoint address;
    auto* in = reinterpret_cast<sockaddr_in*>(&address.storage_);
    in->sin_family = AF_INET;
    in->sin_port = htons(port);
    in->sin_addr.s_addr = htonl(ipv4);
    address.length_ = sizeof(sockaddr_in);
    return address;
}

Endpoint Endpoint::ipv6(const in6_addr& ipv6, std::uint16_t port)
{
    Endpoint address;
    auto* in6 = reinterpret_cast<sockaddr_in6*>(&address.storage_);
    in6->sin6_family = AF_INET6;
    in6->sin6_port = htons(port);
    in6->sin6_addr = ipv6;
    address.length_ = sizeof(sockaddr_in6);
    return address;
}

sockaddr* Endpoint::native() noexcept
{
    return reinterpret_cast<sockaddr*>(&storage_);
}

const sockaddr* Endpoint::native() const noexcept
{
    return reinterpret_cast<const sockaddr*>(&storage_);
}

socklen_t Endpoint::size() const noexcept
{
    return length_;
}

sa_family_t Endpoint::family() const noexcept
{
    return length_ == 0 ? AF_UNSPEC : storage_.ss_family;
}
