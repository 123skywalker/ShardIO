/* endpoint.hpp, sockaddr 存储：IP + 端口，不属于 TcpStream 核心 */

#pragma once

#include <cstdint>
#include <netinet/in.h>
#include <sys/socket.h>

/**
 * @brief 对端或本端地址：`sockaddr` 字节 + 实际长度。
 * @details 供 `TcpStream::connect` / `TcpListener::bind` 使用。不解析 DNS，不拥有 socket。
 *          公共 API 的 IP/端口均为**主机字节序**；内部再 `htonl`/`htons`。
 */
class Endpoint {
public:
    /**
     * @brief 空地址，`size() == 0`。
     */
    Endpoint() noexcept = default;

    /**
     * @brief 从已有 `sockaddr` 拷贝进内部存储。
     * @param addr 源地址指针，须非空。
     * @param length 有效字节数，不超过 `sizeof(sockaddr_storage)`。
     */
    Endpoint(const sockaddr* addr, socklen_t length);

    /**
     * @brief 从 IPv4 地址构造（主机字节序）。
     * @param ipv4 `AF_INET` 地址，主机字节序（如 `INADDR_LOOPBACK`）。
     * @param port 主机字节序端口。
     * @return 填好的 `Endpoint`（内部转为网络序写入 `sockaddr_in`）。
     */
    static Endpoint ipv4(in_addr_t ipv4, std::uint16_t port);

    /**
     * @brief 从 IPv6 地址构造（端口为主机字节序）。
     * @param ipv6 16 字节 IPv6 地址。
     * @param port 主机字节序端口。
     * @return 填好的 `Endpoint`（端口内部 `htons`）。
     */
    static Endpoint ipv6(const in6_addr& ipv6, std::uint16_t port);

    /**
     * @brief 供 `connect` / `bind` 使用的可变 `sockaddr` 指针。
     */
    sockaddr* native() noexcept;

    /**
     * @brief 只读 `sockaddr` 指针。
     */
    const sockaddr* native() const noexcept;

    /**
     * @brief 当前地址的有效长度（`socklen_t`）。
     */
    socklen_t size() const noexcept;

    /**
     * @brief 地址族（`AF_INET` / `AF_INET6` / `AF_UNSPEC`）。
     */
    sa_family_t family() const noexcept;

private:
    sockaddr_storage storage_{};  /**< 足够容纳 IPv4/IPv6 的存储。 */
    socklen_t length_{0};         /**< `storage_` 中有效字节数；0 表示空。 */
};
