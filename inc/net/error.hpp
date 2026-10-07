/* error.hpp, 网络 syscall / io_uring res 转到 error_code */

#pragma once

#include <cerrno>
#include <system_error>

/**
 * @brief POSIX errno → `error_code`。
 */
inline std::error_code net_errno(int err) noexcept
{
    return std::error_code(err, std::generic_category());
}

/**
 * @brief io_uring `cqe->res`：负数为 `-errno`，非负表示成功。
 */
inline std::error_code net_uring(int res) noexcept
{
    return res < 0 ? net_errno(-res) : std::error_code{};
}
