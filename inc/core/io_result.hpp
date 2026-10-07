/* io_result.hpp, 网络操作的成功值 + error_code，不抛普通 socket 错误 */

#pragma once

#include <system_error>

/**
 * @brief 一次异步网络操作的结果：成功载荷或 `error_code`。
 * @details 普通 socket 错误（ECONNRESET 等）放在 `error` 中，不抛异常。
 *          Backend / 运行时内部的严重失败仍可抛。`error == {}` 表示成功。
 * @tparam T 成功时的值类型，如 `std::size_t`、`TcpStream`。
 */
template <typename T>
struct IoResult {
    /**
     * @brief 成功时的返回值。
     * @details recv 中 `value == 0` 且无 error 表示 TCP EOF，不是失败。
     */
    T value{};

    /**
     * @brief 失败原因；默认构造的 `error_code` 表示成功。
     */
    std::error_code error{};

    /**
     * @brief 是否成功（`error` 为空）。
     * @return 成功为 true，即使 `value == 0`（EOF）也为 true。
     */
    explicit operator bool() const noexcept
    {
        return !error;
    }
};

/**
 * @brief 不携带成功值的 I/O 结果。
 */
template <>
struct IoResult<void> {
    std::error_code error{}; /**< 失败原因；为空表示操作成功。 */

    /**
     * @brief 判断操作是否成功。
     * @return `error` 为空时返回 true。
     */
    explicit operator bool() const noexcept
    {
        return !error;
    }
};
