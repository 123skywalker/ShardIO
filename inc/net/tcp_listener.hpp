/* tcp_listener.hpp, 监听 socket：bind/listen 所有权 + 异步 accept → TcpStream */

#pragma once

#include "core/context.hpp"
#include "core/io_result.hpp"
#include "core/task.hpp"
#include "net/endpoint.hpp"
#include "net/tcp_stream.hpp"
#include "net/options.hpp"

#include <memory>

class AcceptStream;

/**
 * @brief 监听套接字及其所属 `Context`。
 * @details 拥有 listen fd。`accept()` 走 oneshot `AcceptOp`；`accept_multi()` 显式。
 *          不拥有 Buffer，不把连接分发到其它 IoContext（连接继承本 Listener 的 Context）。
 *          `close` 不自动 cancel；in-flight AcceptOp 须先结束再销毁本对象。
 */
class TcpListener {
public:
    /**
     * @brief 空监听器，不拥有 socket。
     */
    TcpListener() noexcept = default;

    /**
     * @brief 接管已经 `listen` 的 fd。
     * @param fd 监听套接字；本对象负责随后 `close`。
     * @param context 接受的连接将绑定到该执行域。
     */
    TcpListener(int fd, Context context) noexcept;

    /**
     * @brief 若仍持有 fd 则关闭。
     */
    ~TcpListener();

    /**
     * @brief 转移 listen fd；源变为空。
     * @param other 被掏空的源。
     */
    TcpListener(TcpListener&& other) noexcept;

    /**
     * @brief 先关闭自身，再接管 `other`。
     * @param other 被掏空的源。
     * @return `*this`。
     */
    TcpListener& operator=(TcpListener&& other) noexcept;

    /** @brief 禁止拷贝。 */
    TcpListener(const TcpListener&) = delete;
    /** @brief 禁止拷贝赋值。 */
    TcpListener& operator=(const TcpListener&) = delete;

    /**
     * @brief 创建 socket、`bind`、`listen`（同步，不进 io_uring）。
     * @param context 本监听器与随后接受连接所属的执行域。
     * @param endpoint 绑定地址（端口 0 表示由系统分配）。
     * @param options backlog 与 `SO_REUSEPORT` 配置。
     * @return 成功为已监听的 `TcpListener`；失败为 bind/listen 的 `error`。
     */
    static IoResult<TcpListener> bind(
        Context context, Endpoint endpoint, ListenOptions options = {});

    /**
     * @brief 异步接受一条新连接（oneshot AcceptOp）。
     * @return 成功时 `value` 为已接管新 fd 的 `TcpStream`（同一 Context）；失败为 `error`。
     */
    Task<IoResult<TcpStream>> accept(AcceptOptions options = {});

    /**
     * @brief 启动可由协程逐条消费的 multishot accept 流。
     * @param options 用户态队列容量和新连接配置。
     * @return 可通过 `next()` 获取连接的接受流。
     */
    AcceptStream accept_multi(AcceptMultiOptions options = {});

    /**
     * @brief 当前实际绑定的本端地址（`getsockname`）。
     * @details 端口 0 绑定之后可在此读到内核分配的端口。空监听器返回空地址。
     */
    Endpoint local_endpoint() const;

    /**
     * @brief 关闭 listen fd；之后不可再 `accept`。
     */
    void close() noexcept;

    /**
     * @brief 是否仍拥有 listen socket。
     */
    bool valid() const noexcept;

    /**
     * @brief 底层 listen fd，供 setsockopt / 调试。
     */
    int native_handle() const noexcept;

    /**
     * @brief 取得本监听器固定绑定的执行域句柄。
     * @return 接受连接也会继承的轻量 Context。
     */
    Context context() const noexcept;

private:
    int fd_{-1};            /**< 拥有的 listen fd；-1 表示空 / 已关闭。 */
    Context context_{};   /**< 接受连接所归属的 IoContext。 */
};
