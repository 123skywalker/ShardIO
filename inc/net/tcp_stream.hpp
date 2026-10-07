/* tcp_stream.hpp, TCP 连接：fd 所有权 + 所属 Context + 异步 socket 操作 */

#pragma once

#include "core/context.hpp"
#include "core/io_result.hpp"
#include "core/task.hpp"
#include "memory/buffer.hpp"
#include "memory/buffer_handle.hpp"
#include "net/endpoint.hpp"
#include "net/options.hpp"

#include <cstddef>
#include <memory>
#include <sys/socket.h>

class RecvStream;
class SendPipeline;
class BufferPool;
namespace shardio_detail {
class SendPipelineState;
}

/**
 * @brief 已创建的 TCP socket 及其所属 `Context`。
 * @details 拥有 fd（析构 / `close` 时关闭），连接在整个寿命期固定归属一个 Context。
 *          recv/send/connect：协程帧上构造 Op，`co_await OpAwaiter` → `Context::submit`。
 *          默认 recv/send 不增加虚调用、post 或 Scheduler ready queue。
 */
class TcpStream {
public:
    /**
     * @brief 空流，`fd == -1`，不拥有 socket。
     */
    TcpStream() noexcept = default;

    /**
     * @brief 接管已有 fd（accept / 已连接 socket）。
     * @param fd 已创建的套接字；本对象之后负责 `close`。
     * @param context 该连接固定归属的执行域句柄。
     */
    TcpStream(int fd, Context context) noexcept;

    /**
     * @brief 若仍持有 fd 则关闭。
     */
    ~TcpStream();

    /**
     * @brief 转移 fd 与 Context；源变为空流。
     * @param other 被掏空的源。
     */
    TcpStream(TcpStream&& other) noexcept;

    /**
     * @brief 先关闭自身 fd，再接管 `other`。
     * @param other 被掏空的源。
     * @return `*this`。
     */
    TcpStream& operator=(TcpStream&& other) noexcept;

    /** @brief 禁止拷贝，避免双重 close。 */
    TcpStream(const TcpStream&) = delete;
    /** @brief 禁止拷贝赋值。 */
    TcpStream& operator=(const TcpStream&) = delete;

    /**
     * @brief 创建 TCP socket 并异步 `connect` 到 `peer`。
     * @param context 新连接所属执行域。
     * @param endpoint 远端 IP 与端口。
     * @param options 连接建立后的 socket 配置。
     * @return 成功时 `value` 为已连接流；失败时 `error` 为拒绝 / 超时等。
     */
    static Task<IoResult<TcpStream>> connect(
        Context context, Endpoint endpoint, ConnectOptions options = {});

    /**
     * @brief 异步读取一段 TCP 字节流（oneshot，允许短读）。
     * @param buffer 可写视图；不保证读满 `buffer.size`。须活过本次完成。
     * @param options 接收缓冲和提交配置。
     * @return `value>0` 实收字节；`value==0` 且无 error 为 EOF；失败则 `error`。
     */
    Task<IoResult<std::size_t>> recv(MutableBuffer buffer,
                                     RecvOptions options = {});

    /**
     * @brief 启动使用 Context 缓冲域的 multishot 接收流。
     * @param options 用户态队列容量等高级配置。
     * @return 可通过 `next()` 逐块消费的接收流。
     */
    RecvStream recv_multi(RecvMultiOptions options = {});

    /**
     * @brief 创建连接专属的发送流水线，并自行分配固定数量的稳定缓冲块。
     * @details 同一连接同一时刻只应保留一个活动流水线；创建下一个前先关闭前一个。
     * @param options 深度、单块大小与零拷贝选项。
     * @return 绑定当前连接的 move-only 流水线。
     */
    SendPipeline send_pipeline(SendPipelineOptions options = {});

    /**
     * @brief 创建使用外部缓冲池的高级发送流水线。
     * @details 同一连接同一时刻只应保留一个活动流水线；创建下一个前先关闭前一个。
     * @param options 深度、单块大小与零拷贝选项；外部池单块容量须满足配置。
     * @param buffer_pool 自定义分配器、共享池或已注册内存的来源，须活过流水线。
     * @return 绑定当前连接的 move-only 流水线。
     */
    SendPipeline send_pipeline(SendPipelineOptions options,
                               BufferPool& buffer_pool);

    /**
     * @brief 异步发送一段数据，允许短写。
     * @param buffer 只读视图；须活过本次完成。
     * @param options zero-copy 与提交配置。
     * @return 本轮实际写出字节数，或 `error`。续传由 `send_all` 处理。
     */
    Task<IoResult<std::size_t>> send(ConstBuffer buffer,
                                     SendOptions options = {});

    /**
     * @brief 循环发送直到写完或失败。
     * @param buffer 待发送的完整区间。
     * @param options 每次底层 send 使用的配置。
     * @return 成功为空结果；失败时携带首个发送错误。
     */
    Task<IoResult<void>> send_all(ConstBuffer buffer, SendOptions options = {});

    /**
     * @brief TCP 半关闭或全关闭（`SHUT_RD` / `SHUT_WR` / `SHUT_RDWR`）。
     * @param how 需要关闭的 TCP 方向。
     * @return 成功为空 `error_code`，失败为对应错误。
     */
    std::error_code shutdown(Shutdown how = Shutdown::Both) noexcept;

    /**
     * @brief 关闭并放弃 fd；之后不可再提交本 socket 上的新 Op。
     * @details 不取消已经 in-flight 的 Operation。须先 `cancel`/`complete` 再销毁 `TcpStream`。
     */
    void close() noexcept;

    /**
     * @brief 是否仍拥有有效 socket（`fd_ >= 0`）。
     */
    bool valid() const noexcept;

    /**
     * @brief 底层 Linux fd，供 setsockopt / 调试；业务层尽量不要直接用。
     */
    int native_handle() const noexcept;

    /**
     * @brief 取得本连接固定绑定的执行域句柄。
     * @return 可用于 post、spawn、yield 与 CPU 任务的轻量 Context。
     */
    Context context() const noexcept;

private:
    int fd_{-1};            /**< 拥有的 TCP fd；-1 表示空 / 已关闭。 */
    Context context_{};   /**< 连接所属 IoContext；空句柄表示未绑定。 */
    std::shared_ptr<shardio_detail::SendPipelineState> send_pipeline_{}; /**< 按需创建的发送流水线状态，普通连接为空。 */
};

#include "net/multi_stream.hpp"
#include "net/send_pipeline.hpp"
