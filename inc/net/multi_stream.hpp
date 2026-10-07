/* multi_stream.hpp, 面向协程的 multishot 接收与接受流 */

#pragma once

#include "core/task.hpp"
#include "memory/buffer_handle.hpp"
#include "net/tcp_stream.hpp"

#include <memory>
#include <optional>
#include <system_error>

class BufferRing;
class Context;
class TcpListener;
struct RecvMultiOptions;
struct AcceptMultiOptions;

/**
 * @brief multishot 流的一次读取结果。
 * @tparam T 单次成功事件携带的对象类型。
 */
template <typename T>
class StreamItem {
public:
    /** @brief 构造流结束结果。 */
    StreamItem() noexcept = default;

    /**
     * @brief 构造成功事件。
     * @param value 本次事件携带的对象。
     */
    explicit StreamItem(T value)
        : value_(std::move(value)) {}

    /**
     * @brief 构造错误结束结果。
     * @param error 导致流终止的系统错误。
     */
    explicit StreamItem(std::error_code error) noexcept
        : error_(error) {}

    /**
     * @brief 判断当前结果是否携带一个事件。
     * @return 有事件时返回 true；正常结束或错误结束时返回 false。
     */
    explicit operator bool() const noexcept { return value_.has_value(); }

    /**
     * @brief 取得本次事件对象。
     * @return 由当前结果持有的事件对象。
     */
    T& value() & { return *value_; }

    /**
     * @brief 移出本次事件对象。
     * @return 由当前结果持有的事件对象。
     */
    T&& value() && { return std::move(*value_); }

    /**
     * @brief 取得流终止错误。
     * @return 正常事件或正常结束时为空错误码。
     */
    const std::error_code& error() const noexcept { return error_; }

private:
    std::optional<T> value_{}; /**< 当前事件；空表示流已结束。 */
    std::error_code error_{};  /**< 流异常结束时的错误码。 */
};

/**
 * @brief TCP multishot recv 的协程消费对象。
 * @details 内部使用有界队列；每个 CQE 只入队并直接唤醒等待者，不创建额外 Task。
 */
class RecvStream {
public:
    /** @brief 构造空的接收流。 */
    RecvStream() noexcept = default;
    /** @brief 请求取消仍在执行的 multishot recv。 */
    ~RecvStream();
    /** @brief 转移接收流所有权。 */
    RecvStream(RecvStream&&) noexcept;
    /** @brief 转移接收流所有权。 */
    RecvStream& operator=(RecvStream&&) noexcept;
    /** @brief 禁止复制同一个消费游标。 */
    RecvStream(const RecvStream&) = delete;
    /** @brief 禁止复制赋值。 */
    RecvStream& operator=(const RecvStream&) = delete;

    /**
     * @brief 等待下一块接收缓冲区。
     * @return 有数据时携带 `Buffer`；流结束时为空，错误可由 `error()` 读取。
     */
    Task<StreamItem<Buffer>> next();

    /** @brief 异步请求取消当前 multishot recv；完成事件负责最终回收状态。 */
    void cancel() noexcept;

private:
    friend class TcpStream;
    struct State;

    /**
     * @brief 从已启动的共享状态构造用户句柄。
     * @param state 内部 multishot 状态。
     */
    explicit RecvStream(std::shared_ptr<State> state) noexcept;

    /**
     * @brief 创建并提交一个 multishot recv。
     * @param context socket 所属执行域。
     * @param fd 待读取的 socket 描述符。
     * @param ring 执行域自动管理的 provided-buffer ring。
     * @param capacity 用户态事件队列容量。
     * @return 已启动的接收流。
     */
    static RecvStream start(Context context, int fd, BufferRing& ring,
                            std::size_t capacity);

    std::shared_ptr<State> state_{}; /**< 与内核操作共享寿命的接收状态。 */
};

/**
 * @brief TCP multishot accept 的协程消费对象。
 */
class AcceptStream {
public:
    /** @brief 构造空的接受流。 */
    AcceptStream() noexcept = default;
    /** @brief 请求取消仍在执行的 multishot accept。 */
    ~AcceptStream();
    /** @brief 转移接受流所有权。 */
    AcceptStream(AcceptStream&&) noexcept;
    /** @brief 转移接受流所有权。 */
    AcceptStream& operator=(AcceptStream&&) noexcept;
    /** @brief 禁止复制同一个消费游标。 */
    AcceptStream(const AcceptStream&) = delete;
    /** @brief 禁止复制赋值。 */
    AcceptStream& operator=(const AcceptStream&) = delete;

    /**
     * @brief 等待下一条已接受连接。
     * @return 有连接时携带 `TcpStream`；流结束时为空。
     */
    Task<StreamItem<TcpStream>> next();

    /** @brief 异步请求取消当前 multishot accept。 */
    void cancel() noexcept;

private:
    friend class TcpListener;
    struct State;

    /**
     * @brief 从已启动的共享状态构造用户句柄。
     * @param state 内部 multishot 状态。
     */
    explicit AcceptStream(std::shared_ptr<State> state) noexcept;

    /**
     * @brief 创建并提交一个 multishot accept。
     * @param context listener 所属执行域。
     * @param fd 监听 socket 描述符。
     * @param capacity 用户态事件队列容量。
     * @param no_delay 是否为新连接启用 TCP_NODELAY。
     * @return 已启动的接受流。
     */
    static AcceptStream start(Context context, int fd, std::size_t capacity,
                              bool no_delay);

    std::shared_ptr<State> state_{}; /**< 与内核操作共享寿命的接受状态。 */
};
