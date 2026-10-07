/* options.hpp, TCP 公共 API 的轻量配置项 */

#pragma once

#include <cstddef>

/**
 * @brief 控制一次操作由运行时自动提交，还是加入显式批次。
 */
enum class SubmitMode {
    Immediate, /**< 由普通 API 自动安排提交。 */
    Batch      /**< 由高级批量接口统一提交。 */
};

/**
 * @brief 建立 TCP 连接时使用的配置。
 */
struct ConnectOptions {
    bool no_delay{true}; /**< 成功连接后是否启用 `TCP_NODELAY`。 */
};

/**
 * @brief 单次接收操作的配置。
 */
struct RecvOptions {
    bool fixed_buffer{false}; /**< 是否把带固定索引的缓冲区作为 registered buffer 使用。 */
    SubmitMode submit_mode{SubmitMode::Immediate}; /**< 操作的提交方式。 */
    int flags{0}; /**< 原样传给 Linux recv 的标志。 */
};

/**
 * @brief 单次发送操作的配置。
 */
struct SendOptions {
    bool zero_copy{false}; /**< 是否显式使用 io_uring zero-copy send。 */
    SubmitMode submit_mode{SubmitMode::Immediate}; /**< 操作的提交方式。 */
    int flags{0}; /**< 原样传给 Linux send 的标志。 */
};

/**
 * @brief 配置连接专属的异步发送流水线。
 * @details 仅在 `TcpStream::send_pipeline` 被调用时分配槽位与发送缓冲区。
 */
struct SendPipelineOptions {
    std::size_t depth{8}; /**< 同时允许在途的发送数量，也是默认缓冲块数量。 */
    std::size_t buffer_size{16 * 1024}; /**< 每个发送缓冲块的容量，单次发送不得超过该值。 */
    bool zero_copy{true}; /**< 是否使用 `IORING_OP_SEND_ZC` 并等待零拷贝通知。 */
};

/**
 * @brief 监听套接字的配置。
 */
struct ListenOptions {
    int backlog{128}; /**< 内核监听队列长度。 */
    bool reuse_port{false}; /**< 是否启用 `SO_REUSEPORT` 以创建每 Context Listener。 */
};

/**
 * @brief 单次接受连接的配置。
 */
struct AcceptOptions {
    bool no_delay{true}; /**< 是否在新连接上启用 `TCP_NODELAY`。 */
};

/**
 * @brief multishot 接收流的配置。
 */
struct RecvMultiOptions {
    std::size_t queue_capacity{128}; /**< 用户态有界队列容量，必须是 2 的幂。 */
};

/**
 * @brief multishot 接受流的配置。
 */
struct AcceptMultiOptions {
    std::size_t queue_capacity{128}; /**< 用户态有界队列容量，必须是 2 的幂。 */
    bool no_delay{true}; /**< 是否在取出新连接时启用 `TCP_NODELAY`。 */
};

/**
 * @brief TCP 关闭方向。
 */
enum class Shutdown {
    Read,  /**< 停止接收。 */
    Write, /**< 停止发送。 */
    Both   /**< 同时停止接收和发送。 */
};
