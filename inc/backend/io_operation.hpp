/* io_operation.hpp, ShardIO 异步 I/O 操作基类与具体 Op 声明 */

#pragma once

#include "memory/buffer_handle.hpp"

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <sys/socket.h>
#include <liburing.h>
#include <unistd.h>

class BufferRing;

/**
 * @brief Multishot 每笔 CQE 的同步交付口（Recv 的 Handle、Accept 的 fd）。
 * @details 在 I/O 线程 `complete()` 里直接调用，须短、无阻塞、不 `wait()`。
 *          `on_end`：正常停为 0，错误为负 errno；不要把最后一个成功 fd/字节数当错误码。
 *          库内默认实现见 `core/queue_sink.hpp` 的 `QueueSink`；测试也可自己继承本接口。
 *          `RecvMultiOp` / `AcceptMultiOp` 在 `complete` 里调用本接口。
 */
template <typename T>
class MultiShotSink {
public:
    virtual ~MultiShotSink() = default;

    /**
     * @brief 一笔成功载荷。须 `move` 走或 `release`，不要只 `get()` 拷贝整数。
     */
    virtual void on_shot(T value) noexcept = 0;

    /**
     * @brief 这颗 Op 不再产生 CQE（`MORE=0` 或错误）。
     * @param result 0 为正常结束，负数为 `-errno`。
     */
    virtual void on_end(int result) noexcept = 0;
};

/**
 * @brief 一次 `accept` 得到的连接 fd；析构时若仍持有则 `close`。
 * @details 交给 `on_shot` 后须移入成员或 `release()`，否则函数返回就会关掉 fd。
 */
class AcceptedFd {
public:
    /**
     * @brief 接管一个 fd；负数表示空。
     */
    explicit AcceptedFd(int fd) noexcept
        : fd_(fd)
    {
    }

    /**
     * @brief 仍持有有效 fd 则关闭。
     */
    ~AcceptedFd()
    {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    AcceptedFd(AcceptedFd&& other) noexcept
        : fd_(other.fd_)
    {
        other.fd_ = -1;
    }

    AcceptedFd& operator=(AcceptedFd&& other) noexcept
    {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    AcceptedFd(const AcceptedFd&) = delete;
    AcceptedFd& operator=(const AcceptedFd&) = delete;

    /**
     * @brief 当前 fd；空为 -1。不会交出所有权。
     */
    int get() const noexcept
    {
        return fd_;
    }

    /**
     * @brief 交出 fd 并不再关闭；调用方负责 `close`。
     */
    int release() noexcept
    {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_{-1};  /**< 已接受的连接；-1 表示已 release 或空。 */
};


/**
 * @brief 所有异步 I/O 操作的统一基类。
 * @details 只描述「做什么」并在完成时 resume 等待方。不持有 Backend，不负责向 ring 提交 cancel。
 *          对象地址作为 io_uring user_data，in-flight 期间必须保持稳定，禁止拷贝。
 *
 * 状态机（概念上）：Created → Submitted → InFlight → Completed（含被取消）。
 */
class IoOperation {
public:
    /**
     * @brief 默认构造空操作对象，具体字段由派生类构造函数填充。
     */
    IoOperation() = default;

    /**
     * @brief 虚析构，保证通过基类指针销毁派生 Op 时能释放其自身状态。
     */
    virtual ~IoOperation() = default;

    IoOperation(const IoOperation&) = delete;
    IoOperation& operator=(const IoOperation&) = delete;

    /**
     * @brief 把本操作编译进一枚已取出的 SQE（由 Backend::submit 调用）。
     * @param sqe 空闲 SQE，本函数负责 `io_uring_prep_*`。不在此处 syscall submit。
     */
    virtual void prepare(io_uring_sqe& sqe) = 0;

    /**
     * @brief 处理一次完成事件（对应一笔 CQE）。
     * @param result 原始 `cqe->res`：字节数、新 fd、0 成功/EOF，或负 errno。
     * @param flags 原始 `cqe->flags`：`IORING_CQE_F_MORE` / `IORING_CQE_F_BUFFER` 等。
     * @details 应由 `finish()` 保证逻辑上只完成一次。Oneshot 可忽略 flags；
     *          Multishot 在 `MORE` 置位时不得调用 `finish()`。
     */
    virtual void complete(int result, std::uint32_t flags) noexcept = 0;

    /**
     * @brief 登记完成时要恢复的协程；没有等待方时可不调用。
     * @param waiter 等待本 Op 的 coroutine handle，空 handle 表示仅保存 result。
     */
    void set_waiter(std::coroutine_handle<> waiter) noexcept;

    /**
     * @brief `IoContext::submit` 成功取得 SQE 后标记已进入 ring 路径。
     * @details 供 `IoContext::cancel` 区分「尚未提交则可本地结束」与「需发 CancelOp」。
     */
    void mark_submitted() noexcept;

    /**
     * @brief 是否已成功交给 Backend 编码进 SQE。
     */
    bool is_submitted() const noexcept;

    /**
     * @brief 查询本 Op 是否已经逻辑完成（含取消）。
     */
    bool is_completed() const noexcept;

    /**
     * @brief 返回最近一次 `finish()` 写入的 `cqe->res`。
     */
    int result() const noexcept;

    /**
     * @brief 是否为 `CancelOp`（IoContext 用来跳过对业务 CQE 扫描 inflight 列表）。
     */
    bool is_cancel_request() const noexcept;

protected:
    /**
     * @brief 只完成一次：写入结果并 resume waiter（若有）。
     * @param result 原始 `cqe->res` 或尚未进内核时使用的 `-ECANCELED`。
     * @details 正常完成与 `-ECANCELED` 两笔 CQE 竞态时，第二次调用直接返回。
     */
    void finish(int result) noexcept;

    bool submitted_{false};  /**< 是否已通过 IoContext/Backend 取得 SQE。 */

    bool completed_{false};  /**< 是否已逻辑完成，防止 complete 竞态执行两次。 */

    int result_{0};  /**< 最近一次完成结果，供 awaiter 在 resume 后读取。 */

    std::coroutine_handle<> waiter_{};  /**< 等待本 Op 的协程；空表示尚无等待方。 */

    bool cancel_request_{false};  /**< `CancelOp` 置位；默认业务 Op 为 false。 */

    /**
     * @brief 唤醒 waiter 但不结束 Op（multishot 每笔 CQE 交付一次）。
     * @details 与 `finish` 互斥使用：`MORE` 时只 `notify`；最后一笔或错误才 `finish`。
     */
    void notify() noexcept;
};

/**
 * @brief 请求内核尝试取消一个仍 in-flight 的 `IoOperation`。
 * @details 对应 `io_uring_prep_cancel`，匹配键为目标 Op 地址（与 SQE user_data 相同）。
 *          由 `IoContext::cancel` 持有本对象直至其自身 CQE；不要嵌进 RecvOp 等业务 Op。
 *          目标 Op 会另有一笔 `complete(-ECANCELED, 0)`（或已经成功完成）。
 */
class CancelOp : public IoOperation {
public:
    /**
     * @brief 构造一次取消请求。
     * @param target 仍在内核中排队或执行的目标操作。
     * @param flags 传给 cancel SQE 的标志，默认 0。
     */
    explicit CancelOp(IoOperation& target, int flags = 0);

    /**
     * @brief 使用 `io_uring_prep_cancel` 填写 SQE。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 结束这次 cancel 请求本身，不要对 target 再调 complete。
     * @param flags 本笔 CQE 标志，cancel 请求可忽略。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    IoOperation* target_{nullptr};  /**< 要取消的 in-flight 操作。 */
    int cancel_flags_{0};           /**< 内核 cancel 标志位。 */
};

/**
 * @brief 在 socket 上异步接收数据。
 * @details 未注册走 `io_uring_prep_recv`；`buf_index` 有效则走 `prep_read_fixed`。
 *          完成时 `result > 0` 为收到的字节数，`result == 0` 为对端 EOF，`result < 0` 为 `-errno`。
 */
class RecvOp : public IoOperation {
public:
    /**
     * @brief 构造一次 recv 操作。
     * @param fd 已连接（或已绑定）的套接字文件描述符。
     * @param buffer 接收缓冲区视图，内核将数据写入 `buffer.data`，长度不超过 `buffer.size`。
     * @param buf_index 已 `register_buffers` 时传 `Handle::id()`（fixed_index == BufferId），否则默认哨兵。
     * @param flags 传给 `recv` 的标志，默认 0。
     */
    RecvOp(int fd, MutableBuffer buffer,
           std::uint32_t buf_index = kUnregisteredBuffer, int flags = 0);

    /**
     * @brief 按是否注册选择 `prep_recv` 或 `prep_read_fixed`。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 根据 recv 的 `cqe->res` 完成操作并唤醒等待方。
     * @param flags oneshot recv 忽略；不查看 MORE。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int fd_{-1};           /**< 接收所用的 socket fd。 */
    MutableBuffer buffer_{};  /**< 存放收到数据的非拥有缓冲区。 */
    std::uint32_t buf_index_{kUnregisteredBuffer};  /**< registered `buf_index`，哨兵表示未注册。 */
    int flags_{0};         /**< `prep_recv` 的 flags；fixed 路径仍走 read_fixed。 */
};

/**
 * @brief 使用 provided buffer 的 multishot recv，对应 `io_uring_prep_recv_multishot`。
 * @details 提交前不要 `try_acquire`。每笔带 `F_BUFFER` 的 CQE 同步 `on_shot(Buffer)`。
 *          `on_shot` 返回后 Handle 析构入 Ring reclaim（须在 I/O 线程）。Sink 须比本 Op 活得长。
 */
class RecvMultiOp : public IoOperation {
public:
    /**
     * @brief 构造一次长期 recv。
     * @param fd 已连接套接字。
     * @param ring 提供 `group_id` 与 `checkout`；须在本 Op 寿命内有效。
     * @param sink 每笔数据与结束通知；不得在回调里 `wait()`。
     */
    RecvMultiOp(int fd, BufferRing& ring, MultiShotSink<Buffer>& sink);

    /**
     * @brief `prep_recv_multishot` + `IOSQE_BUFFER_SELECT`，`buf_group` 取自 ring。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 有 buffer 则 `on_shot`；`MORE=0`、EOF(`res==0`) 或错误则 `on_end` + `finish`。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int fd_{-1};              /**< 接收所用的 socket fd。 */
    BufferRing* ring_{nullptr};  /**< 非拥有，指向 provided-buffer 组。 */
    MultiShotSink<Buffer>* sink_{nullptr};  /**< 同步交付，非拥有。 */
};

/**
 * @brief 在 socket 上异步发送数据。
 * @details 未注册走 `prep_send`；已注册走 `prep_write_fixed`。`offset_` 用于 Partial Write。
 *          是否续传由 TcpStream 在 resume 之后决定。
 */
class SendOp : public IoOperation {
public:
    /**
     * @brief 构造一次 send 操作。
     * @param fd 已连接的套接字文件描述符。
     * @param buffer 待发送数据的缓冲区视图。
     * @param buf_index 已注册时传 `Handle::id()`，否则默认哨兵。
     * @param flags 传给 `send` 的标志，默认 0。
     */
    SendOp(int fd, MutableBuffer buffer,
           std::uint32_t buf_index = kUnregisteredBuffer, int flags = 0);

    /**
     * @brief 按是否注册选择 `prep_send` 或 `prep_write_fixed`，发送剩余区间。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 根据已写入字节数推进 `offset_`，然后结束本轮等待。
     * @param flags oneshot send 忽略。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int fd_{-1};              /**< 发送所用的 socket fd。 */
    MutableBuffer buffer_{};     /**< 待发送的完整缓冲区（非拥有）。 */
    std::size_t offset_{0};   /**< 已成功写出的字节偏移，用于短写后续传。 */
    std::uint32_t buf_index_{kUnregisteredBuffer};  /**< registered `buf_index`，哨兵表示未注册。 */
    int flags_{0};            /**< `prep_send` 的 flags。 */
};

/**
 * @brief 零拷贝发送；是否使用由上层决定，不由 IoContext 选择。
 * @details `prep_send_zc`。拥有 `BufferHandle` 时等到 `IORING_CQE_F_NOTIF` 才归还。
 *          视图构造不拥有内存，调用方须保证地址活到 NOTIF。
 */
class SendZcOp : public IoOperation {
public:
    /**
     * @brief 持有 Buffer 直到 NOTIF。
     * @param fd 已连接套接字。
     * @param buffer oneshot 借出；`finish` 后随本对象析构还 Pool。
     * @param buf_index 已 register 时传 `buffer.id()`，否则普通 zc。
     */
    SendZcOp(int fd, BufferHandle buffer,
             std::uint32_t buf_index = kUnregisteredBuffer, int flags = 0);

    /**
     * @brief 非拥有视图；须在 NOTIF 前保持有效（如栈缓冲）。
     */
    SendZcOp(int fd, MutableBuffer buffer,
             std::uint32_t buf_index = kUnregisteredBuffer, int flags = 0);

    /**
     * @brief `prep_send_zc`；有效 `buf_index` 时加 FIXED_BUF。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 发送结果若带 `MORE` 则等 NOTIF；NOTIF 或无后续 CQE 时 `finish`。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int fd_{-1};              /**< 发送所用的 socket fd。 */
    BufferHandle owned_{};    /**< 可选所有权；空表示仅用 `buffer_` 视图。 */
    MutableBuffer buffer_{};     /**< SQE 用的地址+长度；NOTIF 前须有效。 */
    std::uint32_t buf_index_{kUnregisteredBuffer};  /**< registered 下标。 */
    int flags_{0};            /**< `prep_send_zc` 的 send flags（如 MSG_NOSIGNAL）。 */
    int send_result_{0};      /**< 第一笔发送 CQE 的 `res`，NOTIF 时交给 `finish`。 */
    bool have_send_result_{false};  /**< 是否已见过非 NOTIF 的发送完成。 */
};

/**
 * @brief 在监听套接字上异步 accept 新连接。
 * @details 对应 `io_uring_prep_accept`。完成时 `result >= 0` 为新 connection fd，
 *          `result < 0` 为错误。新 fd 所有权交给上层。
 */
class AcceptOp : public IoOperation {
public:
    /**
     * @brief 构造一次 accept 操作。
     * @param listen_fd 已 listen 的套接字文件描述符。
     */
    explicit AcceptOp(int listen_fd);

    /**
     * @brief 使用 `io_uring_prep_accept` 填写 SQE（地址参数第一版可为空）。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 把新连接 fd 或错误码交给等待 accept 的上层。
     * @param flags oneshot accept 忽略。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int listen_fd_{-1};  /**< 监听 socket 的 fd。 */
};

/**
 * @brief Multishot accept：一颗 SQE，每笔 CQE 一个新连接。
 * @details `on_shot(AcceptedFd)` 同步交付；须 `move` 或 `release()`。Sink 须比本 Op 活得长。
 */
class AcceptMultiOp : public IoOperation {
public:
    /**
     * @brief 构造长期 accept。
     * @param listen_fd 已 listen 的套接字。
     * @param sink 新连接与结束通知；不得在回调里 `wait()`。
     */
    AcceptMultiOp(int listen_fd, MultiShotSink<AcceptedFd>& sink);

    /**
     * @brief `io_uring_prep_multishot_accept`，带 `SOCK_CLOEXEC`。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief `res>=0` 则 `on_shot`；无 `MORE` 或错误则 `on_end` + `finish`。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int listen_fd_{-1};  /**< 监听 socket 的 fd。 */
    MultiShotSink<AcceptedFd>* sink_{nullptr};  /**< 同步交付，非拥有。 */
};

/**
 * @brief 异步发起 TCP connect。
 * @details 完成时 `result == 0` 表示连接成功，`result < 0` 为 `-errno`。
 */
class ConnectOp : public IoOperation {
public:
    /**
     * @brief 构造一次 connect 操作。
     * @param fd 尚未连接的套接字文件描述符。
     * @param address 对端地址（含族、端口等），内部拷贝到 `sockaddr_storage`。
     */
    ConnectOp(int fd, const sockaddr_storage& address);

    /**
     * @brief 使用 `io_uring_prep_connect` 填写 SQE。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 根据 connect 结果完成操作。
     * @param flags oneshot connect 忽略。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    int fd_{-1};                     /**< 发起连接所用的 socket fd。 */
    sockaddr_storage address_{};     /**< 对端 sockaddr 存储，与本 Op 同寿命。 */
};

/**
 * @brief Deadline / Timer 的底层实现，对应 io_uring timeout。
 * @details 业务只提供纳秒；`ts_` 仅在 prepare 时填充，不暴露给 TcpStream / Deadline。
 *          到期后由 IoContext 再去 cancel 其它 in-flight Op，本类只结束 timer。
 */
class TimeoutOp : public IoOperation {
public:
    /**
     * @brief 构造一次相对超时。
     * @param timeout_ns 相对当前时刻的超时时间，单位纳秒。
     * @param flags 传给 `io_uring_prep_timeout` 的标志（如绝对时间等），默认 0。
     */
    explicit TimeoutOp(std::uint64_t timeout_ns, unsigned flags = 0);

    /**
     * @brief 把 `timeout_ns_` 写入 `ts_` 并 `io_uring_prep_timeout`。
     */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 超时触发或被取消时完成该 timer。
     * @param flags timeout CQE 标志，本类忽略。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    std::uint64_t timeout_ns_{0};  /**< 相对超时时长（纳秒）。 */
    unsigned timeout_flags_{0};    /**< io_uring timeout 相关 flags。 */
    __kernel_timespec ts_{};       /**< 内核 timespec，与本 Op / 其 CQE 同寿命。 */
};
