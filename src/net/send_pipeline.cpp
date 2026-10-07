/* send_pipeline.cpp, 连接专属发送槽、缓冲池与安全退休状态机 */

#include "net/send_pipeline.hpp"

#include "backend/io_operation.hpp"
#include "core/context.hpp"
#include "memory/buffer_handle.hpp"
#include "memory/buffer_pool.hpp"
#include "net/error.hpp"
#include "net/tcp_stream.hpp"

#include <algorithm>
#include <cerrno>
#include <coroutine>
#include <cstring>
#include <liburing.h>
#include <memory>
#include <new>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace shardio_detail {

class SendPipelineState;

/**
 * @brief 一枚流水线发送操作，持有缓冲区直至普通 CQE 或 ZC NOTIF 最终退休。
 */
class PipelineSendOp final : public IoOperation {
public:
    /**
     * @brief 构造一笔普通或零拷贝发送。
     * @param owner 所属流水线状态。
     * @param fd 连接 fd。
     * @param buffer 已填充、所有权转入操作的稳定缓冲区。
     * @param zero_copy 是否使用 send_zc。
     */
    PipelineSendOp(SendPipelineState& owner, int fd, BufferHandle buffer,
                   bool zero_copy) noexcept;

    /** @brief 根据配置编码普通 send 或 send_zc SQE。 */
    void prepare(io_uring_sqe& sqe) override;

    /**
     * @brief 记录发送结果，并在内核不再引用缓冲区时通知槽位退休。
     * @param result CQE 原始结果。
     * @param flags CQE 标志，零拷贝用其识别 NOTIF/MORE。
     */
    void complete(int result, std::uint32_t flags) noexcept override;

private:
    SendPipelineState* owner_{nullptr}; /**< 完成时回报的流水线状态。 */
    int fd_{-1}; /**< 本次发送使用的连接 fd。 */
    BufferHandle buffer_{}; /**< 保证内核使用期间地址稳定的拥有型缓冲区。 */
    bool zero_copy_{true}; /**< 是否等待零拷贝通知后完成。 */
    int send_result_{0}; /**< 零拷贝首个发送 CQE 的结果。 */
    bool have_send_result_{false}; /**< 是否已经收到零拷贝发送结果。 */
};

/**
 * @brief SendPipeline 的共享实现；只在用户显式创建流水线时分配。
 */
class SendPipelineState final
    : public std::enable_shared_from_this<SendPipelineState> {
    /** @brief 一枚地址稳定、可原地构造 PipelineSendOp 的槽位。 */
    struct Slot {
        alignas(PipelineSendOp) unsigned char storage[sizeof(PipelineSendOp)]; /**< 操作原地存储。 */
        bool live{false}; /**< 当前是否存在已构造且尚未退休的操作。 */
        std::size_t bytes{0}; /**< 本槽在途字节数。 */
    };

public:
    /**
     * @brief 创建默认自管缓冲区的流水线状态。
     * @param fd 连接 fd，不立即取得关闭权。
     * @param context I/O 执行域。
     * @param options 流水线配置。
     */
    SendPipelineState(int fd, Context context, SendPipelineOptions options)
        : fd_(fd)
        , context_(context)
        , options_(options)
        , owned_pool_(std::make_unique<BufferPool>(BufferPool::Config{
              options.buffer_size, options.depth}))
        , pool_(owned_pool_.get())
        , slots_(options.depth)
    {}

    /**
     * @brief 创建借用外部缓冲池的流水线状态。
     * @param fd 连接 fd，不立即取得关闭权。
     * @param context I/O 执行域。
     * @param options 流水线配置。
     * @param pool 外部稳定缓冲池，须比状态活得更久。
     */
    SendPipelineState(int fd, Context context, SendPipelineOptions options,
                      BufferPool& pool)
        : fd_(fd)
        , context_(context)
        , options_(options)
        , pool_(&pool)
        , slots_(options.depth)
    {}

    /** @brief 仅在全部操作退休后由共享所有权释放状态。 */
    ~SendPipelineState() = default;

    /** @brief 禁止复制内部 fd、槽位和等待者状态。 */
    SendPipelineState(const SendPipelineState&) = delete;
    /** @brief 禁止复制赋值内部 fd、槽位和等待者状态。 */
    SendPipelineState& operator=(const SendPipelineState&) = delete;

    /**
     * @brief 是否仍接受发送且连接有效。
     * @return 可提交新发送时为 true。
     */
    bool accepting() const noexcept { return accepting_ && fd_ >= 0; }

    /** @brief 返回配置的槽位数量。 */
    std::size_t depth() const noexcept { return options_.depth; }

    /** @brief 返回配置的单块容量。 */
    std::size_t buffer_size() const noexcept { return options_.buffer_size; }

    /** @brief 返回当前在途发送数量。 */
    std::size_t inflight() const noexcept { return inflight_; }

    /** @brief 返回当前在途字节数。 */
    std::size_t inflight_bytes() const noexcept { return inflight_bytes_; }

    /**
     * @brief 是否存在可立即提交的空槽。
     * @return 在途数小于深度时为 true。
     */
    bool has_capacity() const noexcept { return inflight_ < options_.depth; }

    /**
     * @brief 登记等待空槽的生产协程。
     * @param waiter 等待者句柄。
     */
    void set_capacity_waiter(std::coroutine_handle<> waiter) noexcept
    {
        capacity_waiter_ = waiter;
    }

    /** @brief 清除等待空槽的生产协程。 */
    void clear_capacity_waiter() noexcept { capacity_waiter_ = {}; }

    /**
     * @brief 登记等待全部操作退休的协程。
     * @param waiter 等待者句柄。
     */
    void set_drain_waiter(std::coroutine_handle<> waiter) noexcept
    {
        drain_waiter_ = waiter;
    }

    /** @brief 清除排空等待者。 */
    void clear_drain_waiter() noexcept { drain_waiter_ = {}; }

    /**
     * @brief 复制并提交一笔发送。
     * @param source 用户数据视图。
     * @return 提交结果；失败时不占用槽位。
     */
    IoResult<void> enqueue(ConstBuffer source)
    {
        auto buffer = pool_->try_acquire();
        if (!buffer) {
            return {net_errno(ENOBUFS)};
        }
        std::memcpy(buffer->view().data, source.data, source.size);
        buffer->resize(source.size);

        Slot* slot = nullptr;
        for (auto& candidate : slots_) {
            if (!candidate.live) {
                slot = &candidate;
                break;
            }
        }

        // depth 与槽位数组同源，has_capacity() 成立时必有空槽。
        auto* op = ::new (slot->storage) PipelineSendOp(
            *this, fd_, std::move(*buffer), options_.zero_copy);
        slot->live = true;
        slot->bytes = source.size;
        ++inflight_;
        inflight_bytes_ += source.size;
        keepalive_ = shared_from_this();
        context_.submit(*op);
        return {};
    }

    /** @brief 停止接收新发送，但让已提交操作自然排空。 */
    void stop_accepting() noexcept { accepting_ = false; }

    /**
     * @brief 取消在途发送；不关闭仍归 TcpStream 所有的 fd。
     */
    void abandon() noexcept { request_stop(false); }

    /**
     * @brief 停止并取消流水线，待所有操作退休后关闭连接 fd。
     */
    void close_connection() noexcept { request_stop(true); }

    /**
     * @brief 返回并保留流水线观察到的首个错误。
     * @return 没有错误时为空错误码。
     */
    std::error_code error() const noexcept { return first_error_; }

    /**
     * @brief 由 PipelineSendOp 在最终 CQE 上请求延迟退休槽位。
     */
    void operation_completed() noexcept
    {
        if (reap_posted_) {
            return;
        }
        reap_posted_ = true;
        auto self = shared_from_this();
        // 不能在 complete 栈上析构作为 user_data 的操作，延迟到下一轮任务。
        context_.post([self = std::move(self)] { self->reap(); });
    }

private:
    friend class PipelineSendOp;

    /**
     * @brief 在 Context owner 线程停止、取消，并按需接管 fd 关闭责任。
     * @param close_fd 全部操作退休后是否关闭连接。
     */
    void request_stop(bool close_fd) noexcept
    {
        accepting_ = false;
        close_fd_ = close_fd_ || close_fd;
        if (inflight_ == 0) {
            // 无内核引用时可立即完成 RAII 清理，不依赖 Context 仍在运行。
            finish_if_idle();
            return;
        }
        auto self = shared_from_this();
        context_.dispatch([self = std::move(self)] { self->cancel_owner(); });
    }

    /** @brief 对仍在途的槽位提交取消；无在途时立即完成关闭。 */
    void cancel_owner() noexcept
    {
        for (auto& slot : slots_) {
            if (slot.live) {
                auto* op = std::launder(
                    reinterpret_cast<PipelineSendOp*>(slot.storage));
                if (!op->is_completed()) {
                    context_.cancel(*op);
                }
            }
        }
        finish_if_idle();
    }

    /** @brief 退休已完成槽位，归还 buffer，并恢复容量或排空等待者。 */
    void reap() noexcept
    {
        reap_posted_ = false;
        for (auto& slot : slots_) {
            if (!slot.live) {
                continue;
            }
            auto* op = std::launder(
                reinterpret_cast<PipelineSendOp*>(slot.storage));
            if (!op->is_completed()) {
                continue;
            }
            const int result = op->result();
            if (result < 0 && !first_error_) {
                first_error_ = net_uring(result);
            } else if (result >= 0 && static_cast<std::size_t>(result) != slot.bytes &&
                       !first_error_) {
                first_error_ = net_errno(EIO);
            }
            inflight_bytes_ -= slot.bytes;
            op->~PipelineSendOp();
            slot.live = false;
            slot.bytes = 0;
            --inflight_;
        }

        auto capacity = capacity_waiter_;
        if (capacity && has_capacity()) {
            capacity_waiter_ = {};
        } else {
            capacity = {};
        }
        auto drained = drain_waiter_;
        if (drained && inflight_ == 0) {
            drain_waiter_ = {};
        } else {
            drained = {};
        }

        finish_if_idle();
        if (capacity) {
            capacity.resume();
        }
        if (drained) {
            drained.resume();
        }
    }

    /** @brief 在最后一笔操作退休后关闭 fd、释放自管池和自保持引用。 */
    void finish_if_idle() noexcept
    {
        if (inflight_ != 0) {
            return;
        }
        if (!accepting_) {
            owned_pool_.reset();
            pool_ = nullptr;
        }
        if (close_fd_ && fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        keepalive_.reset();
    }

    int fd_{-1}; /**< 绑定的连接 fd；连接关闭时才由本状态最终释放。 */
    Context context_{}; /**< 只用于提交、取消与恢复等待者的通用执行域。 */
    SendPipelineOptions options_{}; /**< 构造时固定的深度、块大小与发送模式。 */
    std::unique_ptr<BufferPool> owned_pool_{}; /**< 默认入口按需创建的发送专用池。 */
    BufferPool* pool_{nullptr}; /**< 指向自管池或高级入口提供的外部池。 */
    std::vector<Slot> slots_{}; /**< 一次性连续分配且地址稳定的发送槽位。 */
    std::size_t inflight_{0}; /**< 尚未退休的槽位数量。 */
    std::size_t inflight_bytes_{0}; /**< 尚未退休的数据字节总数。 */
    std::coroutine_handle<> capacity_waiter_{}; /**< 等待空槽的单生产者协程。 */
    std::coroutine_handle<> drain_waiter_{}; /**< 等待全部操作退休的协程。 */
    std::error_code first_error_{}; /**< 首次发送或取消错误，由 drain/close 返回。 */
    std::shared_ptr<SendPipelineState> keepalive_{}; /**< CQE 未退休时阻止状态提前析构。 */
    bool accepting_{true}; /**< 是否允许提交新发送。 */
    bool close_fd_{false}; /**< 最后一笔退休后是否负责关闭连接。 */
    bool reap_posted_{false}; /**< 是否已有延迟退休任务，避免重复 post。 */
};

PipelineSendOp::PipelineSendOp(SendPipelineState& owner, int fd,
                               BufferHandle buffer, bool zero_copy) noexcept
    : owner_(&owner)
    , fd_(fd)
    , buffer_(std::move(buffer))
    , zero_copy_(zero_copy)
{
}

void PipelineSendOp::prepare(io_uring_sqe& sqe)
{
    const MutableBuffer view = buffer_.view();
    const std::uint32_t index =
        buffer_.registered() ? buffer_.id() : kUnregisteredBuffer;
    if (zero_copy_) {
        if (index == kUnregisteredBuffer) {
            io_uring_prep_send_zc(
                &sqe, fd_, view.data, view.size, MSG_NOSIGNAL, 0);
        } else {
            io_uring_prep_send_zc_fixed(
                &sqe, fd_, view.data, view.size, MSG_NOSIGNAL, 0, index);
        }
        return;
    }
    if (index == kUnregisteredBuffer) {
        io_uring_prep_send(&sqe, fd_, view.data, view.size, MSG_NOSIGNAL);
    } else {
        io_uring_prep_write_fixed(&sqe, fd_, view.data, view.size, 0,
                                  static_cast<int>(index));
    }
}

void PipelineSendOp::complete(int result, std::uint32_t flags) noexcept
{
    if (!zero_copy_) {
        finish(result);
        owner_->operation_completed();
        return;
    }
    if ((flags & IORING_CQE_F_NOTIF) != 0) {
        finish(have_send_result_ ? send_result_ : result);
        owner_->operation_completed();
        return;
    }
    have_send_result_ = true;
    send_result_ = result;
    if ((flags & IORING_CQE_F_MORE) != 0 && result >= 0) {
        return;
    }
    finish(result);
    owner_->operation_completed();
}

} // namespace shardio_detail

namespace {

struct CapacityAwaiter {
    std::shared_ptr<shardio_detail::SendPipelineState> state; /**< 保证挂起期间状态存活。 */

    bool await_ready() const noexcept
    {
        return !state->accepting() || state->has_capacity();
    }

    bool await_suspend(std::coroutine_handle<> waiter) noexcept
    {
        state->set_capacity_waiter(waiter);
        if (!state->accepting() || state->has_capacity()) {
            state->clear_capacity_waiter();
            return false;
        }
        return true;
    }

    void await_resume() const noexcept {}
};

struct DrainAwaiter {
    std::shared_ptr<shardio_detail::SendPipelineState> state; /**< 保证挂起期间状态存活。 */

    bool await_ready() const noexcept { return state->inflight() == 0; }

    bool await_suspend(std::coroutine_handle<> waiter) noexcept
    {
        state->set_drain_waiter(waiter);
        if (state->inflight() == 0) {
            state->clear_drain_waiter();
            return false;
        }
        return true;
    }

    void await_resume() const noexcept {}
};

} // namespace

SendPipeline::SendPipeline(
    std::shared_ptr<shardio_detail::SendPipelineState> state) noexcept
    : state_(std::move(state))
{
}

SendPipeline::~SendPipeline()
{
    if (state_) {
        state_->abandon();
    }
}

SendPipeline::SendPipeline(SendPipeline&& other) noexcept = default;

SendPipeline& SendPipeline::operator=(SendPipeline&& other) noexcept
{
    if (this != &other) {
        if (state_) {
            state_->abandon();
        }
        state_ = std::move(other.state_);
    }
    return *this;
}

Task<IoResult<void>> SendPipeline::send(ConstBuffer buffer)
{
    auto state = state_;
    if (!state || !state->accepting()) {
        co_return IoResult<void>{net_errno(ECANCELED)};
    }
    if (buffer.size > state->buffer_size()) {
        co_return IoResult<void>{net_errno(EMSGSIZE)};
    }
    co_await CapacityAwaiter{state};
    if (!state->accepting()) {
        co_return IoResult<void>{net_errno(ECANCELED)};
    }
    co_return state->enqueue(buffer);
}

Task<IoResult<void>> SendPipeline::drain()
{
    auto state = state_;
    if (!state) {
        co_return IoResult<void>{net_errno(EBADF)};
    }
    co_await DrainAwaiter{state};
    co_return IoResult<void>{state->error()};
}

Task<IoResult<void>> SendPipeline::close()
{
    auto state = state_;
    if (!state) {
        co_return IoResult<void>{};
    }
    state->stop_accepting();
    co_await DrainAwaiter{state};
    co_return IoResult<void>{state->error()};
}

bool SendPipeline::valid() const noexcept
{
    return state_ && state_->accepting();
}

std::size_t SendPipeline::depth() const noexcept
{
    return state_ ? state_->depth() : 0;
}

std::size_t SendPipeline::buffer_size() const noexcept
{
    return state_ ? state_->buffer_size() : 0;
}

std::size_t SendPipeline::inflight() const noexcept
{
    return state_ ? state_->inflight() : 0;
}

std::size_t SendPipeline::inflight_bytes() const noexcept
{
    return state_ ? state_->inflight_bytes() : 0;
}

void SendPipeline::close_connection(
    const std::shared_ptr<shardio_detail::SendPipelineState>& state) noexcept
{
    if (state) {
        state->close_connection();
    }
}

SendPipeline TcpStream::send_pipeline(SendPipelineOptions options)
{
    auto state = std::make_shared<shardio_detail::SendPipelineState>(
        fd_, context_, options);
    send_pipeline_ = state;
    return SendPipeline{std::move(state)};
}

SendPipeline TcpStream::send_pipeline(SendPipelineOptions options,
                                      BufferPool& buffer_pool)
{
    auto state = std::make_shared<shardio_detail::SendPipelineState>(
        fd_, context_, options, buffer_pool);
    send_pipeline_ = state;
    return SendPipeline{std::move(state)};
}
