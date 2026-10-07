/* multi_stream.cpp, multishot CQE 到有界协程流的直接桥接 */

#include "net/multi_stream.hpp"

#include "backend/io_operation.hpp"
#include "core/context.hpp"
#include "memory/buffer_ring.hpp"
#include "net/error.hpp"
#include "net/tcp_stream.hpp"

#include <coroutine>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <utility>
#include <vector>

template <typename T>
class MultiQueue {
public:
    explicit MultiQueue(std::size_t capacity)
        : slots_(capacity), mask_(capacity - 1) {}

    void push(T value) noexcept
    {
        if (ended_ || count_ == slots_.size()) return;
        slots_[(head_ + count_) & mask_] = std::move(value);
        ++count_;
        wake();
    }

    void end(int result) noexcept
    {
        ended_ = true;
        result_ = result;
        wake();
    }

    bool ready() const noexcept { return count_ != 0 || ended_; }

    bool suspend(std::coroutine_handle<> waiter) noexcept
    {
        waiter_ = waiter;
        if (ready()) {
            waiter_ = {};
            return false;
        }
        return true;
    }

    std::optional<T> pop() noexcept
    {
        if (count_ == 0) return std::nullopt;
        std::optional<T> value = std::move(slots_[head_]);
        slots_[head_].reset();
        head_ = (head_ + 1) & mask_;
        --count_;
        return value;
    }

    int result() const noexcept { return result_; }

private:
    void wake() noexcept
    {
        if (!waiter_) return;
        auto waiter = waiter_;
        waiter_ = {};
        // CQE 完成栈直接恢复 next()，保持 multishot 热路径不进入 Scheduler。
        waiter.resume();
    }

    std::vector<std::optional<T>> slots_;
    std::size_t mask_{0};
    std::size_t head_{0};
    std::size_t count_{0};
    int result_{0};
    bool ended_{false};
    std::coroutine_handle<> waiter_{};
};

struct RecvStream::State final : MultiShotSink<Buffer>,
                                 std::enable_shared_from_this<State> {
    State(Context owner, int fd, BufferRing& ring, std::size_t capacity)
        : context(owner), queue(capacity), op(fd, ring, *this) {}

    void on_shot(Buffer value) noexcept override { queue.push(std::move(value)); }

    void on_end(int result) noexcept override
    {
        queue.end(result);
        auto state = shared_from_this();
        context.post([state = std::move(state)]() mutable {
            state->keepalive.reset();
        });
    }

    Context context;
    MultiQueue<Buffer> queue;
    RecvMultiOp op;
    std::shared_ptr<State> keepalive;
};

struct AcceptStream::State final : MultiShotSink<AcceptedFd>,
                                   std::enable_shared_from_this<State> {
    State(Context owner, int fd, std::size_t capacity, bool enable_no_delay)
        : context(owner), queue(capacity), op(fd, *this), no_delay(enable_no_delay) {}

    void on_shot(AcceptedFd value) noexcept override { queue.push(std::move(value)); }

    void on_end(int result) noexcept override
    {
        queue.end(result);
        auto state = shared_from_this();
        context.post([state = std::move(state)]() mutable {
            state->keepalive.reset();
        });
    }

    Context context;
    MultiQueue<AcceptedFd> queue;
    AcceptMultiOp op;
    bool no_delay;
    std::shared_ptr<State> keepalive;
};

RecvStream::RecvStream(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

RecvStream::~RecvStream() { cancel(); }
RecvStream::RecvStream(RecvStream&&) noexcept = default;
RecvStream& RecvStream::operator=(RecvStream&&) noexcept = default;

RecvStream RecvStream::start(Context context, int fd, BufferRing& ring,
                             std::size_t capacity)
{
    auto state = std::make_shared<State>(context, fd, ring, capacity);
    state->keepalive = state;
    context.submit(state->op);
    return RecvStream{std::move(state)};
}

Task<StreamItem<Buffer>> RecvStream::next()
{
    struct Awaiter {
        MultiQueue<Buffer>* queue;
        bool await_ready() const noexcept { return queue->ready(); }
        bool await_suspend(std::coroutine_handle<> h) noexcept { return queue->suspend(h); }
        void await_resume() const noexcept {}
    };
    if (!state_) co_return StreamItem<Buffer>{};
    co_await Awaiter{&state_->queue};
    if (auto value = state_->queue.pop()) {
        co_return StreamItem<Buffer>{std::move(*value)};
    }
    const int result = state_->queue.result();
    co_return result < 0 ? StreamItem<Buffer>{net_uring(result)}
                         : StreamItem<Buffer>{};
}

void RecvStream::cancel() noexcept
{
    if (state_ && !state_->op.is_completed()) state_->context.cancel(state_->op);
}

AcceptStream::AcceptStream(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

AcceptStream::~AcceptStream() { cancel(); }
AcceptStream::AcceptStream(AcceptStream&&) noexcept = default;
AcceptStream& AcceptStream::operator=(AcceptStream&&) noexcept = default;

AcceptStream AcceptStream::start(Context context, int fd, std::size_t capacity,
                                 bool no_delay)
{
    auto state = std::make_shared<State>(context, fd, capacity, no_delay);
    state->keepalive = state;
    context.submit(state->op);
    return AcceptStream{std::move(state)};
}

Task<StreamItem<TcpStream>> AcceptStream::next()
{
    struct Awaiter {
        MultiQueue<AcceptedFd>* queue;
        bool await_ready() const noexcept { return queue->ready(); }
        bool await_suspend(std::coroutine_handle<> h) noexcept { return queue->suspend(h); }
        void await_resume() const noexcept {}
    };
    if (!state_) co_return StreamItem<TcpStream>{};
    co_await Awaiter{&state_->queue};
    if (auto accepted = state_->queue.pop()) {
        const int fd = accepted->release();
        if (state_->no_delay) {
            const int yes = 1;
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        }
        co_return StreamItem<TcpStream>{TcpStream{fd, state_->context}};
    }
    const int result = state_->queue.result();
    co_return result < 0 ? StreamItem<TcpStream>{net_uring(result)}
                         : StreamItem<TcpStream>{};
}

void AcceptStream::cancel() noexcept
{
    if (state_ && !state_->op.is_completed()) state_->context.cancel(state_->op);
}
