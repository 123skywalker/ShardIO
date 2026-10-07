/* batch.cpp, 借助同一 Scheduler 轮次形成 SQ 批量提交 */

#include "net/batch.hpp"

#include "net/tcp_stream.hpp"

#include <coroutine>
#include <utility>

struct Batch::State {
    std::size_t remaining{0}; /**< 尚未完成的条目数。 */
    std::error_code error{}; /**< 首个操作错误。 */
    std::coroutine_handle<> waiter{}; /**< 等待整个批次的协程。 */
};

Batch::Batch(Context context) noexcept : context_(context) {}

void Batch::send(TcpStream& stream, ConstBuffer buffer, SendOptions options)
{
    options.submit_mode = SubmitMode::Batch;
    entries_.push_back(Entry{
        .stream = &stream,
        .send_buffer = buffer,
        .send_options = options,
        .is_send = true,
    });
}

void Batch::recv(TcpStream& stream, MutableBuffer buffer, RecvOptions options)
{
    options.submit_mode = SubmitMode::Batch;
    entries_.push_back(Entry{
        .stream = &stream,
        .recv_buffer = buffer,
        .recv_options = options,
        .is_send = false,
    });
}

Task<void> Batch::run(Entry entry, std::shared_ptr<State> state)
{
    if (entry.is_send) {
        auto result = co_await entry.stream->send(entry.send_buffer, entry.send_options);
        if (!result && !state->error) state->error = result.error;
    } else {
        auto result = co_await entry.stream->recv(entry.recv_buffer, entry.recv_options);
        if (!result && !state->error) state->error = result.error;
    }
    if (--state->remaining == 0 && state->waiter) {
        // 最后一笔 CQE 直接恢复批次等待者，不额外 post。
        state->waiter.resume();
    }
}

Task<IoResult<void>> Batch::submit()
{
    if (entries_.empty()) co_return IoResult<void>{};
    auto state = std::make_shared<State>();
    state->remaining = entries_.size();
    for (Entry& entry : entries_) {
        context_.spawn(run(entry, state));
    }
    entries_.clear();
    struct Awaiter {
        std::shared_ptr<State> state;
        bool await_ready() const noexcept { return state->remaining == 0; }
        void await_suspend(std::coroutine_handle<> waiter) noexcept {
            state->waiter = waiter;
        }
        void await_resume() const noexcept {}
    };
    co_await Awaiter{state};
    co_return IoResult<void>{state->error};
}

std::size_t Batch::size() const noexcept
{
    return entries_.size();
}
