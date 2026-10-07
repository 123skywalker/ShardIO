/* context.cpp, Context：转调绑定的 IoContext */

#include "core/context.hpp"
#include "core/io_context.hpp"
#include "backend/io_operation.hpp"
#include "net/batch.hpp"

#include <utility>


Context::Context([[maybe_unused]] IoContext* context) noexcept:context_(context)
{
}

void Context::submit(IoOperation& op) const
{
    if (context_) {
        context_->submit(op);
    }
}

void Context::post([[maybe_unused]] TaskFn task) const
{
    if (!context_) {
        return;
    }
    context_->post(std::move(task));
}

void Context::dispatch([[maybe_unused]] TaskFn task) const
{
    if (!context_) {
        return;
    }
    context_->dispatch(std::move(task));
}

void Context::spawn(Task<void> task) const
{
    if (context_) {
        context_->spawn(std::move(task));
    }
}

YieldAwaiter Context::yield() const noexcept
{
    return context_->yield();
}

void Context::cancel(IoOperation& op) const
{
    if (context_) {
        context_->cancel(op);
    }
}

void Context::submit_cpu(TaskFn task) const
{
    context_->cpu_executor().submit(std::move(task));
}

BufferRing& Context::buffer_ring() const noexcept
{
    return context_->buffer_ring();
}

Batch Context::batch() const
{
    return Batch{*this};
}

bool Context::in_this_context() const noexcept
{
    if (!context_) {
        return false;
    }
    return context_->in_this_context();
}
