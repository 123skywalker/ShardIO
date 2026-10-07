/* uring_backend.cpp, UringBackend 与 WakeupOp 实现 */

#include "backend/uring_backend.hpp"
#include "memory/buffer.hpp"
#include "memory/buffer_pool.hpp"
#include "memory/buffer_ring.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <liburing.h>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <system_error>
#include <unistd.h>
#include <vector>

void WakeupOp::bind(int wakeup_fd) noexcept
{
    wakeup_fd_ = wakeup_fd;
}

void WakeupOp::prepare(io_uring_sqe& sqe)
{
    // POLLIN 不是 POLL_IN；poll 只报可读，counter 要自己 drain
    io_uring_prep_poll_multishot(&sqe, wakeup_fd_, POLLIN);
}

void WakeupOp::complete(int, std::uint32_t) noexcept
{
    // 门铃不进 IoContext，complete 不会被调用
}

UringBackend::UringBackend(unsigned queue_depth)
    : queue_depth_(queue_depth)
{
    int ret = io_uring_queue_init(queue_depth, &ring_, 0);
    if (ret < 0) {
        throw std::system_error(
            -ret,
            std::generic_category(),
            "io_uring_queue_init");
    }
    try {
        setup_wakeup();
    } catch (...) {
        // 构造失败不会走析构，ring / fd 必须在这里拆掉
        if (wakeup_fd_ >= 0) {
            ::close(wakeup_fd_);
            wakeup_fd_ = -1;
        }
        io_uring_queue_exit(&ring_);
        throw;
    }
}

UringBackend::~UringBackend()
{
    std::vector<BufRingReg> rings = std::move(buf_rings_);
    buf_rings_.clear();
    for (BufRingReg& item : rings) {
        // 先 unbind：内部 flush 时 br 仍有效，再拆内核对象
        if (item.user_ring != nullptr) {
            item.user_ring->unbind_kernel();
        }
        if (item.br != nullptr) {
            io_uring_free_buf_ring(&ring_, item.br, item.entries, item.group_id);
            item.br = nullptr;
        }
    }
    unregister_buffers();
    // 先拆 ring，避免内核还在 poll 已关闭的 fd
    io_uring_queue_exit(&ring_);
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
}

bool UringBackend::submit(IoOperation& op)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        return false;  // 环满，交给调用方 flush 后再试
    }
    op.prepare(*sqe);
    io_uring_sqe_set_data(sqe, &op);  // CQE user_data 回指本对象
    pending_submissions_++;
    return true;
}

std::size_t UringBackend::flush()
{
    int ret = io_uring_submit(&ring_);
    if (ret < 0) {
        // 失败 ≠ 提交了 0 个，不能 return 0，更不能在这里拆掉 ring
        throw std::system_error(
            -ret,
            std::generic_category(),
            "io_uring_submit");
    }
    pending_submissions_ -= static_cast<std::size_t>(ret);
    return static_cast<std::size_t>(ret);
}

std::size_t UringBackend::wait(std::span<Completion> out)
{
    const std::size_t out_num = out.size();
    if (out_num == 0) {
        return 0;
    }

    // 睡前公布本圈还回的 provided buffer，避免内核饿死
    flush_reclaims();

    // 睡眠前必须有门铃；已 armed 再挂会重复 poll
    if (!wakeup_armed_) {
        arm_wakeup();
    }

    io_uring_cqe* cqe = nullptr;
    for (;;) {
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret == -EINTR) {
            continue;
        }
        if (ret < 0) {
            throw std::system_error(
                -ret,
                std::generic_category(),
                "io_uring_wait_cqe");
        }
        break;
    }

    // 第一笔可能是门铃，decode 返回 false 时 wait 仍可返回 0
    std::size_t num = 0;
    Completion first{};
    if (decode_cqe(cqe, first)) {
        out[0] = first;
        num = 1;
    }

    // wait_cqe 只取出头一笔，其余用 peek 填满 out 或直到 CQ 空
    while (num < out_num) {
        io_uring_cqe* cqes[64];
        unsigned want = static_cast<unsigned>(out_num - num);
        if (want > 64) {
            want = 64;
        }
        unsigned count = io_uring_peek_batch_cqe(&ring_, cqes, want);
        if (count == 0) {
            break;
        }
        for (unsigned i = 0; i < count; ++i) {
            Completion slot{};
            if (decode_cqe(cqes[i], slot)) {
                out[num++] = slot;
            }
        }
    }
    return num;
}

std::size_t UringBackend::poll(std::span<Completion> out)
{
    // 不阻塞、不 ensure 门铃；I/O 线程没在睡觉就不需要 rearm
    if (out.empty()) {
        return 0;
    }
    io_uring_cqe* cqes[64];
    unsigned want = static_cast<unsigned>(out.size());
    if (want > 64) {
        want = 64;
    }
    unsigned count = io_uring_peek_batch_cqe(&ring_, cqes, want);
    std::size_t num = 0;
    for (unsigned i = 0; i < count && num < out.size(); ++i) {
        Completion slot{};
        if (decode_cqe(cqes[i], slot)) {
            out[num++] = slot;
        }
    }
    return num;
}

void UringBackend::flush_reclaims() noexcept
{
    for (BufRingReg& item : buf_rings_) {
        if (item.user_ring != nullptr) {
            item.user_ring->flush_reclaims();
        }
    }
}

void UringBackend::wakeup() noexcept
{
    // 任意线程：只写 eventfd，禁止碰 ring / throw
    if (wakeup_fd_ < 0) {
        return;
    }
    for (;;) {
        int ret = eventfd_write(wakeup_fd_, 1);
        if (ret == 0) {
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        // 已经存在大量未消费 wakeup，视为已有信号
        return;
    }
}

void UringBackend::register_buffers(BufferPool& pool)
{
    if (registered_pool_ != nullptr) {
        throw std::logic_error("UringBackend: buffers already registered");
    }
    const std::size_t n = pool.capacity();
    if (n == 0) {
        pool.mark_registered();
        registered_pool_ = &pool;
        return;
    }
    std::vector<iovec> iov(n);
    for (std::size_t i = 0; i < n; ++i) {
        BufferSlot& s = pool.slot(static_cast<BufferId>(i));
        iov[i].iov_base = s.data;
        iov[i].iov_len = s.capacity;
    }
    int ret = io_uring_register_buffers(
        &ring_, iov.data(), static_cast<unsigned>(n));
    if (ret < 0) {
        throw std::system_error(-ret, std::generic_category(), "io_uring_register_buffers");
    }
    pool.mark_registered();
    registered_pool_ = &pool;
}

void UringBackend::unregister_buffers()
{
    if (registered_pool_ == nullptr) {
        return;
    }
    // 空池未真正 register，只需清标记
    if (registered_pool_->capacity() != 0) {
        (void)io_uring_unregister_buffers(&ring_);
    }
    registered_pool_->mark_unregistered();
    registered_pool_ = nullptr;
}

UringBackend::BufRingReg* UringBackend::find_buf_ring(std::uint16_t group_id) noexcept
{
    for (BufRingReg& item : buf_rings_) {
        if (item.group_id == group_id) {
            return &item;
        }
    }
    return nullptr;
}

void UringBackend::register_buffer_ring(BufferRing& ring)
{
    const unsigned entries = static_cast<unsigned>(ring.capacity());
    // 非 0 且恰一个 bit：内核 buf ring entries 必须是 2 的幂
    if (entries == 0 || (entries & (entries - 1u)) != 0) {
        throw std::logic_error("UringBackend: buf ring capacity must be power of two");
    }
    if (find_buf_ring(ring.group_id()) != nullptr) {
        throw std::logic_error("UringBackend: buf ring group already registered");
    }
    int err = 0;
    io_uring_buf_ring* br = io_uring_setup_buf_ring(
        &ring_, entries, ring.group_id(), 0, &err);
    if (br == nullptr) {
        throw std::system_error(
            err > 0 ? err : -err,
            std::generic_category(),
            "io_uring_setup_buf_ring");
    }
    BufRingReg item;
    item.group_id = ring.group_id();
    item.br = br;
    item.entries = entries;
    item.user_ring = &ring;
    buf_rings_.push_back(item);
    ring.bind_kernel(br, static_cast<int>(entries - 1));
}

void UringBackend::unregister_buffer_ring(BufferRing& ring)
{
    BufRingReg* found = find_buf_ring(ring.group_id());
    if (found == nullptr) {
        ring.unbind_kernel();
        return;
    }
    io_uring_buf_ring* br = found->br;
    const unsigned entries = found->entries;
    const std::uint16_t gid = found->group_id;
    // Ring 自己 flush；拆内核对象必须在其后，避免 unbind 写已释放的 br
    ring.unbind_kernel();
    if (br != nullptr) {
        io_uring_free_buf_ring(&ring_, br, entries, gid);
    }
    // std::remove_if 只负责重排，真正缩小容器要靠 erase
    buf_rings_.erase(std::remove_if(
                         buf_rings_.begin(),
                         buf_rings_.end(),
                         [gid](const BufRingReg& item) { return item.group_id == gid; }),
                     buf_rings_.end());
}

void UringBackend::setup_wakeup()
{
    wakeup_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wakeup_fd_ < 0) {
        throw std::system_error(errno, std::generic_category(), "eventfd");
    }
    wakeup_op_.bind(wakeup_fd_);  // 成员构造时还没有 fd
    arm_wakeup();
}

void UringBackend::arm_wakeup()
{
    // 先交完业务 SQE，门铃单独 submit，才能确认 Kernel 里真有 poll
    flush_all_pending();
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        throw std::system_error(ENOMEM, std::generic_category(), "arm_wakeup");
    }
    wakeup_op_.prepare(*sqe);
    io_uring_sqe_set_data(sqe, &wakeup_op_);
    int ret = io_uring_submit(&ring_);
    if (ret < 0) {
        throw std::system_error(-ret, std::generic_category(), "arm_wakeup");
    }
    // 必须在 submit 成功之后
    wakeup_armed_ = true;  
}
    

void UringBackend::drain_wakeup_fd()
{
    // poll 不会消费 counter；不读空会水平触发连打 CQE
    for (;;) {
        eventfd_t value = 0;
        int ret = eventfd_read(wakeup_fd_, &value);
        if (ret == 0) {
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;  // 已经读空
        }
        throw std::system_error(errno, std::generic_category(), "eventfd_read");
    }
}

void UringBackend::handle_wakeup_cqe(int result, std::uint32_t flags)
{
    if (wakeup_fd_ >= 0) {
        drain_wakeup_fd();
    }
    // MORE=1：multishot 还在，不必提交。失败或无 MORE：只打标，留给下次 wait rearm
    if (result < 0 || (flags & IORING_CQE_F_MORE) == 0) {
        wakeup_armed_ = false;
    }
}

bool UringBackend::decode_cqe(io_uring_cqe* cqe, Completion& out)
{
    auto* operation = static_cast<IoOperation*>(io_uring_cqe_get_data(cqe));
    const int result = cqe->res;
    const std::uint32_t flags = cqe->flags;
    // 先读字段再 seen
    io_uring_cqe_seen(&ring_, cqe);  
    if (operation == &wakeup_op_) {
        handle_wakeup_cqe(result, flags);
        // 不写入 out，IoContext 不会 complete 门铃
        return false;  
    }
    // 只填 Completion；complete 由 IoContext 调用，这里调会做两遍
    out.operation = operation;
    out.result = result;
    out.flags = flags;
    return true;
}

void UringBackend::flush_all_pending()
{
    while (pending_submissions_) {
        // 直至业务 SQE 清零，门铃才能单独一笔 submit
        flush();  
    }
}
