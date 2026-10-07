/* batch.hpp, 在一次 EventLoop flush 前准备多笔 TCP 操作 */

#pragma once

#include "core/context.hpp"
#include "core/io_result.hpp"
#include "core/task.hpp"
#include "memory/buffer.hpp"
#include "net/options.hpp"

#include <memory>
#include <vector>

class TcpStream;

/**
 * @brief 显式批量提交多笔 TCP recv/send 的高级对象。
 * @details 每个操作仍使用原有 oneshot Op；批次仅把多个懒启动 Task 在同一调度轮次启动，
 *          由 `IoContext` 随后一次 flush 提交，不暴露 SQE 或底层 Operation。
 */
class Batch {
public:
    /**
     * @brief 创建绑定到一个执行域的空批次。
     * @param context 所有流必须归属的执行域。
     */
    explicit Batch(Context context) noexcept;

    /**
     * @brief 向批次追加一笔允许短写的发送。
     * @param stream 目标 TCP 连接，必须属于本批次的 Context。
     * @param buffer 提交完成前保持有效的只读缓冲区。
     * @param options zero-copy 与原生 flags 配置。
     */
    void send(TcpStream& stream, ConstBuffer buffer, SendOptions options = {});

    /**
     * @brief 向批次追加一笔允许短读的接收。
     * @param stream 来源 TCP 连接，必须属于本批次的 Context。
     * @param buffer 提交完成前保持有效的可写缓冲区。
     * @param options fixed-buffer 与原生 flags 配置。
     */
    void recv(TcpStream& stream, MutableBuffer buffer, RecvOptions options = {});

    /**
     * @brief 在同一调度轮次启动全部操作并等待全部完成。
     * @return 全部成功时为空成功结果；失败时返回首个 I/O 错误。
     */
    Task<IoResult<void>> submit();

    /**
     * @brief 取得尚未提交的操作数量。
     * @return 当前批次中的操作数。
     */
    std::size_t size() const noexcept;

private:
    struct State;

    /**
     * @brief 批次内一个可延迟启动的操作。
     */
    struct Entry {
        TcpStream* stream{nullptr}; /**< 目标连接，不拥有。 */
        ConstBuffer send_buffer{}; /**< 发送操作的只读视图。 */
        MutableBuffer recv_buffer{}; /**< 接收操作的可写视图。 */
        SendOptions send_options{}; /**< 发送配置。 */
        RecvOptions recv_options{}; /**< 接收配置。 */
        bool is_send{false}; /**< true 表示发送，false 表示接收。 */
    };

    /**
     * @brief 执行一笔条目并更新批次完成状态。
     * @param entry 要执行的条目副本。
     * @param state 本次 submit 的共享完成状态。
     */
    static Task<void> run(Entry entry, std::shared_ptr<State> state);

    Context context_{}; /**< 批次所属执行域。 */
    std::vector<Entry> entries_{}; /**< 提交前积累的操作描述。 */
};
