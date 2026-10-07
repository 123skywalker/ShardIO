/* send_pipeline.hpp, 连接专属的有界异步发送流水线 */

#pragma once

#include "core/io_result.hpp"
#include "core/task.hpp"
#include "memory/buffer.hpp"
#include "net/options.hpp"

#include <cstddef>
#include <memory>

class BufferPool;
class TcpStream;

namespace shardio_detail {
class SendPipelineState;
}

/**
 * @brief 连接专属、运行时定深度的异步发送流水线。
 * @details 默认自行预分配稳定缓冲块；高级入口可借用外部 `BufferPool`。
 *          类型仅可移动。`send` 在复制并提交后返回，实际完成错误由 `drain` 或 `close` 返回。
 *          同一对象应在连接所属 Context 中由一个生产协程使用。
 */
class SendPipeline {
public:
    /** @brief 构造一个不绑定连接的空流水线。 */
    SendPipeline() noexcept = default;

    /** @brief 停止接收新发送，并取消仍在途的操作；资源在 CQE/NOTIF 退休后释放。 */
    ~SendPipeline();

    /**
     * @brief 转移流水线控制权，源对象变为空。
     * @param other 被转移的流水线。
     */
    SendPipeline(SendPipeline&& other) noexcept;

    /**
     * @brief 先安全停止当前流水线，再接管另一个流水线。
     * @param other 被转移的流水线。
     * @return 当前对象。
     */
    SendPipeline& operator=(SendPipeline&& other) noexcept;

    /** @brief 禁止复制，避免多个控制对象同时关闭同一流水线。 */
    SendPipeline(const SendPipeline&) = delete;
    /** @brief 禁止复制赋值，避免重复管理槽位与等待者。 */
    SendPipeline& operator=(const SendPipeline&) = delete;

    /**
     * @brief 等待空槽，把数据复制到流水线缓冲区并提交异步发送。
     * @param buffer 待复制的数据；长度不得超过 `buffer_size()`。
     * @return 数据已进入流水线时成功；已关闭、缓冲不足或参数越界时返回错误。
     */
    Task<IoResult<void>> send(ConstBuffer buffer);

    /**
     * @brief 等待当前已提交的全部发送及零拷贝通知退休。
     * @return 本轮流水线首次 I/O 错误；没有错误则成功。流水线仍可继续使用。
     */
    Task<IoResult<void>> drain();

    /**
     * @brief 停止接收新发送并排空全部在途操作。
     * @return 排空期间观察到的首次 I/O 错误。
     */
    Task<IoResult<void>> close();

    /**
     * @brief 返回是否仍绑定有效的流水线状态。
     * @return 非空且尚未关闭时为 true。
     */
    bool valid() const noexcept;

    /**
     * @brief 返回配置的最大在途发送数。
     * @return 空流水线为 0。
     */
    std::size_t depth() const noexcept;

    /**
     * @brief 返回单个流水线缓冲块容量。
     * @return 空流水线为 0。
     */
    std::size_t buffer_size() const noexcept;

    /**
     * @brief 返回尚未退休的发送数量。
     * @return 当前在途槽位数。
     */
    std::size_t inflight() const noexcept;

    /**
     * @brief 返回尚未退休的发送字节总数。
     * @return 当前在途字节数。
     */
    std::size_t inflight_bytes() const noexcept;

private:
    friend class TcpStream;

    /**
     * @brief 接管由 `TcpStream` 创建的共享状态。
     * @param state 包含连接 fd、Context、槽位和缓冲池的内部状态。
     */
    explicit SendPipeline(
        std::shared_ptr<shardio_detail::SendPipelineState> state) noexcept;

    /**
     * @brief 由 `TcpStream::close` 请求停止流水线，并在操作退休后关闭连接 fd。
     * @param state 当前连接按需持有的流水线状态。
     */
    static void close_connection(
        const std::shared_ptr<shardio_detail::SendPipelineState>& state) noexcept;

    std::shared_ptr<shardio_detail::SendPipelineState> state_{}; /**< 持有连接专属流水线状态。 */
};
