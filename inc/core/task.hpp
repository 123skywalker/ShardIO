/* task.hpp, C++20 lazy / move-only / 单 awaiter 的用户层 Task */

#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>


/**
 * @brief ShardIO 的协程返回对象：拥有 coroutine frame、返回值与父子 continuation。
 * @details Lazy：构造后不跑函数体，直到 `co_await std::move(task)`。
 *          Move-only、同一时刻最多一个 awaiter。不负责 io_uring、socket、线程池或 Buffer。
 *          不提供 detach / fire-and-forget；顶层任务用 `IoContext::spawn(Task<void>)`。
 *          `Task` 本身不按 `void` 整类特化；`promise_type` 与 `Awaiter::await_resume` 在 `T = void` 时特化。
 * @tparam T `co_return` 的值类型；`T = void` 时无返回值。
 */
template <typename T>
class Task {
public:
    /**
     * @brief 编译器为 `Task<T> foo()` 生成的 promise。
     * @details `T` 非 void：`return_value` + `value_`。`T = void`：见下方显式特化，使用 `return_void`。
     */
    struct promise_type;

    /**
     * @brief 指向本 Task 对应 coroutine frame 的句柄类型。
     */
    using handle_type = std::coroutine_handle<promise_type>;

    /**
     * @brief 子协程结束时用对称转移直接 resume 父协程的 awaiter。
     * @details `final_suspend` 返回本类型，避免多一层 `resume()`。
     */
    struct FinalAwaiter {
        /**
         * @brief 始终 suspend，以便在 `await_suspend` 里切换到 continuation。
         * @return 恒为 false。
         */
        bool await_ready() noexcept;

        /**
         * @brief 若有父协程则返回其 handle（对称转移），否则 `noop_coroutine`。
         * @param handle 即将销毁前的子协程。
         * @return 接下来要跑的协程。
         */
        std::coroutine_handle<> await_suspend(handle_type handle) noexcept;

        /**
         * @brief final_suspend 无需产出值。
         */
        void await_resume() noexcept;
    };

    /**
     * @brief `co_await` 一个 Task 时的 awaiter；接管 Task 对 frame 的所有权。
     */
    class Awaiter {
    public:
        /**
         * @brief 接管即将被 co_await 的 frame（由 `operator co_await` 构造）。
         */
        explicit Awaiter(handle_type handle) noexcept;

        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;
        Awaiter(Awaiter&& other) noexcept;
        Awaiter& operator=(Awaiter&& other) noexcept;

        /**
         * @brief 仍持有且未被 `await_resume` 消费的 frame 则 `destroy`。
         */
        ~Awaiter();

        /**
         * @brief 子 Task 若已 `done` 则不必挂起父协程。
         * @return 已完成 true；Lazy 首次 await 一般为 false。
         */
        bool await_ready() const noexcept;

        /**
         * @brief 登记父协程为 continuation，并对称转移到子协程（开始或继续执行）。
         * @param continuation 正在 `co_await` 的父协程。
         * @return 子协程 handle。
         */
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept;

        /**
         * @brief `T` 非 void：取出 `co_return` 的值。`T = void`：见显式特化，只处理异常。
         * @details 若有未处理异常则重新抛出。取值后销毁 frame。
         * @return 移动自 promise 的 `value_`。
         */
        T await_resume();

    private:
        handle_type handle_{};  /**< 从 Task 偷走的 frame；由本 Awaiter 取值后或析构时销毁。 */
    };

    /**
     * @brief 空 Task，不拥有 frame。
     */
    Task() noexcept = default;

    /**
     * @brief 接管一个已创建的 coroutine handle。
     * @param handle 通常来自 `promise_type::get_return_object`。
     */
    explicit Task(handle_type handle) noexcept;

    /**
     * @brief 若仍拥有 frame 则 `handle.destroy()`。
     * @details 不得销毁仍在执行或仍需被 resume 的协程。
     */
    ~Task();

    /**
     * @brief 转移 frame 所有权；源变为空。
     * @param other 被掏空的源 Task。
     */
    Task(Task&& other) noexcept;

    /**
     * @brief 先销毁自身 frame，再接管 `other`。
     * @param other 被掏空的源 Task。
     * @return `*this`。
     */
    Task& operator=(Task&& other) noexcept;

    /** @brief 禁止拷贝：一个 frame 只能有一个 owner。 */
    Task(const Task&) = delete;
    /** @brief 禁止拷贝赋值。 */
    Task& operator=(const Task&) = delete;

    /**
     * @brief 当前是否拥有 coroutine frame。
     */
    bool valid() const noexcept;

    /**
     * @brief 协程是否已到达 `final_suspend`（空 Task 视为 done）。
     * @details 供测试 / 生命周期检查，不要用来轮询。
     */
    bool done() const noexcept;

    /**
     * @brief 以右值 `co_await`，把所有权交给 Awaiter。
     * @return 用于挂起父协程并启动本 Task 的 Awaiter。
     */
    Awaiter operator co_await() && noexcept;

    /**
     * @brief 启动尚未执行的 lazy 协程；所有权仍在本 Task。
     * @details `IoContext::spawn` 把 frame handle 拷进 ready 回调后再 `resume`，不在 `spawn` 调用栈上跑用户代码。
     */
    void start() noexcept;

    /**
     * @brief 当前 frame 的 type-erased handle；所有权仍在 Task。
     * @details 仅供 `spawn` 延迟启动拷贝；禁止 `destroy`。
     */
    std::coroutine_handle<> handle() const noexcept;

private:
    handle_type handle_{};  /**< 拥有的 coroutine frame；空 Task 为 nullptr。 */
};

/**
 * @brief `Task<T>`（`T` 非 void）的 promise：`co_return value` 写入 `value_`。
 */
template <typename T>
struct Task<T>::promise_type {
    /**
     * @brief 由本 promise 生成对外的 `Task<T>`。
     * @return 持有 `from_promise(*this)` 的 Task。
     */
    Task get_return_object();

    /**
     * @brief Lazy：函数体在首次 `co_await` 前不执行。
     * @return `suspend_always`。
     */
    std::suspend_always initial_suspend() noexcept;

    /**
     * @brief 结束后对称转移回父协程。
     * @return `FinalAwaiter`。
     */
    FinalAwaiter final_suspend() noexcept;

    /**
     * @brief 对应 `co_return value`，写入 `value_`。
     * @tparam U 可转为 T 的类型。
     * @param value 返回值，通常被移动进 `value_`。
     */
    template <typename U>
    void return_value(U&& value);

    /**
     * @brief 保存协程内未捕获异常，留待 `await_resume` 再抛。
     */
    void unhandled_exception() noexcept;

    std::optional<T> value_{};  /**< `co_return` 写入的成功值。 */
    std::exception_ptr exception_{};  /**< 未捕获异常；空表示无异常。 */
    std::coroutine_handle<> continuation_{};  /**< 正在 await 本 Task 的父协程；无父则为空。 */
};

/**
 * @brief `Task<void>` 的 promise：`co_return;` 走 `return_void`，没有 `value_`。
 * @details 嵌套类特化，不是 `Task` 整类特化。生命周期 / Awaiter 仍用上面的 `Task<T>`。
 */
template <>
struct Task<void>::promise_type {
    /**
     * @brief 生成对外 `Task<void>`。
     */
    Task<void> get_return_object();

    /**
     * @brief Lazy 起点。
     */
    std::suspend_always initial_suspend() noexcept;

    /**
     * @brief 结束后转回父协程。
     */
    Task<void>::FinalAwaiter final_suspend() noexcept;

    /**
     * @brief 对应 `co_return;` 或函数末尾隐式返回。
     */
    void return_void() noexcept;

    /**
     * @brief 保存未捕获异常。
     */
    void unhandled_exception() noexcept;

    std::exception_ptr exception_{};  /**< 未捕获异常。 */
    std::coroutine_handle<> continuation_{};  /**< 父协程；可空。 */
};

template <typename T>
Task<T>::Task(handle_type handle) noexcept : handle_(handle)
{
}

template <typename T>
Task<T>::~Task()
{
    if (handle_) {
        handle_.destroy();
    }
}

template <typename T>
Task<T>::Task(Task&& other) noexcept : handle_(other.handle_)
{
    other.handle_ = {};
}

template <typename T>
Task<T>& Task<T>::operator=(Task&& other) noexcept
{
    if (this != &other) {
        if (handle_) {
            handle_.destroy();
        }
        handle_ = other.handle_;
        other.handle_ = {};
    }
    return *this;
}

template <typename T>
bool Task<T>::valid() const noexcept
{
    return static_cast<bool>(handle_);
}

template <typename T>
bool Task<T>::done() const noexcept
{
    return !handle_ || handle_.done();
}

template <typename T>
typename Task<T>::Awaiter Task<T>::operator co_await() && noexcept
{
    handle_type h = handle_;
    handle_ = {};
    return Awaiter{h};
}

template <typename T>
void Task<T>::start() noexcept
{
    handle_.resume();
}

template <typename T>
std::coroutine_handle<> Task<T>::handle() const noexcept
{
    return handle_;
}

template <typename T>
Task<T>::Awaiter::Awaiter(handle_type handle) noexcept : handle_(handle)
{
}

template <typename T>
Task<T>::Awaiter::Awaiter(Awaiter&& other) noexcept : handle_(other.handle_)
{
    other.handle_ = {};
}

template <typename T>
typename Task<T>::Awaiter& Task<T>::Awaiter::operator=(Awaiter&& other) noexcept
{
    if (this != &other) {
        if (handle_) {
            handle_.destroy();
        }
        handle_ = other.handle_;
        other.handle_ = {};
    }
    return *this;
}

template <typename T>
Task<T>::Awaiter::~Awaiter()
{
    // await_resume 已消费则 handle_ 为空，避免二次 destroy
    if (handle_) {
        handle_.destroy();
    }
}

template <typename T>
bool Task<T>::Awaiter::await_ready() const noexcept
{
    return handle_ && handle_.done();
}

template <typename T>
std::coroutine_handle<> Task<T>::Awaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept
{
    handle_.promise().continuation_ = continuation;
    // 对称转移：直接去跑子协程
    return handle_;
}

template <typename T>
T Task<T>::Awaiter::await_resume()
{
    auto h = handle_;
    handle_ = {};
    auto& p = h.promise();
    if (p.exception_) {
        auto e = p.exception_;
        h.destroy();
        std::rethrow_exception(e);
    }
    T value = std::move(*p.value_);
    h.destroy();
    return value;
}

template <>
inline void Task<void>::Awaiter::await_resume()
{
    auto h = handle_;
    handle_ = {};
    auto e = h.promise().exception_;
    h.destroy();
    if (e) {
        std::rethrow_exception(e);
    }
}

template <typename T>
bool Task<T>::FinalAwaiter::await_ready() noexcept
{
    return false;
}

template <typename T>
std::coroutine_handle<> Task<T>::FinalAwaiter::await_suspend(
    handle_type handle) noexcept
{
    auto cont = handle.promise().continuation_;
    // 根任务没有父协程：停在 final_suspend，等 Task 析构 destroy
    return cont ? cont : std::noop_coroutine();
}

template <typename T>
void Task<T>::FinalAwaiter::await_resume() noexcept
{
}

template <typename T>
Task<T> Task<T>::promise_type::get_return_object()
{
    return Task<T>{handle_type::from_promise(*this)};
}

template <typename T>
std::suspend_always Task<T>::promise_type::initial_suspend() noexcept
{
    return {};
}

template <typename T>
typename Task<T>::FinalAwaiter Task<T>::promise_type::final_suspend() noexcept
{
    return {};
}

template <typename T>
template <typename U>
void Task<T>::promise_type::return_value(U&& value)
{
    value_ = std::forward<U>(value);
}

template <typename T>
void Task<T>::promise_type::unhandled_exception() noexcept
{
    exception_ = std::current_exception();
}

inline Task<void> Task<void>::promise_type::get_return_object()
{
    return Task<void>{Task<void>::handle_type::from_promise(*this)};
}

inline std::suspend_always Task<void>::promise_type::initial_suspend() noexcept
{
    return {};
}

inline Task<void>::FinalAwaiter Task<void>::promise_type::final_suspend() noexcept
{
    return {};
}

inline void Task<void>::promise_type::return_void() noexcept
{
}

inline void Task<void>::promise_type::unhandled_exception() noexcept
{
    exception_ = std::current_exception();
}
