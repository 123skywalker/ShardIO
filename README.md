# ShardIO

基于 C++20 协程与 Linux io_uring 的轻量异步网络库，面向低开销、多核 TCP 服务。

- **分核架构**：One Core, One Context，每个 Worker 独占 io_uring，连接固定归属，支持绑核与 SO_REUSEPORT。
- **直接热路径**：同核 I/O 直接提交，普通 CQE inline 恢复协程，减少额外调度。
- **轻量 Buffer**：视图不复制数据，RAII 句柄管理归还，预分配内存循环复用。
- **高级功能**：multishot、provided/fixed buffer、SEND_ZC、Batch、SendPipeline，以及 yield / async_cpu。

## 性能

本地 loopback 基准，128 连接、每组 5 次取中位数：

| 场景 | ShardIO | Asio + io_uring |
|---|---:|---:|
| 64B 请求响应，1 Worker | 36,402 ops/s | 35,865 ops/s |
| 64B 请求响应，4 Workers | 41,558 ops/s | 40,791 ops/s |
| 1MiB 流传输，4 Workers | 6,750.8 MiB/s | 6,674.4 MiB/s |

上述场景与 Asio 接近。数据来自历史基准，开发阶段 Debug、ASan、UBSan、TSan 四组 E2E 均通过。测试与基准文件暂未包含在公开仓库。

## 快速开始

需要 Linux / WSL2、C++20 编译器、CMake 3.20+ 与 liburing。

```bash
sudo apt update
sudo apt install -y build-essential cmake liburing-dev

git clone https://github.com/123skywalker/ShardIO.git
cd ShardIO

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 运行双 Worker 回显示例
./build/shardio_example

# 查看示例源码
less example/example.cpp
```

集成到现有 CMake 项目：

```cmake
add_subdirectory(third_party/ShardIO)
target_link_libraries(your_app PRIVATE shardio)
```

公共头文件：`#include <shardio/shardio.hpp>`。

## License

Apache-2.0，详见 [LICENSE](LICENSE)。
