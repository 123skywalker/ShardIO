# ShardIO E2E 测试结构

`runtime_suite.cpp` 保留原有全部测试场景和断言，但通过数据驱动清单划分为五组：

- `correctness`：基础 I/O、TCP、Buffer、multishot 和公开接口闭环。
- `corner`：取消、EOF、RST、EPIPE、SQ 满、ENOBUFS 和停止竞态。
- `scheduler`：yield、公平性、CPU executor 和协程生命周期。
- `stress`：跨线程投递、并发 echo、burst、chaos 和高并发场景。
- `benchmark`：吞吐、延迟、扩展性和 send/send_zc 对比；默认不进入 CTest 发布正确性门禁。

## 运行方式

```bash
./e2e_runtime --group correctness
./e2e_runtime --group corner --group scheduler
./e2e_runtime --test tcp_recv_multi_api
./e2e_runtime --group benchmark
```

不传筛选参数时运行全部分组；设置 `SHARDIO_SKIP_BENCH=1` 时跳过 benchmark。

## 并发生命周期约束

所有 multishot 测试遵守以下顺序：

```text
owner Context 提交/取消
→ sink.on_end
→ Runtime::barrier
→ 销毁 Operation、Sink、TcpStream/TcpListener
```

`on_end` 只表示最后一笔 CQE 已进入回调，不表示 `complete()` 已经退出调用栈。
`Runtime::barrier()` 是测试模块统一的静默点，禁止使用固定 sleep 猜测异步对象何时可以析构。
