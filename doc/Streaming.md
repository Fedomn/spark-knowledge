## doc

不论 Spark Streaming 还是 Structured Streaming 都是 Spark 的一种 execution model：

- [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
  - Together, using replayable sources and idempotent sinks, Structured Streaming can ensure end-to-end exactly-once semantics under any failure.


- Spark Streaming：spark 会以 微批的形式，at fixed time interval 收集数据 并执行计算，返回这个 固定时间内的 结果
- Structured Streaming：构建在 SQL query optimizer Catalyst 之上的 API，提供对 无限表的 连续查询

## output modes 配合 watermark

- update: 
  - 支持所有 (window) aggregation，`每个微批 只会输出 updated 的聚合结果`
  - watermarking 确保 state 会 regular clean
  - 不能用于 aggregation to append-only sinks，比如 file-based formats
- complete:
  - 支持所有 (window) aggregation，`每个微批 都会输出 所有发生过 updated 的聚合结果，不论它们在当前 window 是否包含变化`
  - 即使配置了 watermark，state 也不会被 clean，小心使用，它会造成大量 memory 开销
- append:
  - 只支持 aggregation on event-time windows + watermarking enabled (因为如果没有水位，每个 agg 结果都会在未来被更新，也谈不上 append 了)
  - 它不允许以前输出的 聚合结果 发生变化，保证方式是 group by event-time col + watermark event-time，能知道聚合何时不再更新 
  - `每个微批 不是输出 updated 的聚合结果，而是 watermark 确保 aggregation window 不在更新时，才输出 window 中的 聚合结果`
  - 优势，可用于 append-only sinks (file)
  - 劣势，latency，输出结果 必须等待一个 watermark 结束后，才能得到结果