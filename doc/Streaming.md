## doc

不论 Spark Streaming 还是 Structured Streaming 都是 Spark 的一种 execution model：

- [(Legacy) Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
  - Together, using replayable sources and idempotent sinks, Structured Streaming can ensure end-to-end exactly-once semantics under any failure.


- (Legacy) Spark Streaming：spark 会以 微批的形式，at fixed time interval 收集数据 并执行计算，返回这个 固定时间内的 结果
- Structured Streaming：构建在 SQL query optimizer Catalyst 之上的 API，提供对 无限表的 连续查询。默认情况下，是 micro-batch 处理引擎。
  - The key idea in Structured Streaming is to treat a live data stream as a table that is being **continuously** appended.

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

## arbitrary stateful processing

Stateful processing is available only in Scala in Spark 2.2 to meet custom aggregation

- mapGroupsWithState: Map over groups in your data, operate on each group of data, and generate at most a single row for each group
- flatMapGroupsWithState: Map over groups in your data, operate on each group of data, and generate one or more rows for each group.
- currently, above two function only supported in Scala/Java.


## streaming joins

- stream-static joins: a data stream join with a static dataset
  - stateless operations, and therefore do not require any kind of watermarking.
  - static DataFrame is read repeatedly while joining with the streaming data of every micro-batch, so you can cache the static DataFrame to speed up the read.
- stream-stream joins: 
  - The challenge of generating joins between two data streams is that, at any point in time, the view of either Dataset is incomplete, making it much harder to find matches between inputs. Hence, for both the input streams, we buffer past input as streaming state, so that we can match every future input with past input and accordingly generate joined results.
  - For inner joins, specifying watermarking and event-time constraints are both optional.In other words, at the risk of potentially unbounded state, you may choose not to specify them. Only when both are specified will you get state cleanup.
  - Unlike with inner joins, the watermark delay and event-time constraints are not optional for outer joins. This is because for generating the NULL results, the engine must know when an event is not going to match with anything else in the future. For correct outer join results and state cleanup, the watermarking and event-time constraints must be specified.

- [Introducing Stream-Stream Joins in Apache Spark 2.3](https://www.databricks.com/blog/2018/03/13/introducing-stream-stream-joins-in-apache-spark-2-3.html)