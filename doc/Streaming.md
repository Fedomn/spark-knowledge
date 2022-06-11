不论 Spark Streaming 还是 Structured Streaming 都是 Spark 的一种 execution model：

- [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

- Spark Streaming：spark 会以 微批的形式，at fixed time interval 收集数据 并执行计算，返回这个 固定时间内的 结果
- Structured Streaming：构建在 SQL query optimizer Catalyst 之上的 API，提供对 无限表的 连续查询