# Prototype 的 Session 与 Root Span 问题分析

日期：2026-04-23
背景：排查为什么 prototype 在 Phoenix 里顶层 span 明明带有 `session.id`，但 session 页面里仍然出现 `rootSpan: null`，并且页面上看不到可用数据。

## 观察到的现象

根据 prototype 的测试结果：

- 顶层 workflow 类 span 带有非空的 `session.id`
- 同一个 trace 内部的 child span 往往是 `session.id: ""`
- Phoenix 的 session 查询能查到 session，也能查到 `numTraces`
- 但 session 里的每个 trace 都返回 `rootSpan: null`
- 因此 Phoenix 的 session 页面无法显示正常的 trace tree

## 核心结论

这里大概率有两个问题，而且它们叠在一起了：

1. `session.id` 传播不一致
2. prototype 很可能没有正确创建真正的 root workflow span，导致 Phoenix 无法解析 `rootSpan`

第二个问题更关键。

## 问题一：`session.id` 传播不一致

prototype 确实在一些地方设置了 `SpanAttributes.SESSION_ID`，但并不是所有 span 都一致设置。

### workflow 根 span

workflow span 使用的是：

- `trace_info.conversation_id or trace_info.workflow_id or ""`

相关代码：

- `/Users/yang/Downloads/arize_phoenix_trace.py:261`

这意味着在 debugging 模式下，即便没有 `conversation_id`，workflow root span 仍然会因为 fallback 到 `workflow_id` 而拿到非空 session ID。

### workflow 内部 node span

workflow node span 使用的是：

- `trace_info.conversation_id or ""`

相关代码：

- `/Users/yang/Downloads/arize_phoenix_trace.py:608`

这意味着在 debugging 模式下，如果没有 `conversation_id`，所有子节点 span 的 session ID 就会变成空字符串。

### message 等其他 trace 类型

另外一些 trace 类型也设置了 `SESSION_ID`，例如 message 和 generate-name：

- `/Users/yang/Downloads/arize_phoenix_trace.py:721`
- `/Users/yang/Downloads/arize_phoenix_trace.py:765`
- `/Users/yang/Downloads/arize_phoenix_trace.py:1059`

但并不是所有 trace path 都统一使用 session，也不是所有 child span 都继承了同一个 session 值。

### 后果

Phoenix 的 session 聚合可能仍然能找到带 session 的顶层 span，但子 span 并没有稳定进入同一个 session 视图。这样在 debugging trace 里尤其容易出现 session 体验不完整的问题。

## 问题二：root span 的构造方式很可能是错的

这看起来才是 Phoenix 返回 `rootSpan: null` 的主因。

### prototype 对 workflow span 的处理方式

对于 root workflow，prototype 大致做了这些事：

1. 从 `trace_id` 或 `workflow_run_id` 计算一个确定性的 trace ID
2. 从 `workflow_run_id` 计算一个确定性的 span ID
3. 用这个 trace ID 和 span ID 构造一个 `SpanContext`
4. 再包成 `trace.NonRecordingSpan(...)`
5. 最后把这个 context 作为 `context=` 传入 `start_span(...)` 来创建 workflow span

相关代码：

- `/Users/yang/Downloads/arize_phoenix_trace.py:221`
- `/Users/yang/Downloads/arize_phoenix_trace.py:227`
- `/Users/yang/Downloads/arize_phoenix_trace.py:283`
- `/Users/yang/Downloads/arize_phoenix_trace.py:288`

### 为什么这很可疑

一个真正的 root span，通常应该在没有 parent context 的情况下创建。

但 prototype 不是这样做的。它是把 workflow span 放进一个已经填充过的 `NonRecordingSpan` context 里再创建。

这会让 Phoenix 有可能把这个本该是 root 的 span 视为：

- 一个 synthetic parent 的子节点
- 一个自引用的子节点
- 或者一个 parent chain 无法解析成合法 root 的 span

即便 OTEL SDK 没有报错，Phoenix 也可能无法把它识别成 trace 的合法 root span。

## 为什么 session 查询结果正好符合这个判断

你贴出的 session 查询结果表现为：

- session 存在
- traces 被计数到了
- 每个 trace 记录也存在
- 但每个 trace 的 `rootSpan` 都是 `null`

这个模式非常像：

- traces 已经被 ingest
- session 层也已经索引到了这些 traces
- 但是 Phoenix 无法计算出一个有效的 root span 记录

这更像是 root span 的 parentage 构造错误，而不是一个纯粹的 session 问题。

## 综合解释

prototype 在 Phoenix 里的 session 展示异常，大概率不是 Phoenix session 功能本身坏了。

更可能的链路是：

1. workflow root span 不是以真正 root 的方式创建出来的
2. Phoenix 无法解析 `rootSpan`
3. session 页面依赖合法的 root span 才能展示 trace tree
4. 同时 child span 的 `session.id` 在 debugging 模式下又传播不完整，让 session 视图进一步变差

## 对后续重实现的直接启发

如果我们在 `origin/main` 上重做这部分，最好把它拆成两个独立要求：

1. 保证真正的 root span 存在
   - root workflow span 不能挂在伪造的 parent context 下创建
   - nested child workflow span 可以显式挂到 parent tool span 下
   - 但 root workflow 本身必须仍然是 root
2. 保证 session 稳定传播
   - 同一个逻辑 session 内的所有 spans 都应该带同一个 session ID
   - debugging 模式下，node span 很可能也需要像 workflow root span 一样 fallback 到 `workflow_id`

## 当前工作假设

prototype 的 session 页面异常，应该不是 Phoenix UI bug。

更可能是 tracing 构造问题：

- root workflow span 是在不正确的 parent context 下创建的
- Phoenix 因而无法识别 `rootSpan`
- 同时 child span 在 debugging 模式下的 `session.id` 传播也不完整

这两者叠加起来，就解释了为什么：

- traces 存在
- sessions 存在
- 但 session 页面里仍然看不到可用的 root trace 数据
