# 日志契约

`stable-placement` 定义自己的最小 `Logger` 接口，不直接依赖具体日志库。实现只需提供 `Debugf`、`Warnf` 和 `Errorf`；`libs/logger.LoggerFacade` 满足该接口，可由应用在启动时直接注入。

根包提供：

- `DefaultLogger()`：使用标准库默认 logger；
- `StdLogger`：包装指定的 `log.Logger`；
- `NopLogger`：显式关闭日志。

## 注入位置

memory directory：

```go
directory, err := memory.NewDirectory(
    registry,
    stableplacement.StrategyModeGo,
    strategy,
    publisher,
    memory.WithLogger(logger),
)
```

Redis EventBus：

```go
bus := redis.NewEventBus(client, consumer, redis.WithLogger(logger))
```

未配置 logger 时使用 `stableplacement.DefaultLogger()`。logger 随组件实例保存，不使用可变的包级全局 logger。

## 记录边界

同步 API 已将错误返回给调用方时，库内不重复记录，例如 Directory、NodeRegistry、strategy、cache 和 Redis 命令错误。

库内记录以下无法由调用方完整观察的状态：

- memory directory 已完成状态提交，但 best-effort 事件发布失败，记录为 `Warn`；
- Redis EventBus 首次进入 degraded 状态时，记录触发操作、consumer 信息和根因，记录为 `Error`；
- EventBus 已处于 degraded 状态后不重复输出同类错误，避免故障期间刷屏。

`context.Canceled` 和 `context.DeadlineExceeded` 不触发日志或 degraded 状态。

## 数据安全

日志可以包含事件类型、GrainKey、PlacementID、NodeIdentity、NodeSessionID、consumer group 和错误链。不得记录业务消息 payload、认证凭据或节点连接密钥。
