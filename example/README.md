# stable-placement examples

## Redis 集成验收

`example/bdd/` 和 `example/node-bdd/` 面向真实 Redis 验证 Node Lease v2。示例默认 Redis 地址为 `127.0.0.1:6379`；验收命令仍通过 `STABLE_PLACEMENT_REDIS_ADDR` 显式传入，便于审计实际目标。

密码只能通过环境变量注入：

- 普通集成场景：`STABLE_PLACEMENT_REDIS_PASSWORD`
- 真实 Redis 专项测试：`STABLE_PLACEMENT_REAL_REDIS_PASSWORD`

禁止把密码写入 README、命令参数、测试源码或仓库配置。未启用认证时不要设置密码变量；启用认证时先在当前 shell 环境中安全地 `export` 对应变量，下面的命令会从环境继承。

```bash
STABLE_PLACEMENT_REDIS_ADDR=127.0.0.1:6379 \
  go test -tags=integration ./example/bdd/ -v

STABLE_PLACEMENT_REDIS_ADDR=127.0.0.1:6379 \
  go test -tags=integration ./example/node-bdd/ -v
```

真实 Redis 专项套件使用：

```bash
STABLE_PLACEMENT_REDIS_ADDR=127.0.0.1:6379 \
  go test -tags=integration ./redis/... -v
```

## 当前场景语义

| 场景 | 验证点 |
|---|---|
| 节点注册与幂等重试 | Register 创建 Node Lease；同 session 重试不续约 |
| 节点续约 | RenewNode 推进 NodeLeaseVersion，并使用持久化 TTLMillis |
| session 替换 | 必须显式 Replace；新 session 不继承旧 Placement |
| Node Lease 到期 | Lookup/Exists 立即逻辑失效，Placement 记录保留 |
| 故障恢复 | Owner 不可用后显式 Recover |
| 计划迁移 | 健康或不可用 Owner 均可显式 Transfer |
| Memory/Redis 契约 | TTL、错误、到期边界、Register/Replace 和路由资格一致 |

旧 Grain Lease、LeaseVersion、heartbeat 和 Expire 场景属于 v1 历史，不是当前能力。
