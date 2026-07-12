# stable-placement Node Lease v2 BDD Checklist

Redis 默认地址：`127.0.0.1:6379`，可通过 `STABLE_PLACEMENT_REDIS_ADDR` 覆盖。密码只能通过 `STABLE_PLACEMENT_REDIS_PASSWORD` 注入。

```bash
STABLE_PLACEMENT_REDIS_ADDR=127.0.0.1:6379 \
  go test -tags=integration ./example/node-bdd/ -v
```

## A. Node Lease

- [x] RegisterNode 创建 Active Node，Lease 版本为 1。
- [x] TTLMillis 来自构造时不可变配置；默认一分钟，非正数被拒绝。
- [x] 相同 session 重复 RegisterNode 幂等但不续约。
- [x] 不同 session RegisterNode 失败，必须 ReplaceNodeSession。
- [x] RenewNode 使用持久化 TTLMillis，推进版本且不复活过期 session。
- [x] Redis 以 Redis TIME 判断到期；Memory 使用内部 clock。
- [x] ExpireNodeLeases 推进 Offline 并只发布一次 NodeLeaseExpired。

## B. Placement 路由

- [x] Allocate 只选择 Active、未失效且 Node Lease 有效的节点。
- [x] Lookup 返回带 OwnerNodeSessionID、NodeLeaseVersion、ValidUntil 的 route。
- [x] Node Lease 到期后 Lookup/Exists 立即失效，不等待扫描器。
- [x] Node Lease 到期不改写 Placement，FindByNode 仍可查询记录。
- [x] Directory.Renew 只校验并审计，不延长 TTL。
- [x] 不可用 Owner 的 Active Placement 不会被 Allocate 自动重绑。

## C. Session、恢复与迁移

- [x] ReplaceNodeSession 后旧 Placement 不可路由，新 session 不自动继承。
- [x] 健康 Owner 拒绝 Recover，使用 Transfer。
- [x] missing、Offline、过期或 session 不匹配 Owner 可显式 Recover。
- [x] Transfer 可显式迁移健康或不可用 Owner。
- [x] Release 校验 session 和 PlacementVersion；未替换 session 即使 Lease 到期仍可释放。

## D. Memory / Redis 一致性

- [x] TTL 默认值和非法输入错误一致。
- [x] NodeIdentity 元数据校验一致。
- [x] Register/Replace/Renew 错误语义一致。
- [x] 到期边界、路由资格和 Recover/Transfer 语义一致。
- [x] v1 key 预置不影响 v2 数据与结果。

## E. 部署验收

- [ ] v1 writer、Node、scanner、consumer 未全部停止时，部署门禁拒绝开放 v2 流量。
- [ ] 业务确认旧 Grain 执行停止，并清空所有进程内 Placement 缓存。
- [ ] 检查 `redis.NamespaceVersion == "v2"` 和 `redis.NamespacePrefix == "sp:{stable-placement}:v2:"`。
- [ ] 全部 v2 writer、Node、Node Lease scanner 和 Stream consumer 一次性启动，不做滚动混部。
- [ ] 首笔 v2 写入后禁止直接回滚 v1。
- [ ] 稳定且确认无需回退后，人工清理 v1 key。

完整步骤见 [`docs/node-lease-v2-cutover.md`](../../docs/node-lease-v2-cutover.md)。

## 历史场景

旧 Grain Lease、LeaseVersion、heartbeat、Expire、ExpireDue 和 ExpireHeartbeats 检查项已被 Node Lease v2 取代，不代表当前 API 或运行能力。
