# Stable Placement Node Lease v2 Ontology

Stable Placement 回答一个问题：一个 Grain 当前由哪个 Node session 负责，以及这个归属当前是否可路由。

本文档描述当前 v2 领域模型。v1 的 Grain Lease、heartbeat、Expire 和逐 Grain 到期改写已废止，仅属于历史背景。

## 一、领域边界

Stable Placement 负责维护唯一归属、节点会话、节点有效性、显式迁移与恢复、路由缓存和领域事件。它不创建 Grain/Actor、不管理业务执行、不投递消息、不做在线 v1 迁移、不自动 Rebalance，也不发现其他进程。

Directory 是唯一真相；缓存和事件都不是唯一真相。

## 二、核心实体

### Grain

Grain 是业务逻辑实体。`Kind + GrainID` 构成 GrainKey；TargetNodeType 和 TargetNodeGroup 确定候选节点集合。

### Node 与 NodeSession

NodeIdentity 是 `NodeType/NodeGroup/NodeName` 组成的稳定身份。NodeSessionID 是一次运行实例的唯一身份，同名 Node 重启必须生成新 session。

RegisterNode 不能用不同 session 覆盖现有 Node；替换必须显式调用 ReplaceNodeSession。新 session 不继承旧 session 的 Placement。

### NodeLease

```text
NodeSession 1 -> 1 NodeLease
NodeLease = Version + TTLMillis + ExpireAtUnixMilli
```

Node Lease 是 v2 唯一的 TTL 所有者。配置在实例构造时固定，默认一分钟，非正数被拒绝。TTLMillis 在注册或替换 session 时持久化，续约使用持久值。

Redis 以 Redis TIME 为权威；Memory 以同一操作内的内部 clock 快照为权威。Lease 一旦到期，RenewNode 不能复活该 session。

### Placement

```text
Grain 1 -> 0..1 Active Placement
Placement -> NodeIdentity + OwnerNodeSessionID snapshot
```

Placement 是持久归属关系，不是运行中的 Grain，也不是路由授权。它只含 Active 和 Released 状态，不含 TTL。

Node Lease 到期、Node Offline、Node 删除或 session 替换会使 Placement 逻辑不可路由，但不会改变 Placement 状态或 Owner 快照。保留记录用于 FindByNode、显式 Recover 或显式 Transfer。

### PlacementRoute

PlacementRoute 是 Lookup 返回的当前进程路由快照，包含 Owner session、PlacementVersion、NodeLeaseVersion 和 ValidUntil。ValidUntil 是本地保守截止时间，不持久化；超过截止时间必须回源，不能继续使用缓存。

### InvalidNodeGroup

InvalidNodeGroup 按 NodeType、NodeGroup 和 NodeName 控制新分配。它跨 NodeSessionID 保留，只能由 RestoreNode 清除，不自动迁移已有 Placement。

## 三、核心关系与规则

### 唯一归属

同一 Grain 最多有一条 Active Placement。Active 只表示记录尚未 Release；是否可路由还要检查当前 Owner Node、session、状态和 Node Lease。

### 可路由资格

Placement 可路由必须满足：

1. Placement 为 Active。
2. Owner Node 存在。
3. Placement 的 OwnerNodeSessionID 等于 Node 当前 session。
4. Node 为 Active 或 Draining。
5. 权威时间早于 Node Lease 的 ExpireAtUnixMilli。

Lookup 对不满足条件的记录返回 NotFound，Exists 返回 false。逻辑失效立即生效，不依赖扫描器。

### Owner 变化

Owner 只能由以下显式行为改变：

- Transfer：计划内或管理操作，可迁移健康或不可用 Owner。
- Recover：仅接管逻辑不可用的 Active Owner。
- Release：终止归属。
- 对 Released 记录重新 Allocate。

RegisterNode、RenewNode、Lookup、节点到期和扫描器都不能重绑 Placement。健康 Owner 不能 Recover，必须 Transfer。

### Register 与 Replace

RegisterNode 对相同 session 是不续约的幂等重试；不同 session 必须失败。ReplaceNodeSession 显式创建版本从 1 开始的新 Lease，发布 NodeReplaced 并清缓存。旧 Placement 仍指向旧 session，因此不会被新进程自动接管。

### Renew

RenewNode 延长 Node Lease 并推进 NodeLeaseVersion。Directory.Renew 不续 TTL；它只校验 Placement、Owner session、PlacementVersion 和 Node Lease，并产生审计事件。

### 逻辑失效与扫描

Lease 到期时，关联 Grain 立即全部失去路由资格，不逐条改写 Placement。ExpireNodeLeases 只是有界地把 Node 持久状态推进为 Offline 并发布一次 NodeLeaseExpired；Node tombstone 和 Placement 索引保留。

## 四、缓存与事件

Redis Lookup 用同一次 Redis TIME 计算剩余 TTL；本地从请求开始时刻计算 ValidUntil，使网络延迟只缩短缓存窗口。Memory 使用同一个 clock 快照完成判断和截止时间计算。

NodeLeaseExpired、NodeReplaced、NodeDraining、NodeMarkedInvalid 和 NodeUnregistered 按 NodeIdentity 清缓存。订阅不连续或事件无法判断影响范围时清空缓存并降级回源。PlacementRenewed 是审计，不触发缓存失效。

## 五、实现一致性

Memory 与 Redis 必须具有相同的公共契约、错误类型、TTL 规则、到期边界、session 校验和 Placement 行为。Memory 的锁和 clock、Redis 的 Lua 和 Redis TIME 是实现细节，不改变领域语义。

## 六、Redis v2 与部署边界

v2 使用独立 namespace，既不读取也不清理 v1 key。本库暴露 namespace/version 常量供部署门禁校验，但不探测其他进程。

部署系统必须确保 v1 writer、Node、scanner 和 consumer 全部停止，并确认 Grain 执行停止、缓存清空后，才一次性启动 v2 和开放流量。首笔 v2 写入后不能直接回滚 v1。完整步骤见 [Node Lease v2 冷切换手册](./node-lease-v2-cutover.md)。

## 七、v1 历史概念

Grain Lease、LeaseVersion、LeaseTTL、ExtendTTL、heartbeat、Expire、ExpireDue、ExpireHeartbeats、PlacementStatusExpired 和 ErrLeaseExpired 都是被 v2 取代的历史概念，不是当前能力。
