# Stable Placement Ontology

当前版本使用 Node Lease v2：Lease 只属于 NodeSession。旧 Grain Lease、heartbeat、Expire 和 Placement Expired 语义已被取代，不是当前能力。

Stable Placement 不应该先被定义成 Redis 结构、Hash 结构、Lease 实现或 protoactor-go 的内部模块。

它首先是一个领域模型：

```text
Stable Placement 是一个负责维护 Grain 唯一归属关系的领域。
它保证 Grain 能够被稳定定位、唯一拥有、按策略分配、在故障时恢复，并在需要时安全迁移。
```

核心问题不是：

```text
Redis 怎么存？
```

而是：

```text
一个 Grain 当前应该由哪个节点负责？
这个归属关系如何保持唯一、稳定、可发现、可恢复、可迁移？
```

底层可以是 Memory、Redis、etcd、Database 或自研 Placement Service。

本体模型不应该因为存储实现变化而变化。

---

## 一、核心目标

Stable Placement 的目标不是实时负载均衡，而是：

```text
保证一个 Grain 在整个生命周期内拥有稳定、唯一、可管理的 Owner。
```

关键词：

- Stable：正常情况下归属不随重启、GC、Actor 回收而变化。
- Unique：同一个 Grain 同一时间最多只有一个有效 Owner。
- Discoverable：任意节点都可以查询 Grain 当前 Owner。
- Recoverable：Owner 节点故障后可以恢复归属。
- Transferable：需要迁移时可以显式转移归属。
- Scalable：节点可以按 NodeType + NodeGroup 横向扩展。

---

## 二、领域边界

Stable Placement 负责：

- 维护 Grain 到 Node 的归属关系。
- 保证同一个 Grain 同一时间只有一个有效 Owner。
- 提供 Lookup / Allocate / Renew / Release / Transfer / Recover 能力。
- 维护节点身份、节点会话、节点失效组。
- 提供可主动失效的本地缓存模型。
- 产生领域事件，供缓存失效、审计、Dashboard 和运维使用。

Stable Placement 不负责：

- 创建 Actor。
- 管理 Actor 生命周期。
- 投递业务消息。
- 替代 protoactor-go remote。
- 做实时负载均衡。
- 决定业务协议。
- 绑定某一种存储实现。

边界判断：

```text
是否在回答 "这个 Grain 现在应该归属于谁"？

是  -> 属于 Stable Placement。
否  -> 大概率属于 Actor Runtime、Remote、业务服务或运维系统。
```

---

## 三、核心实体

Stable Placement 至少包含以下实体：

```text
Grain
Node
Placement
Directory
NodeLease
PlacementStrategy
LocalPlacementCache
InvalidNodeGroup
DomainEvent
```

### 1. Grain

Grain 是一个逻辑实体。

示例：

```text
Player
Guild
Mail
Rank
Scene
```

属性：

```text
GrainID
Kind
TargetNodeType
TargetNodeGroup
```

说明：

- Grain 不关心自己在哪台机器。
- Kind 用于区分业务类型。
- TargetNodeType + TargetNodeGroup 用于确定候选节点集合。
- Grain 本身不是 Actor，也不要求一定由 Actor 承载。

### 2. Node

Node 是一个运行节点。

节点身份由三部分组成：

```text
NodeType
NodeGroup
NodeName
```

组合后形成稳定身份：

```text
NodeIdentity = NodeType + "/" + NodeGroup + "/" + NodeName
```

示例：

```text
game/default/game-1
game/default/game-2
chat/world/chat-1
battle/pve/battle-3
```

属性：

```text
NodeType
NodeGroup
NodeName
NodeIdentity
NodeSessionID
Address
Weight
Load
Status
Lease
```

说明：

- NodeIdentity 表示稳定逻辑节点名。
- NodeSessionID 表示一次具体运行实例。
- 同一个 NodeIdentity 可以因为进程重启产生新的 NodeSessionID。
- 同名节点新 session 注册后，旧 session 的 Renew / Release 必须失败。
- RegisterNode 对相同 session 幂等但不续约；不同 session 必须显式 ReplaceNodeSession。
- Node Lease 是唯一 TTL 所有者，绑定当前 NodeSession。
- 节点注册成功只表示在线，不等于一定可以参与 Allocate。

### 3. Placement

Placement 是核心关系。

它表示：

```text
Grain -> Node
```

示例：

```text
Player10001 -> game/default/game-2
```

属性：

```text
GrainID
Kind
NodeIdentity
OwnerNodeSessionID
Version
Status
CreateTime
UpdateTime
```

说明：

- Placement 不是 Actor。
- Placement 不是一次消息路由。
- Placement 是 Grain 到 Node 的稳定归属关系。
- Placement 保存完整 NodeIdentity，而不是只保存 NodeName 或临时地址。
- Placement 保存分配时的 OwnerNodeSessionID 快照，但不保存 TTL。
- Node Lease 到期只让 Placement 逻辑不可路由，记录和 Active 状态继续保留。
- Lookup 只能查询 Placement，不能隐式创建、迁移或恢复 Placement。

### 4. Directory

Directory 是 Placement 的唯一真相。

它维护：

```text
GrainKey -> Placement
NodeIdentity -> Placement Set
Kind -> Placement Set
NodeType + NodeGroup -> Placement Set
```

能力：

```text
Lookup
Allocate
Renew
Release
Transfer
Recover
Exists
FindByNode
FindByKind
FindByGroup
```

说明：

- Directory 是 Source of Truth。
- 本地缓存不是 Source of Truth。
- FindByNode 必须按完整 NodeIdentity 查询。
- FindByNode 默认返回 Active Placement。
- FindByNode 必须支持分页或游标，不能依赖全量扫描。

### 5. NodeLease 与 PlacementRoute

NodeLease 保护一个 NodeSession 承载的全部 Grain，是唯一 TTL 所有者。

属性：

```text
Version
TTLMillis
ExpireAtUnixMilli
```

说明：

- TTL 在构造 Memory/Redis 实例时不可变，默认一分钟，非正数配置被拒绝。
- RegisterNode/ReplaceNodeSession 持久化 TTLMillis；RenewNode 使用持久值且不能复活过期 session。
- Redis 以 Redis TIME 为权威，Memory 使用一次操作内的内部 clock 快照。
- PlacementRoute 携带 OwnerNodeSessionID、NodeLeaseVersion 和进程内 ValidUntil；超过 ValidUntil 必须回源。

### 6. PlacementStrategy

PlacementStrategy 只负责首次放置。

输入：

```text
GrainID
Kind
NodeType
NodeGroup
EffectiveNodes
```

输出：

```text
NodeIdentity
```

说明：

- Strategy 不参与 Lookup。
- Strategy 不能看到已经失效的节点。
- Strategy 只在 NodeType + NodeGroup 的 EffectiveNodes 中选择。
- 已有 Placement 不会因为 Strategy 变化而自动迁移。

### 7. LocalPlacementCache

LocalPlacementCache 是节点本地缓存。

它缓存：

```text
GrainKey -> PlacementRoute
NodeIdentity -> GrainKey Set
```

说明：

- 缓存只用于加速 Lookup。
- 缓存不是 Source of Truth。
- 缓存不确定时必须回源 Directory。
- 收到失效事件时必须主动清理。
- 事件订阅异常时必须清空缓存或降级回源。
- 降级回源期间禁止读写本地缓存。

### 8. InvalidNodeGroup

InvalidNodeGroup 表示某个 NodeType + NodeGroup 下不允许参与新分配的 NodeName 集合。

结构：

```text
key:   NodeType + "/" + NodeGroup
value: NodeName Set
```

示例：

```text
key: game/default
value:
  - game-2
  - game-5
```

说明：

- InvalidNodeGroup 按 NodeName 生效。
- InvalidNodeGroup 不按 NodeSessionID 生效。
- 同名节点换新的 NodeSessionID 后，失效状态仍然持续有效。
- 只有 RestoreNode 才能恢复该 NodeName 的可分配资格。
- 已有 Placement 不会因为 MarkNodeInvalid 自动迁移。

### 9. DomainEvent

DomainEvent 表示领域状态发生变化。

它用于：

- 本地缓存失效。
- Dashboard。
- Debug。
- 审计。
- 运维排障。

事件不是 Source of Truth。

Directory 仍然是 Source of Truth。

---

## 四、核心属性

### GrainKey

```text
GrainKey = Kind + "/" + GrainID
```

用于唯一标识一个业务逻辑实体。

### NodeIdentity

```text
NodeIdentity = NodeType + "/" + NodeGroup + "/" + NodeName
```

用于标识稳定逻辑节点。

### NodeSessionID

```text
NodeSessionID = 一次节点运行实例的唯一标识
```

用于区分同名节点的不同运行实例。

### PlacementVersion

PlacementVersion 表示 Placement 关系的版本。

当 Placement 被 Transfer / Release / Recover 或 Released 后重新 Allocate 改变时，版本必须推进。

### NodeLeaseVersion

NodeLeaseVersion 表示当前 NodeSession Lease 的版本。

RenewNode 推进 NodeLeaseVersion；Directory.Renew 只校验 PlacementVersion 和当前 Node Lease，不推进 TTL。

---

## 五、核心关系

### Grain 与 Placement

```text
Grain 1 -> 0..1 Active Placement
```

说明：

- 一个 Grain 同一时间最多只有一个 Active Placement。
- Grain 可以尚未分配，此时没有 Active Placement。

### Placement 与 Node

```text
Placement 1 -> 1 NodeIdentity
```

说明：

- Placement 指向稳定 NodeIdentity。
- Placement 不直接指向临时进程。
- 运行期进程由 NodeIdentity + NodeSessionID 区分。

### Directory 与 Placement

```text
Directory 1 -> N Placement
```

说明：

- Directory 维护所有 Placement。
- Directory 是唯一真相。
- Directory 必须支持按 GrainKey 和 NodeIdentity 查询。

### NodeLease、NodeSession 与 Placement

```text
NodeSession 1 -> 1 NodeLease
Placement -> OwnerNodeSessionID snapshot
```

说明：

- NodeLease 决定当前 session 关联 Placement 的路由资格。
- NodeLease 到期不逐条改写 Placement；记录保留供显式 Recover 或 Transfer。

### Strategy 与 Node

```text
NodeType + NodeGroup
  -> Candidate Nodes
  -> Remove Invalid NodeName
  -> Effective Nodes
  -> PlacementStrategy
  -> NodeIdentity
```

说明：

- Strategy 只能看到 Effective Nodes。
- Effective Nodes 为空时，Allocate 必须失败。

### Cache 与 Event

```text
Directory
  -> Publish DomainEvent
  -> Node Subscribe Event
  -> Clear LocalPlacementCache
```

说明：

- 缓存依赖事件主动失效。
- 事件丢失或订阅异常时，必须清空缓存或降级回源。

---

## 六、生命周期

### RegisterNode

```text
Node starts
  -> Register(NodeType, NodeGroup, NodeName, NodeSessionID)
  -> create immutable-TTL NodeLease
  -> Node becomes Active
```

注册成功后，节点只是在线。

是否可分配，还要看：

```text
NodeStatus
InvalidNodeGroup
NodeLease validity
```

### Lookup

```text
GrainKey
  -> LocalPlacementCache
  -> Directory
  -> PlacementRoute
```

规则：

- Lookup 无副作用。
- Lookup 不创建 Placement。
- Lookup 不迁移 Placement。
- Lookup 不恢复 Placement。
- 缓存不可信时必须回源 Directory。
- Lookup 同时校验 Owner Node、session、状态和 Node Lease；逻辑失效时返回 NotFound 但保留 Placement。

### Allocate

```text
GrainKey
  -> no Active Placement
  -> Candidate Nodes
  -> Effective Nodes
  -> Strategy
  -> Create Placement
```

规则：

- Allocate 只在没有有效 Placement 时执行。
- Allocate 必须过滤 InvalidNodeGroup。
- Effective Nodes 为空时必须失败。

### Renew

```text
Owner
  -> Renew(NodeIdentity, NodeSessionID, PlacementVersion)
  -> Validate current NodeLease
  -> Audit without changing Placement or TTL
```

规则：

- 必须是当前 Owner。
- NodeIdentity 必须匹配。
- NodeSessionID 必须是当前有效 session。
- PlacementVersion 必须匹配，Node Lease 必须有效。

### Release

```text
Owner
  -> Release(NodeIdentity, NodeSessionID, PlacementVersion)
  -> Placement released
```

规则：

- 必须是当前 Owner session。
- Release 后可以重新 Allocate。
- Release 必须发布事件。

### Transfer

```text
Placement
  -> explicit Transfer
  -> New NodeIdentity
```

规则：

- Transfer 必须是显式命令。
- Lookup 不能触发 Transfer。
- 缩容迁移应先 FindByNode(NodeIdentity)，再逐个 Transfer。
- Transfer 目标必须来自 Effective Nodes。

### Recover

```text
Old Owner unreliable
  -> Recover
  -> New Owner
```

规则：

- Recover 是故障恢复。
- Recover 不等于有计划迁移。
- Recover 只用于 missing、Offline、Lease 到期或 session 不匹配的 Owner，并基于 PlacementVersion 保证唯一性。
- 健康 Owner 必须使用 Transfer；Transfer 也可以显式迁移不可用 Owner。

### MarkNodeInvalid

```text
MarkNodeInvalid(NodeType, NodeGroup, NodeName)
  -> Add NodeName to InvalidNodeGroup
  -> Publish NodeMarkedInvalid
```

规则：

- 只影响新的 Allocate。
- 不自动改变已有 Placement。
- 按 NodeName 生效，跨 NodeSessionID 持续有效。

### RestoreNode

```text
RestoreNode(NodeType, NodeGroup, NodeName)
  -> Remove NodeName from InvalidNodeGroup
  -> Publish NodeRestored
```

规则：

- Restore 后，该 NodeName 可以重新进入 Effective Nodes。
- RestoreNode 必须发布事件。

---

## 七、领域规则

### Rule 1: Grain 唯一归属

同一个 Grain 同一时间最多只有一个 Active Placement。

### Rule 2: Lookup 无副作用

Lookup 只能读取 Placement，不能创建、迁移或恢复 Placement。

### Rule 3: Placement 稳定

正常情况下，Placement 不因为 Actor GC、Actor 回收或普通重启而变化。

### Rule 4: Placement 只能被命令改变

允许改变 Placement 的命令包括：

```text
Allocate
Release
Transfer
Recover
```

Node Lease 到期不改变 Placement；Owner 变化只能由显式 Transfer、Recover、Release 或 Released 后 Allocate 完成。

### Rule 5: Owner 操作必须校验 session

Renew / Release 不能只校验 NodeIdentity。

它们必须同时校验：

```text
NodeIdentity
NodeSessionID
PlacementVersion
Renew: current NodeLease validity
Release: current session identity, even if NodeLease expired
```

### Rule 6: NodeGroup 是扩展边界

默认情况下，Strategy 只能在指定 NodeType + NodeGroup 内选择节点。

跨 Group 迁移必须是显式 Transfer 或更高层策略决策。

### Rule 7: 缓存不是唯一真相

本地缓存只能作为加速层。

Directory 才是 Source of Truth。

### Rule 8: 缓存必须可主动失效

任何会改变 Placement 或节点有效性的操作，都必须能触发缓存失效。

包括：

```text
PlacementTransferred
PlacementReleased
PlacementRecovered
NodeLeaseExpired
NodeDraining
NodeReplaced
NodeUnregistered
NodeMarkedInvalid
NodeRestored
ManualCacheClear
```

### Rule 9: 事件订阅异常必须降级

事件订阅异常时，节点不能继续信任本地缓存。

必须：

```text
Clear LocalPlacementCache
or
Fallback to Directory and disable cache read/write
```

### Rule 10: InvalidNodeGroup 按 NodeName 生效

失效组按 NodeName 生效，不按 NodeSessionID 生效。

同名节点换新的 NodeSessionID，不会自动解除失效状态。

### Rule 11: 失效节点不影响已有 Placement 的确定性

节点进入失效组后，已有 Placement 不应被 Lookup 自动修改。

已有 Placement 只能通过显式命令迁移或释放。

---

## 八、领域事件

Stable Placement 需要产生领域事件：

```text
NodeRegistered
NodeReplaced
NodeDraining
NodeMarkedInvalid
NodeRestored
NodeUnregistered
PlacementCreated
PlacementRenewed
PlacementReleased
PlacementTransferred
PlacementRecovered
NodeLeaseExpired
PlacementCacheInvalidated
ManualCacheClear
```

事件使用原则：

- 事件用于通知和失效，不替代 Directory。
- 收到比本地版本新的事件时，必须清理或更新本地缓存。
- 收到无法判断版本的事件时，必须保守清理缓存。
- 订阅异常、事件缺失、事件无法解析时，必须清空缓存或降级回源。

Memory 与 Redis 对 TTL 配置、错误、到期边界、Register/Replace、Recover/Transfer 和路由资格保持一致；Redis Lease 判断统一使用 Redis TIME。

v1/v2 只能冷切换。部署系统必须在 v1 writer、Node、scanner、consumer 全部停止，Grain 执行停止且缓存清空后，整体启动 v2 workload。`首笔 v2 业务写入` 后禁止直接回滚；该术语与启动控制面写入的边界以 [Node Lease v2 冷切换手册](./node-lease-v2-cutover.md) 为准。

---

## 九、聚合边界

从 DDD 视角看，核心 Aggregate Root 应该是：

```text
Placement
```

不是 Node。

也不是 Grain。

原因：

- 唯一性围绕 Grain -> Node 的关系展开。
- PlacementVersion / Owner session / Status 服务于 Placement 的一致性，NodeLease 服务于路由资格。
- Transfer / Release / Recover 都是在改变 Placement。
- Node 是 Placement 的目标，不是 Placement 本身。
- Grain 是业务实体，不属于 Stable Placement 管理生命周期。

NodeRegistry / InvalidNodeGroup / LocalPlacementCache 是围绕 Placement 协作的领域对象。

它们不应该替代 Placement 成为核心聚合。

---

## 十、最终本体模型

```text
                              +----------------------+
                              |      Directory       |
                              |  Source of Truth     |
                              +----------+-----------+
                                         |
                                         |
                                  manages Placement
                                         |
                                         v
+-----------+       owns        +------------------+       points to      +-----------+
|   Grain   |------------------>|    Placement     |-------------------->|   Node    |
+-----------+                   +---------+--------+                     +-----+-----+
                                          |                                    |
                                          | owner session snapshot             |
                                          +------------------------------------+
                                                                              |
                                                                              v
                                                                       +---------------+
                                                                       | Node Registry |
                                                                       +-------+-------+
                                                                              |
                                                                       owns NodeLease
                                                                              |
                                                                       filters by
                                                                               |
                                                                               v
+-------------------+      invalidates       +----------------------+   +------------------+
|  Domain Event     |----------------------->| LocalPlacementCache  |   | InvalidNodeGroup |
+-------------------+                        +----------------------+   +------------------+
          ^
          |
          |
   emitted by commands
          |
          v
  Allocate / Renew / Release / Transfer / Recover / MarkNodeInvalid / RestoreNode
```

---

## 十一、实现映射

本体模型和实现方式的关系：

- MemoryDirectory / RedisDirectory / EtcdDirectory / DatabaseDirectory 都只是 Directory 的实现。
- Redis Pub/Sub / etcd watch / MQ / stream 都只是 DomainEvent 的传播方式。
- protoactor-go 只是 Actor Runtime 和 Remote 通信层，不是 Stable Placement 的领域核心。
- LocalPlacementCache 是性能优化，不改变 Directory 的唯一真相地位。
- PlacementStrategy 是分配算法，不负责 Lookup，也不负责自动 Rebalance。

因此，Stable Placement 的演进顺序应该是：

```text
先稳定领域模型
再定义接口
再实现 MemoryDirectory
再替换或扩展底层存储和事件通道
```
