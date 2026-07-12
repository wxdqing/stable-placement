# stable-placement 节点视角 BDD Checklist

Redis 默认：`127.0.0.1:6379`（`STABLE_PLACEMENT_REDIS_ADDR` 可覆盖；密码只通过 `STABLE_PLACEMENT_REDIS_PASSWORD` 注入）

运行：

```bash
STABLE_PLACEMENT_REDIS_ADDR=127.0.0.1:6379 \
  STABLE_PLACEMENT_REDIS_PASSWORD="$STABLE_PLACEMENT_REDIS_PASSWORD" \
  go test -tags=integration ./example/node-bdd/ -v
```

组织方式：以 `game/default` 节点集群为主线，覆盖扩容、缩容及当前所有领域规则。

---

## A. 节点集群基础

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| A1 | RegisterNode 后 FindNodes 可列出 game 节点 | NodeRegistry | `cluster_test.go` | [x] |
| A2 | 多节点注册后列表按 NodeIdentity 稳定排序 | FindNodes | `cluster_test.go` | [x] |
| A3 | RenewNode 续约 Node Lease，错误或过期 session 被拒绝 | NodeRegistry | `cluster_test.go` | [x] |

## B. 扩容（Scale Up）

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| B1 | 初始 1 节点 Allocate 后归属该节点 | Allocate | `scale_up_test.go` | [x] |
| B2 | 新增 game-2/game-3 后 FindNodes 可见扩容节点 | 扩容 | `scale_up_test.go` | [x] |
| B3 | 扩容后新 Allocate 可使用新节点（RoundRobin） | Strategy | `scale_up_test.go` | [x] |
| B4 | 扩容不自动迁移已有 Placement（Rule 3/11） | Placement 稳定 | `scale_up_test.go` | [x] |
| B5 | 并发 Allocate 同一 Grain 仅一个 Active（Rule 1） | 唯一归属 | `scale_up_test.go` | [x] |

## C. 缩容（Scale Down）

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| C1 | MarkNodeInvalid 后新 Allocate 不选该 NodeName | InvalidNodeGroup | `scale_down_test.go` | [x] |
| C2 | MarkNodeInvalid 后已有 Placement Lookup 不变 | Rule 11 | `scale_down_test.go` | [x] |
| C3 | InvalidNodeGroup 跨 NodeSessionID 持续（Rule 10） | session 替换后仍无效 | `scale_down_test.go` | [x] |
| C4 | DrainNode 前未 MarkNodeInvalid 必须失败 | 缩容流程 | `scale_down_test.go` | [x] |
| C5 | DrainNode 后节点 Status=draining，不参与 Allocate | NodeDraining | `scale_down_test.go` | [x] |
| C6 | FindByNode 分页列出待迁移 Placement | 缩容迁移 | `scale_down_test.go` | [x] |
| C7 | 逐个 Transfer 后 FindByNode 为空 | 显式迁移 | `scale_down_test.go` | [x] |
| C8 | Placement 迁走后 UnregisterNode 成功下线 | CompleteDrain | `scale_down_test.go` | [x] |
| C9 | RestoreNode 后节点重新参与 Allocate | RestoreNode | `scale_down_test.go` | [x] |

## D. Placement 命令

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| D1 | Lookup 未分配返回 NotFound，不创建 Placement | Rule 2 | `placement_test.go` | [x] |
| D2 | Lookup 已分配返回 Active，与 Allocate 一致 | Lookup | `placement_test.go` | [x] |
| D3 | Renew 校验 Owner/session/version（Rule 5） | Renew | `placement_test.go` | [x] |
| D4 | 旧 session / 非 Owner Renew 失败 | Rule 5 | `placement_test.go` | [x] |
| D5 | Release 后 Lookup NotFound，可重新 Allocate | Release | `placement_test.go` | [x] |
| D6 | 健康 Owner 拒绝 Recover，Placement 保持不变 | Recover 语义 | `placement_test.go` | [x] |
| D7 | Transfer 显式更换 Owner，推进 Version | Transfer | `placement_test.go` | [x] |
| D8 | Node Lease 到期后同节点多条 Route 逻辑失效，Placement 保留 | Node Lease | `placement_test.go` | [x] |
| D9 | Node Lease 到期后同 session 仍可 Release 保留的 Placement | Release | `placement_test.go` | [x] |
| D10 | Exists 仅对 Active Placement 返回 true | Exists | `placement_test.go` | [x] |

## E. Session 与节点替换

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| E1 | ReplaceNodeSession 后旧 session Renew 失败 | NodeReplaced | `session_test.go` | [x] |
| E2 | ReplaceNodeSession 后新 session 不继承旧 Placement | NodeReplaced | `session_test.go` | [x] |
| E3 | UnregisterNode 错误 session 失败 | NodeRegistry | `session_test.go` | [x] |

## F. 边界与负向

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| F1 | 无可用节点时 Allocate 返回 NoAvailableNode | Allocate | `negative_test.go` | [x] |
| F2 | 全部节点 Invalid 时 Allocate 失败 | InvalidNodeGroup | `negative_test.go` | [x] |
| F3 | Transfer 到无效/ draining 节点失败 | Transfer | `negative_test.go` | [x] |
| F4 | Version 冲突时 Renew/Release 失败 | 并发校验 | `negative_test.go` | [x] |
| F5 | Owner 不可用时 Allocate 不自动重分配 | Allocate | `negative_test.go` | [x] |

## G. ResolveRoute Owner 生命周期（Memory + Redis）

以下每个场景均由同一公共 API 测试分别驱动 Memory 和真实 Redis backend。

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| G1 | 首次请求在目标组健康节点 Allocate | ResolveRoute | `route_resolve_test.go` | [x] |
| G2 | 100 个并发请求只得到一个 Owner Session | 原子唯一归属 | `route_resolve_test.go` | [x] |
| G3 | 扩容后已有 Owner 不变，新 Grain 可分配到新节点 | 稳定放置 | `route_resolve_test.go` | [x] |
| G4 | 同 NodeIdentity 新 Session 自动 Recover 并推进 Version | Session Recover | `route_resolve_test.go` | [x] |
| G5 | Owner Offline 且未 Invalid 时返回 OwnerUnavailable，Placement 保留 | 人工迁移边界 | `route_resolve_test.go` | [x] |
| G6 | 人工 Invalid 后 Recover 到同组其他节点 | Invalid Recover | `route_resolve_test.go` | [x] |
| G7 | healthy+Invalid 不隐式迁移，显式 Transfer 才迁移 | 显式迁移 | `route_resolve_test.go` | [x] |
| G8 | 请求目标组变化返回 TargetMismatch | 分组隔离 | `route_resolve_test.go` | [x] |

---

## 可靠性保障

- 每个场景独立 `nodeGroup`（`nbdd-{runID}`），避免共享 Redis 状态串扰
- 场景结束 `cleanup`：Release Placement → UnregisterNode → RestoreNode；清理失败会使测试失败
- `integration` gate 下 Redis 不可达必须失败，不允许 Skip
- 关键断言使用 `errors.Is` 校验领域错误
- 缩容全流程按 ontology 推荐顺序执行并逐步断言

## 验收

- [x] 2026-07-13：真实 Redis 7.0.15，`-race -tags=integration ./example/node-bdd`，原 34 个 top-level 场景及 ResolveRoute 8 场景 x 2 backend 通过，0 Skip

## Node Lease v2 部署门禁

- [ ] v1 writer、Node、scanner、consumer 全部停止，旧 Grain 执行停止且 Placement 缓存清空。
- [ ] v1 key 预置不影响 v2；检查 `redis.NamespaceVersion` / `redis.NamespacePrefix`，本库不探测其他进程。
- [ ] 全部 v2 workload 一次性启动并健康后才开放流量。
- [ ] `首笔 v2 业务写入` 后禁止直接回滚；定义和完整步骤以 [`docs/node-lease-v2-cutover.md`](../../docs/node-lease-v2-cutover.md) 为准。
- [ ] 稳定后由运维人工清理 v1 key。
