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
| D6 | Release 后 Recover 返回 NotRecoverable | Recover 语义 | `placement_test.go` | [x] |
| D7 | Transfer 显式更换 Owner，推进 Version | Transfer | `placement_test.go` | [x] |
| D8 | Node Lease 到期后 Lookup NotFound，Placement 保留并可 Recover | Node Lease/Recover | `placement_test.go` | [x] |
| D9 | ExpireNodeLeases 只推进 Node Offline，不改写 Placement | Node Lease | `placement_test.go` | [x] |
| D10 | Exists 仅对 Active Placement 返回 true | Exists | `placement_test.go` | [x] |

## E. Session 与节点替换

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| E1 | ReplaceNodeSession 后旧 session Renew 失败 | NodeReplaced | `session_test.go` | [x] |
| E2 | ReplaceNodeSession 后旧 session Release 失败 | Rule 5 | `session_test.go` | [x] |
| E3 | UnregisterNode 错误 session 失败 | NodeRegistry | `session_test.go` | [x] |

## F. 边界与负向

| ID | 场景 | 规则来源 | 测试文件 | 状态 |
|----|------|----------|----------|------|
| F1 | 无可用节点时 Allocate 返回 NoAvailableNode | Allocate | `negative_test.go` | [x] |
| F2 | 全部节点 Invalid 时 Allocate 失败 | InvalidNodeGroup | `negative_test.go` | [x] |
| F3 | Transfer 到无效/ draining 节点失败 | Transfer | `negative_test.go` | [x] |
| F4 | Version 冲突时 Renew/Release 失败 | 并发校验 | `negative_test.go` | [x] |

---

## 可靠性保障

- 每个场景独立 `nodeGroup`（`nbdd-{runID}`），避免共享 Redis 状态串扰
- 场景结束 `cleanup`：Release Placement → UnregisterNode → RestoreNode；清理失败会使测试失败
- Redis 不可达时 `t.Skip`，不污染 CI 单元测试
- 关键断言使用 `errors.Is` 校验领域错误
- 缩容全流程按 ontology 推荐顺序执行并逐步断言

## 验收

- [x] `go test -tags=integration ./example/node-bdd/ -v` 全部通过（31 场景）

## Node Lease v2 部署门禁

- [ ] v1 writer、Node、scanner、consumer 全部停止，旧 Grain 执行停止且 Placement 缓存清空。
- [ ] v1 key 预置不影响 v2；检查 `redis.NamespaceVersion` / `redis.NamespacePrefix`，本库不探测其他进程。
- [ ] 全部 v2 workload 一次性启动并健康后才开放流量。
- [ ] `首笔 v2 业务写入` 后禁止直接回滚；定义和完整步骤以 [`docs/node-lease-v2-cutover.md`](../../docs/node-lease-v2-cutover.md) 为准。
- [ ] 稳定后由运维人工清理 v1 key。
