# stable-placement examples

## BDD（Redis 集成验收）

目录 `example/bdd/` 提供面向真实 Redis 的 BDD 风格验收测试，默认连接 `127.0.0.1:16379`。

### 前置条件

- Redis 已监听 `16379`（本地或开发机）
- 可使用共享 Redis；每个场景使用独立 run id，结束后清理本场景写入的节点与 Placement

### 运行

在 `libs/stable-placement` 目录：

```bash
go test -tags=integration ./example/bdd/ -v
```

指定 Redis 地址（与项目 dev 配置一致时）：

```bash
STABLE_PLACEMENT_REDIS_ADDR=172.16.4.129:16379 go test -tags=integration ./example/bdd/ -v
```

从 monorepo 根目录：

```bash
go test -tags=integration ./libs/stable-placement/example/bdd/ -v
```

### 场景清单

| 场景 | 验证点 |
|------|--------|
| 注册节点后分配 Placement | Allocate + Lookup 归属稳定 |
| Owner 续约 | Renew 推进 LeaseVersion |
| 释放后禁止 Recover | Release → Recover 返回 `ErrPlacementNotRecoverable` |
| 过期后可 Recover | Expire → Recover 恢复 Active |
| 失效节点不参与新分配 | MarkNodeInvalid 后 Allocate 跳过该 NodeName |
| 显式迁移 | Transfer 更换 Owner |
| 幂等分配 | 同一 Grain 并发/重复 Allocate 仅一个 Active Placement |

---

## 节点视角 BDD（完整规则验收）

目录 `example/node-bdd/` 以 `game` 节点集群为主线，按 **CHECKLIST.md** 驱动，覆盖扩容、缩容及当前所有领域规则。

```bash
go test -tags=integration ./example/node-bdd/ -v
```

详见 [example/node-bdd/CHECKLIST.md](./node-bdd/CHECKLIST.md)。
