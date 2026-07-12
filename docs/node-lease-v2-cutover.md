# Stable Placement Node Lease v2 冷切换手册

## 适用范围

本手册是 v1 到 v2 的唯一受支持切换流程。v1 和 v2 namespace 相互隔离，但两个版本同时接受业务流量仍会让同一个 Grain 产生两个 Owner，因此禁止滚动升级、双写或混部运行。

本库只暴露 `redis.NamespaceVersion` 和 `redis.NamespacePrefix` 常量供部署系统检查，不探测其他进程、workload 或 v1 key。停机确认、流量门禁和清理由部署与运维系统负责。

## 切换前门禁

- 确认制品的 `redis.NamespaceVersion == "v2"`。
- 确认制品的 `redis.NamespacePrefix == "sp:{stable-placement}:v2:"`。
- 在验收 Redis 中预置 v1 key，证明 v2 不读取、修改或删除它们。
- 准备 v1 只读回退窗口；窗口内保留 v1 key，但禁止任何 v1 写入。
- 未取得所有 v1 workload 已停止的证据前，部署系统必须拒绝开放 v2 业务流量。

## 冷切换步骤

1. 停止全部 v1 writer、Node 实例、后台 scanner 和 Stream consumer。确认没有任何 v1 组件继续写 Redis 或处理业务。
2. 由业务侧确认所有旧 Grain 执行已经停止。清空所有进程内 Placement 缓存，防止旧 route 在切换后继续使用。
3. 保留 v1 Redis key 进入只读回退窗口。不得恢复 v1 writer、Node、scanner 或 consumer，也不得让 v1 接受业务流量。
4. 一次性启动全部 v2 writer、Node、Node Lease scanner 和 Stream consumer，不做滚动混部。检查 Node Lease 注册与扫描、Stream continuity、pending 处理和缓存从 degraded 恢复；确认全部通过后才开放 v2 业务流量。
5. 开放流量并记录 `首笔 v2 业务写入` 时间。`首笔 v2 业务写入` 发生后禁止直接回滚到 v1，失败时优先前向修复。
6. v2 稳定运行且确认不再需要回退后，由运维人工清理 v1 key。本库不提供自动迁移或清理工具。

## 首笔 v2 业务写入定义

`首笔 v2 业务写入` 是任何承载真实业务状态的 Allocate、Renew、Release、Transfer、Recover 或其对应领域/审计写入，不以是否已经开放流量为判断条件。启动阶段的 Node 注册、RenewNode、Node Lease 扫描和 consumer group 管理属于控制面写入，不计入该边界。经批准的合成验收数据只有在隔离标识明确、验收后完整清理且确认未承载真实业务状态时，才不计入该边界。

所有引用回退边界的文档均以本定义为准。

## 回退边界

在 `首笔 v2 业务写入` 之前，可以在保持 v1 key 只读且确认尚未发生 `首笔 v2 业务写入` 的前提下，停止全部 v2 writer、Node、scanner 和 consumer，清空启动阶段产生的 v2 namespace 控制面状态，再按部署流程恢复 v1。再次尝试 v2 切换时必须从空的 v2 namespace 重新开始。

在 `首笔 v2 业务写入` 之后不允许直接回滚。若必须恢复 v1，先停止全部 v2 writer、Node、scanner 和 consumer，停止 Grain 执行并清空缓存；随后人工核对 v1/v2 业务状态，显式清空 v2 namespace，完成审批后才可恢复 v1。这是一次新的冷切换，不是直接回滚。

## 开放流量验收

- [ ] 所有 v1 writer、Node、scanner、consumer 均已停止。
- [ ] 所有旧 Grain 执行均已停止。
- [ ] 所有进程内 Placement 缓存均已清空。
- [ ] v1 key 保留只读，没有写入方。
- [ ] v1 key 预置不影响 v2 读写结果。
- [ ] namespace/version 常量与 v2 预期一致。
- [ ] 全部 v2 writer、Node Lease scanner 和 consumer 已启动并健康。
- [ ] 全部 v2 Node Lease 注册成功。
- [ ] Stream continuity 和 pending 状态正常。
- [ ] 缓存已退出 degraded，且未使用过期 route。
- [ ] 部署系统允许开放流量。

## 证据与清理

部署记录至少保留：v1 workload 停止清单、Grain 停止确认、缓存清理记录、namespace/version 常量检查、v1 key 隔离测试、v2 workload 健康结果、开放流量时间和 `首笔 v2 业务写入` 时间。

v1 key 只能在 v2 稳定、回退窗口关闭且负责人批准后人工清理。清理前后记录 key 数量和操作人，不把清理职责下沉到 stable-placement 库。
