# 入仓关系契约子集

值类型long/string/bool严格匹配，long不接收bool（bool也不接收long/string）。候选row_id全批唯一，表中primary必选非null且主键列必须required。unique键采用NULLS DISTINCT：含任一null的unique元组不冲突；主键不允许null。每个unique冲突组的全部新行隔离，旧有效行不隔离或更改；冲突证据列对应全部row_id和已有键。空串`""`是普通string值，不是null，照常参与查找与冲突判定。

foreign_keys目标columns必须精确匹配已登记primary或unique，MATCH仅支持SIMPLE：外键元组任一列为null即整体跳过（不是查询时等null），全部非null才查找目标。已有有效库可作为目标，剩余候选可彼此作目标。

## 最大闭合集合（顺序无关）

计算分两阶段，纯函数、与候选输入顺序无关，结果唯一：

1. 波次0a隔离本地非法行（未知列、必填缺失/null、类型不符）；
2. 波次0b隔离唯一键冲突行：同批同键冲突组全部隔离，与旧有效表冲突只隔离新行；
3. 外键不动点：每轮基于“波次开始时的存活快照”判定，引用目标不在旧有效表、也不在存活候选中的行移出，直到某轮无移出。

最大闭集合从通过本地/unique的新行全集单调删除无目标者，因此合法A1/B2环保留（同波互引视为有效，不要求从已有父表起步），A3缺B999、B4/C5引用A3要传递隔离。隔离行保留在候选键索引中但不在存活快照，使“父行已隔离（传播边FK_PARENT_QUARANTINED）”与“目标根本不存在（FK_MISSING_TARGET）”可区分；同键冲突组在传播证据`conflict_group_row_ids`中列全组成员。

## 原因码与根因链

- `LOCAL_INVALID`：本地类型/非空/未知列，终端原因；
- `UK_CONFLICT`：唯一键（含主键）冲突，终端原因；
- `FK_MISSING_TARGET`：该行直接引用了任何表都不存在的目标键，终端原因；
- `FK_PARENT_QUARANTINED`：引用的父候选在先前波次被隔离，传播原因。

原因图中直接missing先定位不存在的目标键，传播边引用已隔离父行及根因；波次严格递增，DFS沿传播边回溯必然终止在LOCAL_INVALID/UK_CONFLICT/真正缺目标，不能生成没有终点的理由环（代码含环防御断言）。同一坏父沿多张表传播时每条路径都保留可解释根因集合。

## 发布策略与事务

atomic模式和closed-subset只影响发布策略，不改变计算所得闭集和隔离原因：

- atomic：任一隔离即整批不发布，回执`REJECTED_ATOMIC`、published_count=0（隔离区/原因边/回执仍同事务落库）；
- closed-subset：只发布最大闭合集合，全量接受时`PUBLISHED_FULL`，有隔离时`PUBLISHED_CLOSED_SUBSET`。

SQLite单事务写正式表、`_gw_quarantine`隔离区、`_gw_reason_edges`原因边、`_gw_batch_candidates`候选原文与`_gw_batches`批回执。提交先`BEGIN IMMEDIATE`取写锁，校验当前data_version等于计划基准版本，再在锁内用当前库重新解析清单并重算闭包；plan_digest不一致即抛StaleVersionError回滚，绝不用旧结果覆盖。批ID不可复用，data_version每提交一批递增。隔离区保存候选原始values，门禁不修复、不改写隔离数据。

## 独立检查器

`warehouse_data.checker`与引擎零代码共享，从落库事实独立重算闭包，逐条核对：接受/隔离集合与重算一致、每条接受行引用可达、每条隔离行原文未改写且根因链终止在终端原因、原因边波次严格递减无环、模式策略与回执计数一致、正式表无未登记批次写入或悬空外键、回执自摘要成立。
