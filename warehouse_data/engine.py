"""批级关系契约门禁引擎（纯计算，不触碰存储）。

两阶段隔离，顺序无关、结果唯一：
1. 本地非法（类型/非空/未知列）与唯一键冲突（NULLS DISTINCT，冲突组全隔离）；
2. 反复隔离引用目标不在旧有效表或剩余候选中的行，直到不动点。
合法环因每轮基于剩余集合快照判定而保留；原因边随波次严格指向前序波次，
根因必然终止在本地错误、唯一冲突或真正缺目标，不可能出现理由环。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .contract import Contract, TableContract, parse_contract
from .io import TYPE_PREDICATES, canonical, digest_json

LOCAL_INVALID = "LOCAL_INVALID"
UK_CONFLICT = "UK_CONFLICT"
FK_MISSING_TARGET = "FK_MISSING_TARGET"
FK_PARENT_QUARANTINED = "FK_PARENT_QUARANTINED"
TERMINAL_REASONS = frozenset({LOCAL_INVALID, UK_CONFLICT, FK_MISSING_TARGET})
MODES = ("atomic", "closed-subset")


class ManifestError(ValueError):
    """清单形状不合法（身份、模式、表归属等输入级错误）。"""


class BaselineViolation(ValueError):
    """既有仓库行不满足契约；前提声明既有版本已满足契约，出现即拒绝继续。"""


@dataclass(frozen=True)
class QuarantineRecord:
    row_id: str
    table: str
    wave: int
    reason: str
    details: list[dict[str, Any]]
    values: dict[str, Any]


@dataclass(frozen=True)
class ReasonEdge:
    source_row_id: str
    source_table: str
    fk_columns: tuple[str, ...]
    target_table: str
    target_columns: tuple[str, ...]
    key_tuple: list[Any]
    reason: str
    target_row_id: str | None
    wave: int


@dataclass
class Plan:
    batch_id: str
    mode: str
    base_version: str
    contract_digest: str
    candidate_digest: str
    plan_digest: str
    accepted: list[dict[str, Any]] = field(default_factory=list)
    quarantined: list[QuarantineRecord] = field(default_factory=list)
    edges: list[ReasonEdge] = field(default_factory=list)
    roots: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    waves: dict[str, int] = field(default_factory=dict)
    passes: list[dict[str, Any]] = field(default_factory=list)

    @property
    def accepted_ids(self) -> frozenset[str]:
        return frozenset(r["row_id"] for r in self.accepted)

    @property
    def quarantined_ids(self) -> frozenset[str]:
        return frozenset(r.row_id for r in self.quarantined)

    def to_receipt(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "mode": self.mode,
            "base_version": self.base_version,
            "contract_digest": self.contract_digest,
            "candidate_digest": self.candidate_digest,
            "plan_digest": self.plan_digest,
            "accepted_row_ids": sorted(self.accepted_ids),
            "quarantined_row_ids": sorted(self.quarantined_ids),
            "quarantine_reasons": [
                {"row_id": q.row_id, "table": q.table, "wave": q.wave, "reason": q.reason}
                for q in self.quarantined
            ],
            "root_causes": self.roots,
            "fixpoint_passes": self.passes,
        }


def parse_manifest(v: dict[str, Any]) -> tuple[Contract, dict[str, list[dict[str, Any]]], list[dict[str, Any]], str, str, str, str]:
    """校验清单形状并解析契约，返回 (contract, existing, candidates, mode, batch_id, warehouse_version, manifest_digest占位由调用方补)。"""
    if v.get("kind") != "relational-contract":
        raise ManifestError("kind 必须是 relational-contract")
    contract = parse_contract(v.get("contracts"))
    mode = v.get("mode")
    if mode not in MODES:
        raise ManifestError(f"模式未知: {mode!r}，仅支持 {MODES}")
    for name in ("batch_id", "warehouse_version", "contract_version"):
        if not isinstance(v.get(name), str) or not v[name]:
            raise ManifestError(f"{name} 必须是非空字符串")
    raw_existing = v.get("existing")
    if not isinstance(raw_existing, dict):
        raise ManifestError("existing 必须是对象")
    existing: dict[str, list[dict[str, Any]]] = {}
    for tc in contract.tables:
        rows = raw_existing.get(tc.table, [])
        if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
            raise ManifestError(f"existing.{tc.table} 必须是行对象数组")
        existing[tc.table] = rows
    extra_tables = set(raw_existing) - set(contract.by_table)
    if extra_tables:
        raise ManifestError(f"existing 含未登记表: {sorted(extra_tables)}")
    raw_candidates = v.get("candidates")
    if not isinstance(raw_candidates, list):
        raise ManifestError("candidates 必须是数组")
    candidates: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for i, row in enumerate(raw_candidates):
        if not isinstance(row, dict):
            raise ManifestError(f"候选 {i} 必须是对象")
        rid = row.get("row_id")
        if not isinstance(rid, str) or not rid:
            raise ManifestError(f"候选 {i} row_id 必须是非空字符串")
        if rid in seen_ids:
            raise ManifestError(f"候选 row_id 全批必须唯一，重复: {rid}")
        seen_ids.add(rid)
        table = row.get("table")
        if table not in contract.by_table:
            raise ManifestError(f"候选 {rid} 引用未登记表: {table!r}")
        values = row.get("values")
        if not isinstance(values, dict):
            raise ManifestError(f"候选 {rid} values 必须是对象")
        candidates.append({"row_id": rid, "table": table, "values": values})
    return contract, existing, candidates, mode, v["batch_id"], v["warehouse_version"], v["contract_version"]


def _local_issues(tc: TableContract, values: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    known = tc.column_map
    for name in sorted(values):
        if name not in known:
            issues.append(f"unknown_column:{name}")
    for col in tc.columns:
        if col.name not in values:
            if col.required:
                issues.append(f"required_missing:{col.name}")
            continue
        x = values[col.name]
        if x is None:
            if col.required:
                issues.append(f"required_null:{col.name}")
            continue
        if not TYPE_PREDICATES[col.type](x):
            issues.append(f"type_mismatch:{col.name}:expected_{col.type}")
    return issues


def _key_tuple(values: dict[str, Any], cols: tuple[str, ...]) -> tuple[Any, ...] | None:
    """NULLS DISTINCT：任一列 null（含缺列）则该键元组不参与冲突/不成为有效目标。"""
    out: list[Any] = []
    for c in cols:
        x = values.get(c)
        if x is None:
            return None
        out.append(x)
    return tuple(out)


def _baseline_row_id(table: str, idx: int) -> str:
    return f"existing:{table}#{idx}"


def _build_baseline_indexes(
    contract: Contract, existing: dict[str, list[dict[str, Any]]]
) -> tuple[dict[tuple[str, str], dict[tuple[Any, ...], str]], list[dict[str, Any]]]:
    """旧有效表按登记键建索引；同时核验旧库确实满足契约前提。"""
    index: dict[tuple[str, str], dict[tuple[Any, ...], str]] = {}
    baseline_checks: list[dict[str, Any]] = []
    for tc in contract.tables:
        for key in tc.registered_keys:
            index[(tc.table, key)] = {}
        for idx, values in enumerate(existing.get(tc.table, [])):
            rid = _baseline_row_id(tc.table, idx)
            issues = _local_issues(tc, values)
            if issues:
                raise BaselineViolation(f"旧库行 {rid} 不满足本地契约: {issues}")
            for key in tc.registered_keys:
                tup = _key_tuple(values, key)
                if tup is None:
                    if key == tc.primary:
                        raise BaselineViolation(f"旧库行 {rid} 主键为 null")
                    continue
                slot = index[(tc.table, key)]
                if tup in slot:
                    raise BaselineViolation(
                        f"旧库表 {tc.table} 登记键 {list(key)} 重复: {tup!r}（{slot[tup]} 与 {rid}）"
                    )
                slot[tup] = rid
            baseline_checks.append({"row_id": rid, "table": tc.table, "values": values})
    return index, baseline_checks


def plan_batch(
    contract: Contract,
    candidates: list[dict[str, Any]],
    existing: dict[str, list[dict[str, Any]]],
    mode: str,
    *,
    batch_id: str,
    base_version: str,
) -> Plan:
    """求可共同接受的最大闭合集合；纯函数，同输入必同输出，与候选顺序无关。"""
    if mode not in MODES:
        raise ManifestError(f"模式未知: {mode!r}")
    candidate_digest = digest_json(sorted((c["row_id"], c["table"], c["values"]) for c in candidates))
    baseline_index, _ = _build_baseline_indexes(contract, existing)

    quarantine: dict[str, QuarantineRecord] = {}
    waves: dict[str, int] = {}
    edges: list[ReasonEdge] = []

    # ---- 波次 0a：本地非法隔离（原样保留 values，绝不修复数据）----
    survivors: dict[str, dict[str, Any]] = {}
    for cand in candidates:
        tc = contract.by_table[cand["table"]]
        issues = _local_issues(tc, cand["values"])
        if issues:
            quarantine[cand["row_id"]] = QuarantineRecord(
                cand["row_id"], cand["table"], 0, LOCAL_INVALID,
                [{"issues": sorted(issues)}], cand["values"],
            )
            waves[cand["row_id"]] = 0
        else:
            survivors[cand["row_id"]] = cand

    # ---- 波次 0b：唯一键冲突，同批/与旧库冲突的整组一起隔离，不按顺序选第一条 ----
    # 候选侧索引只纳入本地合法行；NULLS DISTINCT 元组才入索引。
    candidate_index: dict[tuple[str, str], dict[tuple[Any, ...], list[str]]] = {}
    for tc in contract.tables:
        for key in tc.registered_keys:
            candidate_index[(tc.table, key)] = {}
    for rid in sorted(survivors):
        cand = survivors[rid]
        tc = contract.by_table[cand["table"]]
        for key in tc.registered_keys:
            tup = _key_tuple(cand["values"], key)
            if tup is None:
                continue
            candidate_index[(tc.table, key)].setdefault(tup, []).append(rid)

    uk_evictions: dict[str, list[dict[str, Any]]] = {}
    for (table, key), groups in candidate_index.items():
        for tup, rids in groups.items():
            evidence: dict[str, Any] = {
                "table": table, "key": list(key), "key_tuple": list(tup),
                "candidate_row_ids": sorted(rids), "baseline_row_id": None,
            }
            bad = False
            if len(rids) > 1:
                bad = True  # 同批冲突组：全部隔离
            old = baseline_index[(table, key)].get(tup)
            if old is not None:
                bad = True  # 与旧有效表冲突：只隔离新行，旧行不动
                evidence["baseline_row_id"] = old
            if bad:
                for rid in rids:
                    uk_evictions.setdefault(rid, []).append(evidence)
    for rid in sorted(uk_evictions):
        cand = survivors.pop(rid)
        quarantine[rid] = QuarantineRecord(
            rid, cand["table"], 0, UK_CONFLICT,
            sorted(uk_evictions[rid], key=lambda e: canonical(e)), cand["values"],
        )
        waves[rid] = 0

    # ---- 外键不动点：每轮快照剩余集合，缺目标/父行已隔离者移出 ----
    # 最终态目标解析（证据生成用）；波次循环内另用带同轮失效判断的 resolve_target。
    def target_exists(table: str, key: tuple[str, ...], tup: tuple[Any, ...]) -> str | None:
        old = baseline_index[(table, key)].get(tup)
        if old is not None:
            return old
        slot = candidate_index[(table, key)].get(tup)
        return slot[0] if slot else None

    # UK 隔离行保留在候选索引中：外键波次靠“命中但不在存活快照”识别传播边，
    # 否则引用冲突行的子行会被误报成“目标不存在”，根因链无法落到 UK_CONFLICT。

    pass_log: list[dict[str, Any]] = []
    wave_no = 0
    while True:
        wave_no += 1
        snapshot = sorted(survivors)  # 顺序无关：每轮对“波次开始快照”判定
        snapshot_set = frozenset(survivors)
        newly: dict[str, list[ReasonEdge]] = {}

        def resolve_target(table: str, key: tuple[str, ...], tup: tuple[Any, ...]) -> str | None:
            old = baseline_index[(table, key)].get(tup)
            if old is not None:
                return old
            # 候选索引保留已隔离行：命中后由调用方按快照集合区分
            # “同波/存活目标（合法环有效）”与“先前波次已隔离父行（传播边）”。
            slot = candidate_index[(table, key)].get(tup)
            return slot[0] if slot else None

        for rid in snapshot:
            cand = survivors[rid]
            tc = contract.by_table[cand["table"]]
            row_edges: list[ReasonEdge] = []
            for fk in tc.foreign_keys:
                tup = _key_tuple(cand["values"], fk.columns)
                if tup is None:
                    continue  # MATCH SIMPLE：任一列 null 即跳过；空串不是 null，照常查找
                target_rid = resolve_target(fk.target_table, fk.target_columns, tup)
                if target_rid is None:
                    row_edges.append(ReasonEdge(
                        rid, tc.table, fk.columns, fk.target_table, fk.target_columns,
                        list(tup), FK_MISSING_TARGET, None, wave_no,
                    ))
                elif target_rid.startswith("existing:"):
                    continue  # 目标在旧有效表
                elif target_rid in snapshot_set:
                    continue  # 同波存活候选（含相互引用的合法环）
                else:
                    # 目标候选在先前波次已隔离：传播边，根因链指向该父行
                    row_edges.append(ReasonEdge(
                        rid, tc.table, fk.columns, fk.target_table, fk.target_columns,
                        list(tup), FK_PARENT_QUARANTINED, target_rid, wave_no,
                    ))
            if row_edges:
                newly[rid] = row_edges
        pass_log.append({"wave": wave_no, "active_before": len(snapshot), "evicted": sorted(newly)})
        if not newly:
            break
        for rid, row_edges in newly.items():
            cand = survivors.pop(rid)
            edges.extend(row_edges)
            waves[rid] = wave_no
            # 主原因按该行实际边类型：自身直接缺目标才是终端，纯传播行保持传播码，
            # 根因由原因边继续向下追，直到 LOCAL_INVALID / UK_CONFLICT / 真正缺目标。
            primary = FK_MISSING_TARGET if any(e.reason == FK_MISSING_TARGET for e in row_edges) else FK_PARENT_QUARANTINED
            details = []
            for e in row_edges:
                item: dict[str, Any] = {
                    "fk_columns": list(e.fk_columns), "target_table": e.target_table,
                    "target_columns": list(e.target_columns), "key_tuple": e.key_tuple,
                    "reason": e.reason, "target_row_id": e.target_row_id,
                }
                if e.reason == FK_PARENT_QUARANTINED and e.target_row_id is not None:
                    peer_slot = candidate_index[(e.target_table, e.target_columns)].get(tuple(e.key_tuple), ())
                    peers = sorted(r for r in peer_slot if r in quarantine)
                    if len(peers) > 1:
                        item["conflict_group_row_ids"] = peers
                details.append(item)
            quarantine[rid] = QuarantineRecord(rid, cand["table"], wave_no, primary, details, cand["values"])
            # 注意：隔离行不从候选索引删除——下一波要靠“索引命中但不在快照集合”
            # 区分“父行已隔离（传播边）”与“目标根本不存在（缺目标边）”。

    # ---- 根因链：波次严格递减，DFS 必终止，杜绝循环理由 ----
    edges_by_source: dict[str, list[ReasonEdge]] = {}
    for e in edges:
        edges_by_source.setdefault(e.source_row_id, []).append(e)

    def find_roots(rid: str) -> list[dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}

        def walk(x: str, trail: tuple[str, ...]) -> None:
            q = quarantine.get(x)
            if q is not None and q.reason in (LOCAL_INVALID, UK_CONFLICT):
                found[x] = {"row_id": x, "table": q.table, "reason": q.reason, "details": q.details}
                return
            out_edges = edges_by_source.get(x, [])
            terminal_missing = [e for e in out_edges if e.reason == FK_MISSING_TARGET]
            if terminal_missing:
                found[x] = {
                    "row_id": x, "table": quarantine[x].table, "reason": FK_MISSING_TARGET,
                    "details": [
                        {"target_table": e.target_table, "target_columns": list(e.target_columns),
                         "key_tuple": e.key_tuple}
                        for e in terminal_missing
                    ],
                }
            for e in out_edges:
                if e.target_row_id is None:
                    continue
                if e.target_row_id in trail:  # 防御性检查：正常算法下不可能发生
                    raise RuntimeError(f"原因链出现环: {trail + (x, e.target_row_id)}")
                walk(e.target_row_id, trail + (x,))

        walk(rid, ())
        return sorted(found.values(), key=lambda r: canonical(r))

    roots = {rid: find_roots(rid) for rid in sorted(quarantine)}

    # ---- 接受行证据：每条非跳过外键都记录解析位置 ----
    accepted: list[dict[str, Any]] = []
    for rid in sorted(survivors):
        cand = survivors[rid]
        tc = contract.by_table[cand["table"]]
        evidence: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for fk in tc.foreign_keys:
            tup = _key_tuple(cand["values"], fk.columns)
            if tup is None:
                null_cols = [c for c in fk.columns if cand["values"].get(c) is None]
                skipped.append({"fk_columns": list(fk.columns), "null_columns": null_cols, "match": "SIMPLE"})
                continue
            target_rid = target_exists(fk.target_table, fk.target_columns, tup)
            evidence.append({
                "fk_columns": list(fk.columns), "target_table": fk.target_table,
                "target_columns": list(fk.target_columns), "key_tuple": list(tup),
                "resolved_in": "existing" if target_rid.startswith("existing:") else "candidate",
                "target_row_id": target_rid,
            })
        accepted.append({"row_id": rid, "table": cand["table"], "values": cand["values"],
                         "fk_evidence": evidence, "fk_skipped_simple": skipped})

    q_list = [quarantine[rid] for rid in sorted(quarantine)]
    disposition_digest = digest_json({
        "accepted": sorted((r["row_id"], r["table"]) for r in accepted),
        "quarantined": sorted((q.row_id, q.table, q.reason, q.wave) for q in q_list),
        "edges": sorted(
            (e.source_row_id, e.target_table, tuple(e.key_tuple), e.reason, e.target_row_id) for e in edges
        ),
        "roots": roots,
    })
    return Plan(
        batch_id=batch_id, mode=mode, base_version=base_version,
        contract_digest=contract.digest, candidate_digest=candidate_digest,
        plan_digest=disposition_digest, accepted=accepted, quarantined=q_list,
        edges=edges, roots=roots, waves=waves, passes=pass_log,
    )
