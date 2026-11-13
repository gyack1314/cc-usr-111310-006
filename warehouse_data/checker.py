"""独立关系契约检查器。

与门禁引擎零代码共享（不 import warehouse_data.engine / store），
只依赖契约定义、标准库与落库后的事实：正式表、隔离区、原因边、回执、批候选原文。
独立重算最大闭合集合与根因链，逐条核对每条接受行的引用与每条隔离行的根因。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from .contract import Contract
from .io import TYPE_PREDICATES, digest_json

LOCAL_INVALID = "LOCAL_INVALID"
UK_CONFLICT = "UK_CONFLICT"
FK_MISSING_TARGET = "FK_MISSING_TARGET"
FK_PARENT_QUARANTINED = "FK_PARENT_QUARANTINED"
TERMINAL = frozenset({LOCAL_INVALID, UK_CONFLICT, FK_MISSING_TARGET})
STATUS_FULL = "PUBLISHED_FULL"
STATUS_CLOSED = "PUBLISHED_CLOSED_SUBSET"
STATUS_REJECTED = "REJECTED_ATOMIC"


class CheckFailure(AssertionError):
    """检查器自身输入不可用（回执缺失等）。"""


def _key_tuple(values: dict[str, Any], cols: tuple[str, ...]) -> tuple[Any, ...] | None:
    out: list[Any] = []
    for c in cols:
        x = values.get(c)
        if x is None:
            return None
        out.append(x)
    return tuple(out)


def _local_issues(tc, values: dict[str, Any]) -> list[str]:
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


def independent_plan(
    contract: Contract,
    candidates: list[dict[str, Any]],
    existing: dict[str, list[dict[str, Any]]],
) -> dict[str, frozenset[str]]:
    """检查器自带的闭包算法：本地+UK 隔离，再外键删到不动点。"""
    baseline: dict[tuple[str, tuple[str, ...]], dict[tuple[Any, ...], str]] = {}
    for tc in contract.tables:
        for key in tc.registered_keys:
            baseline[(tc.table, key)] = {}
        for idx, values in enumerate(existing.get(tc.table, [])):
            if _local_issues(tc, values):
                raise CheckFailure(f"旧库行 existing:{tc.table}#{idx} 不满足契约")
            for key in tc.registered_keys:
                tup = _key_tuple(values, key)
                if tup is not None:
                    baseline[(tc.table, key)][tup] = f"existing:{tc.table}#{idx}"

    q: set[str] = set()
    alive: dict[str, dict[str, Any]] = {}
    for cand in candidates:
        tc = contract.by_table[cand["table"]]
        if _local_issues(tc, cand["values"]):
            q.add(cand["row_id"])
        else:
            alive[cand["row_id"]] = cand

    cand_index: dict[tuple[str, tuple[str, ...]], dict[tuple[Any, ...], list[str]]] = {}
    for tc in contract.tables:
        for key in tc.registered_keys:
            cand_index[(tc.table, key)] = {}
    for rid, cand in alive.items():
        tc = contract.by_table[cand["table"]]
        for key in tc.registered_keys:
            tup = _key_tuple(cand["values"], key)
            if tup is not None:
                cand_index[(tc.table, key)].setdefault(tup, []).append(rid)

    uk_bad: set[str] = set()
    for (table, key), groups in cand_index.items():
        for tup, rids in groups.items():
            if len(rids) > 1 or tup in baseline[(table, key)]:
                uk_bad.update(rids)
    for rid in uk_bad:
        q.add(rid)
        alive.pop(rid, None)
    # UK 隔离行保留在 cand_index，波次内靠快照集合区分传播与缺目标。

    while True:
        snapshot = sorted(alive)
        snapshot_set = frozenset(alive)
        newly: set[str] = set()

        def resolve(table: str, key: tuple[str, ...], tup: tuple[Any, ...]) -> str | None:
            if tup in baseline[(table, key)]:
                return baseline[(table, key)][tup]
            slot = cand_index[(table, key)].get(tup)
            return slot[0] if slot else None

        for rid in snapshot:
            cand = alive[rid]
            tc = contract.by_table[cand["table"]]
            for fk in tc.foreign_keys:
                tup = _key_tuple(cand["values"], fk.columns)
                if tup is None:
                    continue
                target = resolve(fk.target_table, fk.target_columns, tup)
                if target is None:
                    newly.add(rid)
                    break
                if target.startswith("existing:") or target in snapshot_set:
                    continue
                newly.add(rid)  # 目标候选在先前波次已隔离
                break
        if not newly:
            break
        for rid in newly:
            alive.pop(rid)
            q.add(rid)
            # 隔离行保留在 cand_index 中，下一波靠快照集合区分“已隔离父行”与“缺目标”。
    return {"accepted": frozenset(alive), "quarantined": frozenset(q)}


def check_batch(conn: sqlite3.Connection, contract: Contract, batch_id: str) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    violations: list[str] = []
    evidence: dict[str, Any] = {}

    brow = conn.execute("SELECT * FROM _gw_batches WHERE batch_id=?", (batch_id,)).fetchone()
    if brow is None:
        raise CheckFailure(f"批回执不存在: {batch_id}")
    receipt = json.loads(brow["receipt_json"])
    if brow["plan_digest"] != receipt["plan_digest"]:
        violations.append("回执 plan_digest 与批表登记值不一致")
    recomputed_receipt_digest = digest_json(
        {k: v for k, v in receipt.items() if k != "receipt_digest"}
    )
    if recomputed_receipt_digest != receipt.get("receipt_digest"):
        violations.append("回执自摘要不成立，回执内容可能被改写")

    candidates = [
        {"row_id": r["row_id"], "table": r["table_name"], "values": json.loads(r["raw_json"])}
        for r in conn.execute(
            "SELECT row_id,table_name,raw_json,ordinal FROM _gw_batch_candidates "
            "WHERE batch_id=? ORDER BY ordinal", (batch_id,)
        )
    ]
    if digest_json(sorted((c["row_id"], c["table"], c["values"]) for c in candidates)) \
            != receipt["candidate_digest"]:
        violations.append("隔离区/批候选原文与回执 candidate_digest 不一致")

    # 当前正式表（含本批已发布行）即检查时刻的“旧有效表 + 已接受候选”
    existing: dict[str, list[dict[str, Any]]] = {}
    published_rows: dict[str, dict[str, Any]] = {}
    for tc in contract.tables:
        col_sql = ", ".join(f'"{c.name}"' for c in tc.columns)
        rows = conn.execute(f"SELECT {col_sql}, _gw_batch_id, _gw_row_id FROM \"{tc.table}\"").fetchall()
        table_rows: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            for col in tc.columns:
                if col.type == "bool" and item[col.name] is not None:
                    item[col.name] = bool(item[col.name])
            gw_rid = item.pop("_gw_row_id")
            gw_batch = item.pop("_gw_batch_id")
            table_rows.append(item)
            if gw_batch == batch_id:
                published_rows[gw_rid] = {"table": tc.table, "values": item}
            # 全库完整性：正式表每行必须能追溯到已登记批次（绕过门禁的手写行无处可藏）
            if conn.execute(
                "SELECT 1 FROM _gw_batches WHERE batch_id=?", (gw_batch,)
            ).fetchone() is None:
                violations.append(
                    f"正式表 {tc.table} 行 {gw_rid} 的批次 {gw_batch} 未登记，疑似绕过门禁写入"
                )
            if _local_issues(tc, item):
                violations.append(f"正式表 {tc.table} 行 {gw_rid} 不满足本地契约")
        existing[tc.table] = table_rows

    q_rows = {
        r["row_id"]: dict(r)
        for r in conn.execute("SELECT * FROM _gw_quarantine WHERE batch_id=?", (batch_id,))
    }
    edges = [dict(r) for r in conn.execute(
        "SELECT * FROM _gw_reason_edges WHERE batch_id=? ORDER BY id", (batch_id,)
    )]

    # 独立重算：以“发布前的库”为基线 = 当前正式表减去本批发布行
    pre_existing: dict[str, list[dict[str, Any]]] = {}
    for tc in contract.tables:
        published_value_sets = [
            v["values"] for v in published_rows.values() if v["table"] == tc.table
        ]
        pre_existing[tc.table] = [
            row for row in existing[tc.table] if row not in published_value_sets
        ]

    expected = independent_plan(contract, candidates, pre_existing)
    evidence["independent_accepted"] = sorted(expected["accepted"])
    evidence["independent_quarantined"] = sorted(expected["quarantined"])

    actual_q = frozenset(q_rows)
    actual_pub = frozenset(published_rows)
    accepted_receipt = frozenset(receipt["accepted_row_ids"])
    q_receipt = frozenset(receipt["quarantined_row_ids"])

    if accepted_receipt != expected["accepted"]:
        violations.append(
            f"接受集合与独立重算不一致: 差异 {sorted(accepted_receipt ^ expected['accepted'])}"
        )
    if q_receipt != expected["quarantined"]:
        violations.append(
            f"隔离集合与独立重算不一致: 差异 {sorted(q_receipt ^ expected['quarantined'])}"
        )
    if actual_q != q_receipt:
        violations.append("隔离区落库行与回执隔离行不一致")
    # 落库发布集合取决于模式策略：atomic 拒绝时即使存在闭合集合也不得发布
    expected_published = frozenset() if brow["mode"] == "atomic" and q_receipt else accepted_receipt
    if actual_pub != expected_published:
        violations.append(
            f"正式表本批发布行 {sorted(actual_pub)} 与策略应发布集合 {sorted(expected_published)} 不一致"
        )
    if accepted_receipt & q_receipt or actual_pub & actual_q:
        violations.append("同一 row_id 同时被接受和隔离")

    cand_by_id = {c["row_id"]: c for c in candidates}

    # ---- 每条接受行：本地合法、无 UK 冲突、每个非跳过 FK 目标真实可达 ----
    # all_index 覆盖当前正式表（旧库+已发布候选）；未发布的接受行（atomic 拒绝）
    # 其同批目标改用候选侧索引核对。
    all_index: dict[tuple[str, tuple[str, ...]], dict[tuple[Any, ...], list[str]]] = {}
    for tc in contract.tables:
        for key in tc.registered_keys:
            all_index[(tc.table, key)] = {}
        for idx, values in enumerate(existing.get(tc.table, [])):
            for key in tc.registered_keys:
                tup = _key_tuple(values, key)
                if tup is not None:
                    all_index[(tc.table, key)].setdefault(tup, []).append(f"existing:{tc.table}#{idx}")
    cand_accepted_index: dict[tuple[str, tuple[str, ...]], dict[tuple[Any, ...], list[str]]] = {}
    for tc in contract.tables:
        for key in tc.registered_keys:
            cand_accepted_index[(tc.table, key)] = {}
    for rid in accepted_receipt:
        cand = cand_by_id[rid]
        tc = contract.by_table[cand["table"]]
        for key in tc.registered_keys:
            tup = _key_tuple(cand["values"], key)
            if tup is not None:
                cand_accepted_index[(tc.table, key)].setdefault(tup, []).append(rid)

    def target_present(table: str, key: tuple[str, ...], tup: tuple[Any, ...]) -> bool:
        return bool(all_index[(table, key)].get(tup) or cand_accepted_index[(table, key)].get(tup))

    for rid in sorted(accepted_receipt):
        cand = cand_by_id[rid]
        tc = contract.by_table[cand["table"]]
        if rid in expected_published and rid not in published_rows:
            violations.append(f"应发布接受行 {rid} 未落入正式表")
        if _local_issues(tc, cand["values"]):
            violations.append(f"接受行 {rid} 实际不满足本地契约")
        for key in tc.registered_keys:
            tup = _key_tuple(cand["values"], key)
            if tup is None:
                continue
            # 已发布行同时在正式表索引中，只计一次；未发布（atomic 拒绝）才只在候选侧
            if tup not in all_index[(tc.table, key)]:
                holders = len(cand_accepted_index[(tc.table, key)].get(tup, []))
            else:
                holders = len(all_index[(tc.table, key)].get(tup, []))
            if holders > 1:
                violations.append(f"接受行 {rid} 的登记键 {list(key)} 仍存在冲突")
        for fk in tc.foreign_keys:
            tup = _key_tuple(cand["values"], fk.columns)
            if tup is None:
                null_cols = [c for c in fk.columns if cand["values"].get(c) is None]
                if not null_cols:
                    violations.append(f"接受行 {rid} 外键元组异常为 null 但无 null 列")
                continue  # MATCH SIMPLE 跳过
            if not target_present(fk.target_table, fk.target_columns, tup):
                violations.append(f"接受行 {rid} 外键 {list(fk.columns)}={list(tup)} 目标不存在")

    # ---- 全正式表外键悬空扫描：任何已发布行（含历史批次）都不得引用不存在目标 ----
    for tc in contract.tables:
        for values in existing.get(tc.table, []):
            for fk in tc.foreign_keys:
                tup = _key_tuple(values, fk.columns)
                if tup is None:
                    continue
                if not all_index[(fk.target_table, fk.target_columns)].get(tup):
                    violations.append(
                        f"正式表 {tc.table} 行 {tup!r} 外键 {list(fk.columns)}={list(tup)} 悬空"
                    )

    # ---- 每条隔离行：原样保留、原因合法、根因链终止在三类终端原因，且无理由环 ----
    for rid in sorted(q_receipt):
        qrow = q_rows.get(rid)
        cand = cand_by_id[rid]
        if qrow is None:
            violations.append(f"隔离行 {rid} 缺少隔离区记录")
            continue
        if json.loads(qrow["raw_values_json"]) != cand["values"]:
            violations.append(f"隔离行 {rid} 原文被改写，门禁不得修复隔离数据")
        if qrow["reason"] not in (LOCAL_INVALID, UK_CONFLICT, FK_MISSING_TARGET, FK_PARENT_QUARANTINED):
            violations.append(f"隔离行 {rid} 原因码未知: {qrow['reason']}")
        if qrow["wave"] < 0:
            violations.append(f"隔离行 {rid} 波次非法")

    # 原因边结构与有向无环（波次严格递减）
    out_edges: dict[str, list[dict[str, Any]]] = {}
    for e in edges:
        out_edges.setdefault(e["source_row_id"], []).append(e)
        if e["source_row_id"] not in q_receipt:
            violations.append(f"原因边源行 {e['source_row_id']} 不在隔离集合")
        if e["reason"] == FK_PARENT_QUARANTINED:
            if e["target_row_id"] is None or e["target_row_id"] not in q_receipt:
                violations.append(f"原因边 {e['source_row_id']} 指向的隔离父行不存在")
            else:
                if q_rows[e["target_row_id"]]["wave"] >= q_rows[e["source_row_id"]]["wave"]:
                    violations.append(f"原因边波次未严格递减: {e['source_row_id']} -> {e['target_row_id']}")
        if e["reason"] == FK_MISSING_TARGET and e["target_row_id"] is not None:
            violations.append(f"缺目标边不应带 target_row_id: {e['source_row_id']}")

    def roots_of(rid: str) -> set[str]:
        seen: set[str] = set()
        stack: list[tuple[str, tuple[str, ...]]] = [(rid, ())]
        while stack:
            cur, trail = stack.pop()
            if cur in trail:
                violations.append(f"根因链出现环: {trail + (cur,)}")
                continue
            qrow = q_rows.get(cur)
            if qrow is not None and qrow["reason"] in (LOCAL_INVALID, UK_CONFLICT):
                seen.add(cur)
                continue
            cur_edges = out_edges.get(cur, [])
            if not cur_edges or any(e["reason"] == FK_MISSING_TARGET for e in cur_edges):
                seen.add(cur)  # 终端：真正缺目标
            for e in cur_edges:
                if e["target_row_id"] is not None:
                    stack.append((e["target_row_id"], trail + (cur,)))
        return seen

    root_map: dict[str, set[str]] = {}
    for rid in sorted(q_receipt):
        roots = roots_of(rid)
        root_map[rid] = roots
        if not roots:
            violations.append(f"隔离行 {rid} 没有可追溯的终端根因")
        for r in roots:
            qrow = q_rows.get(r)
            if qrow is not None and qrow["reason"] not in TERMINAL:
                violations.append(f"隔离行 {rid} 的根因 {r} 不是终端原因")
    evidence["root_causes"] = {k: sorted(v) for k, v in sorted(root_map.items())}

    # ---- 最大性：任何被隔离行都不可能被加回（重算集合即最大闭合集合）----
    if expected["accepted"] | expected["quarantined"] != frozenset(cand_by_id):
        violations.append("存在候选行既未接受也未隔离，处置不完备")

    # ---- 模式策略与差异化回执 ----
    status = brow["status"]
    mode = brow["mode"]
    if mode == "atomic":
        if q_receipt and status != STATUS_REJECTED:
            violations.append("atomic 模式存在隔离却未整批拒绝")
        if q_receipt and actual_pub:
            violations.append("atomic 拒绝批次仍有行写入正式表")
        if not q_receipt and status != STATUS_FULL:
            violations.append("atomic 全量接受时状态应为 PUBLISHED_FULL")
    elif mode == "closed-subset":
        if status not in (STATUS_FULL, STATUS_CLOSED):
            violations.append("closed-subset 回执状态非法")
        if q_receipt and status != STATUS_CLOSED:
            violations.append("closed-subset 存在隔离时状态应为 PUBLISHED_CLOSED_SUBSET")
        if actual_pub != expected["accepted"]:
            violations.append("closed-subset 发布集合不等于闭合集合")
    if receipt["published_count"] != len(actual_pub):
        violations.append("回执 published_count 与正式表事实不符")
    if receipt["quarantined_count"] != len(actual_q):
        violations.append("回执 quarantined_count 与隔离区事实不符")

    # 版本链
    if receipt["committed_from_version"] != brow["base_version"]:
        violations.append("回执基准版本与批表登记不一致")
    evidence["status"] = status
    evidence["mode"] = mode
    evidence["published"] = sorted(actual_pub)
    evidence["quarantined"] = sorted(actual_q)
    return {
        "batch_id": batch_id,
        "ok": not violations,
        "violation_count": len(violations),
        "violations": violations,
        "evidence": evidence,
    }
