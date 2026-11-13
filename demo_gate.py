"""入仓关系质量门禁端到端演示。

场景：
  1. 双表合法环（工单 <-> 物料记录互引），含同键两条内容不同新行与顺序无关性；
  2. 三层坏父传播（坏父 -> 工单 -> 工单明细，跨多表），根因落到真正缺目标；
  3. 复合外键 MATCH SIMPLE 部分 null 跳过、空串不是 null；
  4. 同键两条内容不同的新行整组隔离（含与旧库冲突）；
  另含 atomic / closed-subset 回执差异，以及计算后旧库变化导致的版本失效拒绝。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from warehouse_data.checker import check_batch
from warehouse_data.contract import parse_contract
from warehouse_data.engine import parse_manifest, plan_batch
from warehouse_data.store import (
    BatchExistsError,
    StaleVersionError,
    Warehouse,
)


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def contracts() -> list[dict]:
    return [
        {
            "table": "supplier",
            "columns": {
                "supplier_id": {"type": "long", "required": True},
                "name": {"type": "string", "required": True},
                "active": {"type": "bool", "required": False},
            },
            "primary": ["supplier_id"],
            "unique": [["name"]],
            "foreign_keys": [],
        },
        {
            "table": "material",
            "columns": {
                "material_id": {"type": "long", "required": True},
                "spec": {"type": "string", "required": True},
                "root_wo_id": {"type": "long", "required": False},
            },
            "primary": ["material_id"],
            "unique": [],
            "foreign_keys": [
                {"columns": ["root_wo_id"], "target": "work_order",
                 "target_columns": ["wo_id"], "match": "SIMPLE"},
            ],
        },
        {
            "table": "work_order",
            "columns": {
                "wo_id": {"type": "long", "required": True},
                "material_id": {"type": "long", "required": False},
                "supplier_id": {"type": "long", "required": False},
            },
            "primary": ["wo_id"],
            "unique": [],
            "foreign_keys": [
                {"columns": ["material_id"], "target": "material",
                 "target_columns": ["material_id"], "match": "SIMPLE"},
                {"columns": ["supplier_id"], "target": "supplier",
                 "target_columns": ["supplier_id"], "match": "SIMPLE"},
            ],
        },
        {
            "table": "wo_detail",
            "columns": {
                "detail_id": {"type": "long", "required": True},
                "wo_id": {"type": "long", "required": False},
                "loc_zone": {"type": "string", "required": False},
                "loc_bin": {"type": "string", "required": False},
            },
            "primary": ["detail_id"],
            "unique": [["loc_zone", "loc_bin"]],
            "foreign_keys": [
                {"columns": ["wo_id"], "target": "work_order",
                 "target_columns": ["wo_id"], "match": "SIMPLE"},
                {"columns": ["loc_zone", "loc_bin"], "target": "location",
                 "target_columns": ["zone", "bin"], "match": "SIMPLE"},
            ],
        },
        {
            "table": "location",
            "columns": {
                "zone": {"type": "string", "required": True},
                "bin": {"type": "string", "required": True},
            },
            "primary": ["zone", "bin"],
            "unique": [],
            "foreign_keys": [],
        },
    ]


def manifest(batch_id: str, mode: str, candidates: list[dict], existing: dict | None = None,
             base_version: str = "v1") -> dict:
    tables = [c["table"] for c in contracts()]
    return {
        "kind": "relational-contract",
        "contracts": contracts(),
        "existing": existing or {t: [] for t in tables},
        "candidates": candidates,
        "mode": mode,
        "batch_id": batch_id,
        "warehouse_version": base_version,
        "contract_version": "cv-demo-1",
    }


def show_plan(label: str, plan) -> None:
    print(f"\n[{label}] 不动点轮次: {json.dumps(plan.passes, ensure_ascii=False)}")
    print(f"[{label}] 接受 {len(plan.accepted)} 行: {sorted(plan.accepted_ids)}")
    print(f"[{label}] 隔离 {len(plan.quarantined)} 行:")
    for q in plan.quarantined:
        print(f"    - {q.row_id:10s} 表={q.table:10s} 波次={q.wave} 原因={q.reason}")
        for root in plan.roots[q.row_id]:
            print(f"        根因 -> {root['row_id']} [{root['reason']}] {json.dumps(root['details'], ensure_ascii=False)}")


def run_check(wh_path: str, contract, batch_id: str) -> dict:
    conn = sqlite3.connect(wh_path)
    try:
        return check_batch(conn, contract, batch_id)
    finally:
        conn.close()


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="gate-demo-"))
    db_path = workdir / "warehouse.sqlite"
    print(f"演示库目录: {workdir}")
    contract = parse_contract(contracts())
    print(f"契约摘要 contract_digest={contract.digest[:16]}…")

    # ---------------- 场景 1：双表合法环（纯环，无旧父行也整体保留） ----------------
    banner("场景1 双表合法环：work_order w1 <-> material m1 互引，无需从已有父表起步")
    m1 = [
        {"row_id": "w1", "table": "work_order", "values": {"wo_id": 1, "material_id": 10, "supplier_id": None}},
        {"row_id": "m1", "table": "material", "values": {"material_id": 10, "spec": "钢板A", "root_wo_id": 1}},
    ]
    man1 = manifest("b1-cycle", "closed-subset", m1)
    _, existing1, cands1, mode1, bid1, ver1, _ = parse_manifest(man1)
    plan1 = plan_batch(contract, cands1, existing1, mode1, batch_id=bid1, base_version=ver1)
    show_plan("原始顺序", plan1)
    shuffled = manifest("b1-cycle", "closed-subset", list(reversed(m1)))
    _, ex_s, cand_s, mode_s, bid_s, ver_s, _ = parse_manifest(shuffled)
    plan1r = plan_batch(contract, cand_s, ex_s, mode_s, batch_id=bid_s, base_version=ver_s)
    show_plan("逆序输入", plan1r)
    assert plan1.plan_digest == plan1r.plan_digest, "闭包结果必须与输入顺序无关"
    assert plan1.accepted_ids == frozenset({"w1", "m1"}), "合法环保留"
    assert not plan1.quarantined, "纯环无隔离"
    print("断言通过：逆序输入 plan_digest 相同；环 w1<->m1 不依赖任何旧父行，整体保留")

    with Warehouse(db_path) as wh:
        wh.initialize(contract)
        receipt1 = wh.commit(contract, man1, plan1)
    print(f"回执 status={receipt1['status']} 发布={receipt1['published_count']} 隔离={receipt1['quarantined_count']}")
    rep1 = run_check(db_path, contract, "b1-cycle")
    print(f"独立检查器: ok={rep1['ok']} 违规数={rep1['violation_count']}")
    assert rep1["ok"]

    # ---------------- 场景 1b：同键两条内容不同的新行，整组隔离且顺序无关 ----------------
    banner("场景1b 同键两条内容不同：wo_id=2 出现两次，两条新行一起隔离，不按顺序留第一条")
    m1b = [
        {"row_id": "w-first", "table": "work_order",
         "values": {"wo_id": 2, "material_id": 10, "supplier_id": None}},
        {"row_id": "w-second", "table": "work_order",
         "values": {"wo_id": 2, "material_id": None, "supplier_id": None}},
    ]
    man1b = manifest("b1b-dup-key", "closed-subset", m1b, base_version="v2")
    _, ex1b, cand1b, mode1b, bid1b, ver1b, _ = parse_manifest(man1b)
    plan1b = plan_batch(contract, cand1b, ex1b, mode1b, batch_id=bid1b, base_version=ver1b)
    plan1b_rev = plan_batch(contract, list(reversed(cand1b)), ex1b, mode1b,
                            batch_id=bid1b, base_version=ver1b)
    show_plan("同键冲突", plan1b)
    assert plan1b.quarantined_ids == frozenset({"w-first", "w-second"})
    assert not plan1b.accepted
    assert plan1b.plan_digest == plan1b_rev.plan_digest, "冲突组选择不能依赖输入顺序"
    print("断言通过：无论谁排第一，w-first 与 w-second 都被 UK_CONFLICT 整组隔离")
    with Warehouse(db_path) as wh:
        receipt1b = wh.commit(contract, man1b, plan1b)
    rep1b = run_check(db_path, contract, "b1b-dup-key")
    print(f"回执 status={receipt1b['status']}；独立检查器 ok={rep1b['ok']}")
    assert rep1b["ok"]

    # ---------------- 场景 2：三层坏父传播 ----------------
    banner("场景2 三层坏父传播：m-bad 引用不存在的工单 999 -> w-bad -> d-bad 连锁隔离")
    m2 = [
        # 第一层：真正缺目标（终端根因）
        {"row_id": "m-bad", "table": "material",
         "values": {"material_id": 20, "spec": "坏父料", "root_wo_id": 999}},
        # 第二层：引用已隔离父行
        {"row_id": "w-bad", "table": "work_order",
         "values": {"wo_id": 2, "material_id": 20, "supplier_id": None}},
        # 第三层：跨表继续传播
        {"row_id": "d-bad", "table": "wo_detail",
         "values": {"detail_id": 30, "wo_id": 2, "loc_zone": None, "loc_bin": None}},
        # 对照组：引用旧有效库的合法行，必须存活
        {"row_id": "d-ok", "table": "wo_detail",
         "values": {"detail_id": 31, "wo_id": 1, "loc_zone": None, "loc_bin": None}},
    ]
    man2 = manifest("b2-cascade", "closed-subset", m2, base_version="v3")
    with Warehouse(db_path) as wh:
        man2["existing"] = wh.load_existing(contract)
    _, existing2, cands2, mode2, bid2, ver2, _ = parse_manifest(man2)
    plan2 = plan_batch(contract, cands2, existing2, mode2, batch_id=bid2, base_version=ver2)
    show_plan("坏父传播", plan2)
    assert plan2.quarantined_ids == frozenset({"m-bad", "w-bad", "d-bad"})
    assert plan2.accepted_ids == frozenset({"d-ok"})
    assert {r["row_id"] for r in plan2.roots["d-bad"]} == {"m-bad"}, "明细根因必须穿透到终端缺目标"
    assert plan2.roots["m-bad"][0]["reason"] == "FK_MISSING_TARGET"
    waves = plan2.waves
    assert waves["m-bad"] < waves["w-bad"] < waves["d-bad"], "波次严格递增，原因图无环"
    print("断言通过：三层传播波次 %s；d-bad 根因穿透到 m-bad(FK_MISSING_TARGET)；d-ok 引用旧库存活"
          % {k: waves[k] for k in ("m-bad", "w-bad", "d-bad")})
    with Warehouse(db_path) as wh:
        receipt2 = wh.commit(contract, man2, plan2)
    rep2 = run_check(db_path, contract, "b2-cascade")
    print(f"回执 status={receipt2['status']}；独立检查器 ok={rep2['ok']}")
    assert rep2["ok"]

    # ---------------- 场景 3：复合外键部分 null / 空串 ----------------
    banner("场景3 MATCH SIMPLE：复合键任一 null 跳过；空串不是 null，照常查目标")
    m3 = [
        {"row_id": "loc1", "table": "location", "values": {"zone": "Z1", "bin": "B1"}},
        # 部分 null：loc_zone 有值、loc_bin 为 null -> SIMPLE 跳过，接受
        {"row_id": "d-partial-null", "table": "wo_detail",
         "values": {"detail_id": 40, "wo_id": 1, "loc_zone": "Z1", "loc_bin": None}},
        # 全非 null 且目标存在 -> 接受
        {"row_id": "d-full-match", "table": "wo_detail",
         "values": {"detail_id": 41, "wo_id": 1, "loc_zone": "Z1", "loc_bin": "B1"}},
        # 空串不是 null：复合键 ("Z9","") 必须参与查找，目标不存在 -> 隔离
        {"row_id": "d-empty-string", "table": "wo_detail",
         "values": {"detail_id": 42, "wo_id": 1, "loc_zone": "Z9", "loc_bin": ""}},
        # 本地非法对照组：bool 列收到 long（supplier.active 类型严格）
        {"row_id": "s-bad-type", "table": "supplier",
         "values": {"supplier_id": 7, "name": "类型错供应商", "active": 1}},
    ]
    man3 = manifest("b3-composite", "closed-subset", m3, base_version="v4")
    with Warehouse(db_path) as wh:
        man3["existing"] = wh.load_existing(contract)
    _, existing3, cands3, mode3, bid3, ver3, _ = parse_manifest(man3)
    plan3 = plan_batch(contract, cands3, existing3, mode3, batch_id=bid3, base_version=ver3)
    show_plan("复合键", plan3)
    assert plan3.accepted_ids == frozenset({"loc1", "d-partial-null", "d-full-match"})
    assert plan3.quarantined_ids == frozenset({"d-empty-string", "s-bad-type"})
    qmap = {q.row_id: q for q in plan3.quarantined}
    assert qmap["d-empty-string"].reason == "FK_MISSING_TARGET"
    assert qmap["s-bad-type"].reason == "LOCAL_INVALID"
    skipped = next(r for r in plan3.accepted if r["row_id"] == "d-partial-null")["fk_skipped_simple"]
    assert skipped and skipped[0]["null_columns"] == ["loc_bin"]
    print("断言通过：部分 null 跳过并留证；空串行按缺目标隔离；bool 列收 long 判 LOCAL_INVALID")
    with Warehouse(db_path) as wh:
        receipt3 = wh.commit(contract, man3, plan3)
    rep3 = run_check(db_path, contract, "b3-composite")
    print(f"回执 status={receipt3['status']}；独立检查器 ok={rep3['ok']}")
    assert rep3["ok"]

    # ---------------- 场景 4：同键两条内容不同 + 与旧库冲突 ----------------
    banner("场景4 唯一冲突：同批同键两条内容不同整组隔离；与旧有效库冲突只隔离新行")
    # 先把“在册供应商 99”作为独立基线批次入仓，成为正式表中的旧有效行
    base_rows = [
        {"row_id": "s-base-99", "table": "supplier",
         "values": {"supplier_id": 99, "name": "在册供应商", "active": True}},
    ]
    man_base = manifest("b4-base", "closed-subset", base_rows, base_version="v5")
    with Warehouse(db_path) as wh:
        man_base["existing"] = wh.load_existing(contract)
    _, ex_b, cand_b, mode_b, bid_b, ver_b, _ = parse_manifest(man_base)
    plan_base = plan_batch(contract, cand_b, ex_b, mode_b, batch_id=bid_b, base_version=ver_b)
    with Warehouse(db_path) as wh:
        wh.commit(contract, man_base, plan_base)
    print("基线批次 b4-base 已发布在册供应商 supplier_id=99")

    m4 = [
        # 同 name 唯一键（supplier.uk_name），内容不同 -> 两条新行一起隔离
        {"row_id": "s-a", "table": "supplier",
         "values": {"supplier_id": 1, "name": "重名厂", "active": True}},
        {"row_id": "s-b", "table": "supplier",
         "values": {"supplier_id": 2, "name": "重名厂", "active": False}},
        # 主键与旧库 supplier_id=99 冲突：只隔离新行，旧行不动
        {"row_id": "s-old-key", "table": "supplier",
         "values": {"supplier_id": 99, "name": "抢主键者", "active": None}},
        # 引用被隔离供应商的工单 -> 外键传播隔离，根因是 UK_CONFLICT
        {"row_id": "w-orphan", "table": "work_order",
         "values": {"wo_id": 5, "material_id": None, "supplier_id": 1}},
        # 合法对照
        {"row_id": "w-good", "table": "work_order",
         "values": {"wo_id": 6, "material_id": 10, "supplier_id": 99}},
    ]
    man4 = manifest("b4-unique", "closed-subset", m4, base_version="v6")
    with Warehouse(db_path) as wh:
        man4["existing"] = wh.load_existing(contract)
    _, existing4p, cands4, mode4, bid4, ver4, _ = parse_manifest(man4)
    plan4 = plan_batch(contract, cands4, existing4p, mode4, batch_id=bid4, base_version=ver4)
    show_plan("唯一冲突", plan4)
    assert plan4.quarantined_ids == frozenset({"s-a", "s-b", "s-old-key", "w-orphan"})
    assert plan4.accepted_ids == frozenset({"w-good"})
    orphan_roots = {r["row_id"] for r in plan4.roots["w-orphan"]}
    assert orphan_roots <= {"s-a", "s-b"}, orphan_roots
    assert all(r["reason"] == "UK_CONFLICT" for r in plan4.roots["w-orphan"])
    orphan_edge = next(e for e in plan4.edges if e.source_row_id == "w-orphan")
    assert orphan_edge.target_row_id in {"s-a", "s-b"}
    print(f"断言通过：重名组 s-a/s-b 全隔离；旧库 99 不动、s-old-key 隔离；"
          f"w-orphan 传播边指向冲突组成员 {orphan_edge.target_row_id}，根因落到 UK_CONFLICT")
    with Warehouse(db_path) as wh:
        receipt4 = wh.commit(contract, man4, plan4)
    rep4 = run_check(db_path, contract, "b4-unique")
    print(f"回执 status={receipt4['status']}；独立检查器 ok={rep4['ok']} 证据根因={json.dumps(rep4['evidence']['root_causes'], ensure_ascii=False)}")
    assert rep4["ok"]

    # ---------------- 模式差异：atomic 遇隔离整批拒绝 ----------------
    banner("模式对比：同样候选 atomic 整批不发布，closed-subset 只发闭合集合")
    # 用独立库构造受控前提：旧库已有 wo_id=1，d-ok 引用它成为闭合集合中唯一可发布行
    mode_db = workdir / "mode-compare.sqlite"
    baseline_m = manifest(
        "b0", "closed-subset",
        [{"row_id": "w-base", "table": "work_order",
          "values": {"wo_id": 1, "material_id": None, "supplier_id": None}}],
    )
    _, ex0, cand0, mode0, bid0, ver0, _ = parse_manifest(baseline_m)
    plan0 = plan_batch(contract, cand0, ex0, mode0, batch_id=bid0, base_version=ver0)
    with Warehouse(mode_db) as wh:
        wh.initialize(contract)
        wh.commit(contract, baseline_m, plan0)

    man_atomic = manifest("b5-atomic", "atomic", m2, base_version="v2")
    with Warehouse(mode_db) as wh:
        man_atomic["existing"] = wh.load_existing(contract)
    _, ex_a, cand_a, mode_a, bid_a, ver_a, _ = parse_manifest(man_atomic)
    plan_a = plan_batch(contract, cand_a, ex_a, mode_a, batch_id=bid_a, base_version=ver_a)
    with Warehouse(mode_db) as wh:
        receipt_a = wh.commit(contract, man_atomic, plan_a)
    rep_a = run_check(mode_db, contract, "b5-atomic")
    print(f"atomic        -> status={receipt_a['status']} published={receipt_a['published_count']} "
          f"quarantined={receipt_a['quarantined_count']} 检查器ok={rep_a['ok']}")
    assert receipt_a["status"] == "REJECTED_ATOMIC" and receipt_a["published_count"] == 0
    assert rep_a["ok"]

    man_cs = manifest("b5-closed", "closed-subset", m2, base_version="v3")
    with Warehouse(mode_db) as wh:
        man_cs["existing"] = wh.load_existing(contract)  # atomic 未落正式表，基线不变
    _, ex_c, cand_c, mode_c, bid_c, ver_c, _ = parse_manifest(man_cs)
    plan_c = plan_batch(contract, cand_c, ex_c, mode_c, batch_id=bid_c, base_version=ver_c)
    with Warehouse(mode_db) as wh:
        receipt_c = wh.commit(contract, man_cs, plan_c)
    rep_c = run_check(mode_db, contract, "b5-closed")
    print(f"closed-subset -> status={receipt_c['status']} published={receipt_c['published_count']} "
          f"quarantined={receipt_c['quarantined_count']} 检查器ok={rep_c['ok']}")
    assert receipt_c["status"] == "PUBLISHED_CLOSED_SUBSET" and receipt_c["published_count"] == 1
    assert receipt_a["status"] != receipt_c["status"], "两种模式回执必须明确不同"
    assert rep_c["ok"]

    # ---------------- 并发：计算后旧库变化，提交被版本门禁拒绝 ----------------
    banner("并发防护：计划基于 v7，另一连接先发布 v8 后，旧计划提交必须失败")
    man6 = manifest("b6-stale", "closed-subset",
                    [{"row_id": "w6", "table": "work_order",
                      "values": {"wo_id": 7, "material_id": None, "supplier_id": None}}],
                    base_version="v7")
    with Warehouse(db_path) as wh:
        man6["existing"] = wh.load_existing(contract)
    _, ex6, cand6, mode6, bid6, ver6, _ = parse_manifest(man6)
    plan6 = plan_batch(contract, cand6, ex6, mode6, batch_id=bid6, base_version=ver6)
    other = manifest("b-other", "closed-subset",
                     [{"row_id": "w-other", "table": "work_order",
                       "values": {"wo_id": 8, "material_id": None, "supplier_id": None}}],
                     base_version="v7")
    with Warehouse(db_path) as wh_other:
        other["existing"] = wh_other.load_existing(contract)
        _, ex_o, cand_o, mode_o, bid_o, ver_o, _ = parse_manifest(other)
        wh_other.commit(contract, other,
                        plan_batch(contract, cand_o, ex_o, mode_o, batch_id=bid_o, base_version=ver_o))
    with Warehouse(db_path) as wh:
        try:
            wh.commit(contract, man6, plan6)
            raise AssertionError("陈旧计划不应提交成功")
        except StaleVersionError as e:
            print(f"按预期拒绝: {type(e).__name__}: {e}")
    # 用新版本重算后可以正常提交
    man6b = manifest("b6-stale", "closed-subset", man6["candidates"], base_version="v8")
    with Warehouse(db_path) as wh:
        man6b["existing"] = wh.load_existing(contract)
    _, ex6b, cand6b, mode6b, bid6b, ver6b, _ = parse_manifest(man6b)
    plan6b = plan_batch(contract, cand6b, ex6b, mode6b, batch_id=bid6b, base_version=ver6b)
    with Warehouse(db_path) as wh:
        r6 = wh.commit(contract, man6b, plan6b)
    print(f"基于新版本重算后提交: status={r6['status']} published={r6['published_count']}")
    rep6 = run_check(db_path, contract, "b6-stale")
    assert rep6["ok"]

    # ---------------- 批身份不可复用 + 隔离原文未被篡改 ----------------
    banner("批身份与隔离原文：重复 batch_id 拒绝；隔离区保存原始 values 而非修复后数据")
    with Warehouse(db_path) as wh:
        try:
            wh.commit(contract, man1, plan1)
            raise AssertionError("重复 batch_id 不应成功")
        except BatchExistsError as e:
            print(f"按预期拒绝: {type(e).__name__}: {e}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    raw = conn.execute(
        "SELECT raw_values_json FROM _gw_quarantine WHERE batch_id=? AND row_id=?",
        ("b3-composite", "s-bad-type"),
    ).fetchone()
    conn.close()
    assert json.loads(raw["raw_values_json"]) == {"supplier_id": 7, "name": "类型错供应商", "active": 1}
    print(f"隔离原文原样保留: {raw['raw_values_json']}（未把 active:1 修成 true）")

    banner("全部场景通过")
    print(f"数据库保留在: {db_path}")
    with sqlite3.connect(db_path) as conn:
        for r in conn.execute("SELECT batch_id,mode,status,base_version,committed_version FROM _gw_batches ORDER BY rowid"):
            print("  批 %-10s 模式 %-13s 状态 %-22s %s -> %s" % tuple(r))


if __name__ == "__main__":
    main()
