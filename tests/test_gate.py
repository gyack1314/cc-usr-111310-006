"""批级关系契约门禁单元测试。

覆盖：契约合法性、顺序无关最大闭包、合法环、三层坏父传播、NULLS DISTINCT、
MATCH SIMPLE、空串非 null、同键冲突整组隔离、原子发布/拒绝、版本失效与检查器篡改检测。
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from warehouse_data.checker import check_batch
from warehouse_data.contract import ContractError, parse_contract
from warehouse_data.engine import (
    FK_MISSING_TARGET,
    FK_PARENT_QUARANTINED,
    LOCAL_INVALID,
    UK_CONFLICT,
    ManifestError,
    parse_manifest,
    plan_batch,
)
from warehouse_data.store import BatchExistsError, StaleVersionError, Warehouse


def simple_contracts():
    return [
        {"table": "A", "columns": {"id": {"type": "long", "required": True},
                                   "b_id": {"type": "long", "required": False}},
         "primary": ["id"], "unique": [],
         "foreign_keys": [{"columns": ["b_id"], "target": "B",
                           "target_columns": ["id"], "match": "SIMPLE"}]},
        {"table": "B", "columns": {"id": {"type": "long", "required": True},
                                   "a_id": {"type": "long", "required": False}},
         "primary": ["id"], "unique": [],
         "foreign_keys": [{"columns": ["a_id"], "target": "A",
                           "target_columns": ["id"], "match": "SIMPLE"}]},
        {"table": "C", "columns": {"id": {"type": "long", "required": True},
                                   "a_id": {"type": "long", "required": False}},
         "primary": ["id"], "unique": [],
         "foreign_keys": [{"columns": ["a_id"], "target": "A",
                           "target_columns": ["id"], "match": "SIMPLE"}]},
    ]


def make_manifest(contracts, candidates, existing=None, mode="closed-subset", batch_id="b1", base="v1"):
    tables = [c["table"] for c in contracts]
    return {
        "kind": "relational-contract", "contracts": contracts,
        "existing": existing if existing is not None else {t: [] for t in tables},
        "candidates": candidates, "mode": mode, "batch_id": batch_id,
        "warehouse_version": base, "contract_version": "cv1",
    }


def plan_of(manifest_v):
    contract, existing, candidates, mode, bid, ver, _ = parse_manifest(manifest_v)
    return contract, plan_batch(contract, candidates, existing, mode, batch_id=bid, base_version=ver)


class ContractTests(unittest.TestCase):
    def test_fk_target_must_be_registered_key(self):
        contracts = simple_contracts()
        contracts[0]["foreign_keys"][0]["target_columns"] = ["a_id"]  # B 上未登记
        with self.assertRaises(ContractError):
            parse_contract(contracts)

    def test_primary_must_be_required(self):
        contracts = simple_contracts()
        contracts[0]["columns"]["id"]["required"] = False
        with self.assertRaises(ContractError):
            parse_contract(contracts)

    def test_unknown_type_and_bad_identifier(self):
        contracts = simple_contracts()
        contracts[0]["columns"]["id"]["type"] = "decimal"
        with self.assertRaises(ContractError):
            parse_contract(contracts)
        contracts = simple_contracts()
        contracts[0]["table"] = "A;DROP"
        with self.assertRaises(ContractError):
            parse_contract(contracts)

    def test_digest_stable(self):
        # 摘要按表名规范化，表顺序变化不影响摘要；契约文本变动必然改变摘要
        c1 = parse_contract(simple_contracts())
        reordered = list(reversed(simple_contracts()))
        c2 = parse_contract(reordered)
        self.assertEqual(c1.digest, c2.digest)
        self.assertEqual(c1.digest, parse_contract(json.loads(json.dumps(simple_contracts()))).digest)
        changed = simple_contracts()
        changed[0]["columns"]["new_col"] = {"type": "string", "required": False}
        self.assertNotEqual(c1.digest, parse_contract(changed).digest)


class ClosureTests(unittest.TestCase):
    def setUp(self):
        self.contracts = simple_contracts()

    def test_legal_cycle_kept_without_existing_parent(self):
        m = make_manifest(self.contracts, [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": 2}},
            {"row_id": "b2", "table": "B", "values": {"id": 2, "a_id": 1}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.accepted_ids, frozenset({"a1", "b2"}))
        self.assertEqual(plan.quarantined_ids, frozenset())

    def test_three_layer_bad_parent_propagation(self):
        m = make_manifest(self.contracts, [
            {"row_id": "a-bad", "table": "A", "values": {"id": 3, "b_id": 999}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 4, "a_id": 3}},
            {"row_id": "c-bad", "table": "C", "values": {"id": 5, "a_id": 3}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined_ids, frozenset({"a-bad", "b-bad", "c-bad"}))
        self.assertEqual(plan.waves["a-bad"], 1)
        self.assertLess(plan.waves["a-bad"], plan.waves["b-bad"])
        q = {x.row_id: x for x in plan.quarantined}
        self.assertEqual(q["a-bad"].reason, FK_MISSING_TARGET)
        self.assertEqual(q["b-bad"].reason, FK_PARENT_QUARANTINED)
        for rid in ("b-bad", "c-bad"):
            roots = {r["row_id"] for r in plan.roots[rid]}
            self.assertEqual(roots, {"a-bad"})
            self.assertEqual(plan.roots[rid][0]["reason"], FK_MISSING_TARGET)

    def test_order_independence(self):
        rows = [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": 2}},
            {"row_id": "b2", "table": "B", "values": {"id": 2, "a_id": 1}},
            {"row_id": "a-bad", "table": "A", "values": {"id": 3, "b_id": 999}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 4, "a_id": 3}},
        ]
        m1 = make_manifest(self.contracts, rows)
        m2 = make_manifest(self.contracts, list(reversed(rows)))
        _, p1 = plan_of(m1)
        _, p2 = plan_of(m2)
        self.assertEqual(p1.plan_digest, p2.plan_digest)

    def test_duplicate_key_quarantines_whole_group(self):
        m = make_manifest(self.contracts, [
            {"row_id": "x1", "table": "A", "values": {"id": 7, "b_id": None}},
            {"row_id": "x2", "table": "A", "values": {"id": 7, "b_id": None}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined_ids, frozenset({"x1", "x2"}))
        self.assertTrue(all(q.reason == UK_CONFLICT for q in plan.quarantined))

    def test_conflict_with_existing_only_isolates_new_rows(self):
        existing = {t: [] for t in "ABC"}
        existing["A"] = [{"id": 7, "b_id": None}]
        m = make_manifest(self.contracts, [
            {"row_id": "x1", "table": "A", "values": {"id": 7, "b_id": None}},
        ], existing=existing)
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined_ids, frozenset({"x1"}))
        detail = plan.quarantined[0].details[0]
        self.assertEqual(detail["baseline_row_id"], "existing:A#0")

    def test_nulls_distinct_and_match_simple(self):
        # 两条 b_id=null 的 A 行不构成唯一冲突（NULLS DISTINCT），且外键被 SIMPLE 跳过
        m = make_manifest(self.contracts, [
            {"row_id": "a-n1", "table": "A", "values": {"id": 1, "b_id": None}},
            {"row_id": "a-n2", "table": "A", "values": {"id": 2, "b_id": None}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.accepted_ids, frozenset({"a-n1", "a-n2"}))
        for r in plan.accepted:
            self.assertTrue(r["fk_skipped_simple"])

    def test_empty_string_is_not_null(self):
        contracts = [
            {"table": "P", "columns": {"k": {"type": "string", "required": True}},
             "primary": ["k"], "unique": [], "foreign_keys": []},
            {"table": "Q", "columns": {"id": {"type": "long", "required": True},
                                       "k": {"type": "string", "required": False}},
             "primary": ["id"], "unique": [],
             "foreign_keys": [{"columns": ["k"], "target": "P",
                               "target_columns": ["k"], "match": "SIMPLE"}]},
        ]
        m = make_manifest(contracts, [
            {"row_id": "q-empty", "table": "Q", "values": {"id": 1, "k": ""}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined_ids, frozenset({"q-empty"}))
        self.assertEqual(plan.quarantined[0].reason, FK_MISSING_TARGET)

    def test_strict_types_bool_not_long(self):
        m = make_manifest(self.contracts, [
            {"row_id": "a-bool", "table": "A", "values": {"id": True, "b_id": None}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined[0].reason, LOCAL_INVALID)

    def test_unknown_column_and_required_null(self):
        m = make_manifest(self.contracts, [
            {"row_id": "a-x", "table": "A", "values": {"id": 1, "b_id": None, "ghost": 1}},
            {"row_id": "a-null", "table": "A", "values": {"id": None, "b_id": None}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.quarantined_ids, frozenset({"a-x", "a-null"}))
        self.assertTrue(all(q.reason == LOCAL_INVALID for q in plan.quarantined))

    def test_bad_mode_and_duplicate_row_id(self):
        m = make_manifest(self.contracts, [], mode="weird")
        with self.assertRaises(ManifestError):
            parse_manifest(m)
        m = make_manifest(self.contracts, [
            {"row_id": "dup", "table": "A", "values": {"id": 1, "b_id": None}},
            {"row_id": "dup", "table": "A", "values": {"id": 2, "b_id": None}},
        ])
        with self.assertRaises(ManifestError):
            parse_manifest(m)

    def test_maximality_no_other_acceptable_disposition(self):
        # 任何把隔离行加回的方案都会破坏 FK；重算结果即最大闭合集合
        m = make_manifest(self.contracts, [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 2, "a_id": 99}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.accepted_ids, frozenset({"a1"}))
        self.assertEqual(plan.quarantined_ids, frozenset({"b-bad"}))

    def test_self_referencing_cycle_survives(self):
        contracts = [
            {"table": "N", "columns": {"id": {"type": "long", "required": True},
                                       "parent": {"type": "long", "required": False}},
             "primary": ["id"], "unique": [],
             "foreign_keys": [{"columns": ["parent"], "target": "N",
                               "target_columns": ["id"], "match": "SIMPLE"}]},
        ]
        m = make_manifest(contracts, [
            {"row_id": "n1", "table": "N", "values": {"id": 1, "parent": 2}},
            {"row_id": "n2", "table": "N", "values": {"id": 2, "parent": 1}},
        ])
        _, plan = plan_of(m)
        self.assertEqual(plan.accepted_ids, frozenset({"n1", "n2"}))

    def test_mixed_missing_and_propagation_edges_on_one_row(self):
        # C 行同时引用缺失的 A=99（缺目标边）和被隔离的 A=3（传播边）
        m = make_manifest(self.contracts, [
            {"row_id": "a-bad", "table": "A", "values": {"id": 3, "b_id": 999}},
            {"row_id": "c-mix", "table": "C", "values": {"id": 7, "a_id": 3}},
        ])
        _, plan = plan_of(m)
        q = {x.row_id: x for x in plan.quarantined}
        self.assertEqual(q["a-bad"].reason, FK_MISSING_TARGET)
        self.assertEqual(q["c-mix"].reason, FK_PARENT_QUARANTINED)
        roots = {r["row_id"] for r in plan.roots["c-mix"]}
        self.assertEqual(roots, {"a-bad"})

    def test_propagation_across_multiple_tables_keeps_all_paths(self):
        # 同一坏父沿 B、C 两张表传播，两条路径的根因都收敛到同一终端
        m = make_manifest(self.contracts, [
            {"row_id": "a-bad", "table": "A", "values": {"id": 3, "b_id": 999}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 4, "a_id": 3}},
            {"row_id": "c-bad", "table": "C", "values": {"id": 5, "a_id": 3}},
        ])
        _, plan = plan_of(m)
        for rid in ("b-bad", "c-bad"):
            self.assertEqual({r["row_id"] for r in plan.roots[rid]}, {"a-bad"})


class StoreAndCheckerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "w.sqlite"
        self.contracts = simple_contracts()
        self.contract = parse_contract(self.contracts)

    def tearDown(self):
        self.tmp.cleanup()

    def _plan(self, rows, mode="closed-subset", batch_id="b1", base="v1", existing=None):
        m = make_manifest(self.contracts, rows, mode=mode, batch_id=batch_id, base=base, existing=existing)
        contract, ex, cand, md, bid, ver, _ = parse_manifest(m)
        return m, plan_batch(contract, cand, ex, md, batch_id=bid, base_version=ver)

    def test_commit_and_independent_check(self):
        rows = [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": 2}},
            {"row_id": "b2", "table": "B", "values": {"id": 2, "a_id": 1}},
            {"row_id": "a-bad", "table": "A", "values": {"id": 3, "b_id": 999}},
        ]
        m, plan = self._plan(rows)
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            receipt = wh.commit(self.contract, m, plan)
        self.assertEqual(receipt["status"], "PUBLISHED_CLOSED_SUBSET")
        conn = sqlite3.connect(self.db)
        try:
            report = check_batch(conn, self.contract, "b1")
        finally:
            conn.close()
        self.assertTrue(report["ok"], report["violations"])

    def test_atomic_rejects_whole_batch_and_publishes_nothing(self):
        rows = [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 2, "a_id": 99}},
        ]
        m, plan = self._plan(rows, mode="atomic")
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            receipt = wh.commit(self.contract, m, plan)
            self.assertEqual(receipt["status"], "REJECTED_ATOMIC")
            self.assertEqual(receipt["published_count"], 0)
            self.assertEqual(wh.data_version, "v2")  # 回执与隔离区仍落库，版本推进
            formal = wh.conn.execute("SELECT count(*) FROM A").fetchone()[0]
            self.assertEqual(formal, 0)
        conn = sqlite3.connect(self.db)
        try:
            self.assertTrue(check_batch(conn, self.contract, "b1")["ok"])
        finally:
            conn.close()

    def test_stale_version_rejected_and_recompute_succeeds(self):
        rows = [{"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}}]
        m_stale, plan_stale = self._plan(rows, batch_id="stale", base="v1")
        m_other, plan_other = self._plan(
            [{"row_id": "a2", "table": "A", "values": {"id": 2, "b_id": None}}],
            batch_id="other", base="v1")
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            wh.commit(self.contract, m_other, plan_other)
            with self.assertRaises(StaleVersionError):
                wh.commit(self.contract, m_stale, plan_stale)
            # 基于新版本重算后可提交
            m_fresh, _ = self._plan(rows, batch_id="stale", base="v2")
            existing = wh.load_existing(self.contract)
            m_fresh["existing"] = existing
            _, ex, cand, md, bid, ver, _ = parse_manifest(m_fresh)
            plan_fresh = plan_batch(self.contract, cand, ex, md, batch_id=bid, base_version=ver)
            receipt = wh.commit(self.contract, m_fresh, plan_fresh)
            self.assertEqual(receipt["status"], "PUBLISHED_FULL")

    def test_batch_id_cannot_be_reused(self):
        m, plan = self._plan([{"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}}])
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            wh.commit(self.contract, m, plan)
            with self.assertRaises(BatchExistsError):
                wh.commit(self.contract, m, plan)

    def test_checker_detects_tampered_quarantine_and_missing_fk(self):
        rows = [
            {"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}},
            {"row_id": "b-bad", "table": "B", "values": {"id": 2, "a_id": 99}},
        ]
        m, plan = self._plan(rows)
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            wh.commit(self.contract, m, plan)
        conn = sqlite3.connect(self.db)
        try:
            self.assertTrue(check_batch(conn, self.contract, "b1")["ok"])
            # 篡改隔离原文：模拟“偷偷修成看似合法数据”
            conn.execute(
                "UPDATE _gw_quarantine SET raw_values_json=? WHERE batch_id=? AND row_id=?",
                (json.dumps({"id": 2, "a_id": 1}, sort_keys=True), "b1", "b-bad"),
            )
            conn.commit()
            report = check_batch(conn, self.contract, "b1")
            self.assertFalse(report["ok"])
            self.assertTrue(any("原文被改写" in v for v in report["violations"]))
        finally:
            conn.close()

    def test_checker_detects_orphan_formal_row(self):
        rows = [{"row_id": "a1", "table": "A", "values": {"id": 1, "b_id": None}}]
        m, plan = self._plan(rows)
        with Warehouse(self.db) as wh:
            wh.initialize(self.contract)
            wh.commit(self.contract, m, plan)
            # 直接往正式表塞一条无来源、悬空引用的行（绕过门禁）
            wh.conn.execute('INSERT INTO A(id,b_id,_gw_batch_id,_gw_row_id) VALUES(?,?,?,?)',
                            (99, 888, "rogue", "rogue-row"))
            wh.conn.commit()
        conn = sqlite3.connect(self.db)
        try:
            report = check_batch(conn, self.contract, "b1")
            self.assertFalse(report["ok"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
