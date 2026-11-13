"""SQLite 湖仓存储与原子发布。

一次提交在单个事务里写：正式表、隔离区、原因边、批回执、批候选原文。
提交前 BEGIN IMMEDIATE 取写锁，校验当前数据版本 == 计划基准版本，
并在锁内用当前数据重新跑一遍门禁；计划摘要不一致即拒绝，绝不用旧结果覆盖。
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .contract import Contract
from .engine import (
    MODES,
    Plan,
    parse_manifest,
    plan_batch,
)
from .io import canonical, digest_json

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

PUBLISHED_CLOSED = "PUBLISHED_CLOSED_SUBSET"
PUBLISHED_FULL = "PUBLISHED_FULL"
REJECTED_ATOMIC = "REJECTED_ATOMIC"


class StaleVersionError(RuntimeError):
    """依赖版本已变化，原计划不得提交。"""


class BatchExistsError(RuntimeError):
    """同 batch_id 已提交过；批身份不可复用。"""


class StoreError(RuntimeError):
    """存储层其他错误。"""


def _quote_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise StoreError(f"标识符不合法: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _sql_type(t: str) -> str:
    return {"long": "INTEGER", "string": "TEXT", "bool": "INTEGER"}[t]


class Warehouse:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Warehouse":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------- 初始化 ----------------
    def initialize(self, contract: Contract) -> None:
        c = self.conn
        c.execute(
            "CREATE TABLE IF NOT EXISTS _gw_meta(meta_key TEXT PRIMARY KEY, meta_value TEXT NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS _gw_batches("
            "batch_id TEXT PRIMARY KEY, mode TEXT NOT NULL, base_version TEXT NOT NULL, "
            "contract_digest TEXT NOT NULL, candidate_digest TEXT NOT NULL, plan_digest TEXT NOT NULL, "
            "status TEXT NOT NULL, receipt_json TEXT NOT NULL, committed_version TEXT, "
            "created_at TEXT NOT NULL DEFAULT (datetime('now')))"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS _gw_batch_candidates("
            "batch_id TEXT NOT NULL, row_id TEXT NOT NULL, table_name TEXT NOT NULL, "
            "raw_json TEXT NOT NULL, ordinal INTEGER NOT NULL, "
            "PRIMARY KEY(batch_id,row_id)) WITHOUT ROWID"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS _gw_quarantine("
            "batch_id TEXT NOT NULL, row_id TEXT NOT NULL, table_name TEXT NOT NULL, wave INTEGER NOT NULL, "
            "reason TEXT NOT NULL, details_json TEXT NOT NULL, raw_values_json TEXT NOT NULL, "
            "PRIMARY KEY(batch_id,row_id)) WITHOUT ROWID"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS _gw_reason_edges("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, "
            "source_row_id TEXT NOT NULL, source_table TEXT NOT NULL, "
            "fk_columns_json TEXT NOT NULL, target_table TEXT NOT NULL, target_columns_json TEXT NOT NULL, "
            "key_tuple_json TEXT NOT NULL, reason TEXT NOT NULL, target_row_id TEXT, wave INTEGER NOT NULL)"
        )
        for tc in contract.tables:
            cols = ", ".join(
                f"{_quote_ident(col.name)} {_sql_type(col.type)}" for col in tc.columns
            )
            c.execute(
                f"CREATE TABLE IF NOT EXISTS {_quote_ident(tc.table)}("
                f"{cols}, _gw_batch_id TEXT NOT NULL, _gw_row_id TEXT NOT NULL)"
            )
            c.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident('uq_' + tc.table + '_rowid')} "
                f"ON {_quote_ident(tc.table)}(_gw_row_id)"
            )
        stored = self._meta("contract_digest")
        if stored is None:
            self._put_meta("contract_digest", contract.digest)
            self._put_meta("data_version", "v1")
        elif stored != contract.digest:
            raise StoreError("仓库契约摘要与初始化契约不一致，禁止混用契约")
        self.conn.commit()

    def _meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT meta_value FROM _gw_meta WHERE meta_key=?", (key,)
        ).fetchone()
        return None if row is None else row["meta_value"]

    def _put_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO _gw_meta(meta_key,meta_value) VALUES(?,?) "
            "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value",
            (key, value),
        )

    @property
    def data_version(self) -> str:
        v = self._meta("data_version")
        if v is None:
            raise StoreError("仓库未初始化")
        return v

    def load_existing(self, contract: Contract) -> dict[str, list[dict[str, Any]]]:
        """读出正式表当前全部有效行（剥离门禁内部列，bool 按契约还原）。"""
        out: dict[str, list[dict[str, Any]]] = {}
        for tc in contract.tables:
            col_sql = ", ".join(_quote_ident(col.name) for col in tc.columns)
            rows = self.conn.execute(
                f"SELECT {col_sql} FROM {_quote_ident(tc.table)}"
            ).fetchall()
            decoded: list[dict[str, Any]] = []
            for r in rows:
                item = dict(r)
                for col in tc.columns:
                    if col.type == "bool" and item[col.name] is not None:
                        item[col.name] = bool(item[col.name])
                decoded.append(item)
            out[tc.table] = decoded
        return out

    # ---------------- 原子提交 ----------------
    def commit(
        self,
        contract: Contract,
        manifest: dict[str, Any],
        plan: Plan,
    ) -> dict[str, Any]:
        if plan.mode not in MODES:
            raise StoreError(f"模式未知: {plan.mode}")
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            # 1) 批身份不可复用
            dup = c.execute(
                "SELECT 1 FROM _gw_batches WHERE batch_id=?", (plan.batch_id,)
            ).fetchone()
            if dup is not None:
                raise BatchExistsError(f"batch_id 已存在: {plan.batch_id}")
            # 2) 依赖版本：当前数据版本必须仍是计划基准版本
            current_version = self.data_version
            if current_version != plan.base_version:
                raise StaleVersionError(
                    f"数据版本已从 {plan.base_version} 变为 {current_version}，计划失效"
                )
            # 3) 锁内用当前库重新解析、重新计算，比对计划摘要，不能用旧结果覆盖
            re_contract, re_existing, re_candidates, re_mode, re_batch, re_base, re_cv = parse_manifest(manifest)
            if re_contract.digest != contract.digest or re_contract.digest != plan.contract_digest:
                raise StaleVersionError("契约摘要与计划不一致")
            if re_mode != plan.mode or re_batch != plan.batch_id or re_base != plan.base_version:
                raise StaleVersionError("清单身份与计划不一致")
            fresh = plan_batch(
                re_contract, re_candidates, self.load_existing(contract), re_mode,
                batch_id=re_batch, base_version=current_version,
            )
            if (fresh.plan_digest != plan.plan_digest
                    or fresh.candidate_digest != plan.candidate_digest):
                raise StaleVersionError(
                    "锁内重新验证的闭包结果与原计划不一致（旧库在计算后发生变化）"
                )

            # 4) 同事务落库
            self._persist_candidates(contract, manifest, plan)
            self._persist_quarantine(plan)
            self._persist_edges(plan)
            published: list[dict[str, Any]] = []
            if plan.mode == "atomic":
                status = REJECTED_ATOMIC if plan.quarantined else PUBLISHED_FULL
                if not plan.quarantined:
                    published = self._insert_accepted(contract, plan)
            else:
                status = PUBLISHED_FULL if not plan.quarantined else PUBLISHED_CLOSED
                published = self._insert_accepted(contract, plan)

            next_version = "v" + str(int(current_version.lstrip("v")) + 1)
            receipt = self._build_receipt(plan, status, current_version, next_version, published)
            c.execute(
                "INSERT INTO _gw_batches(batch_id,mode,base_version,contract_digest,candidate_digest,"
                "plan_digest,status,receipt_json,committed_version) VALUES(?,?,?,?,?,?,?,?,?)",
                (plan.batch_id, plan.mode, current_version, plan.contract_digest,
                 plan.candidate_digest, plan.plan_digest, status,
                 canonical(receipt), next_version),
            )
            self._put_meta("data_version", next_version)
            c.commit()
            return receipt
        except Exception:
            c.rollback()
            raise

    def _persist_candidates(self, contract: Contract, manifest: dict[str, Any], plan: Plan) -> None:
        for ordinal, cand in enumerate(manifest["candidates"]):
            self.conn.execute(
                "INSERT INTO _gw_batch_candidates(batch_id,row_id,table_name,raw_json,ordinal) "
                "VALUES(?,?,?,?,?)",
                (plan.batch_id, cand["row_id"], cand["table"], canonical(cand["values"]), ordinal),
            )

    def _persist_quarantine(self, plan: Plan) -> None:
        for q in plan.quarantined:
            self.conn.execute(
                "INSERT INTO _gw_quarantine(batch_id,row_id,table_name,wave,reason,details_json,raw_values_json) "
                "VALUES(?,?,?,?,?,?,?)",
                (plan.batch_id, q.row_id, q.table, q.wave, q.reason,
                 canonical(q.details), canonical(q.values)),
            )

    def _persist_edges(self, plan: Plan) -> None:
        for e in plan.edges:
            self.conn.execute(
                "INSERT INTO _gw_reason_edges(batch_id,source_row_id,source_table,fk_columns_json,"
                "target_table,target_columns_json,key_tuple_json,reason,target_row_id,wave) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (plan.batch_id, e.source_row_id, e.source_table, canonical(list(e.fk_columns)),
                 e.target_table, canonical(list(e.target_columns)), canonical(e.key_tuple),
                 e.reason, e.target_row_id, e.wave),
            )

    def _insert_accepted(self, contract: Contract, plan: Plan) -> list[dict[str, Any]]:
        published: list[dict[str, Any]] = []
        for row in plan.accepted:
            tc = contract.by_table[row["table"]]
            col_names = [col.name for col in tc.columns]
            placeholders = ", ".join("?" for _ in col_names)
            col_sql = ", ".join(_quote_ident(n) for n in col_names)
            params: list[Any] = [row["values"].get(n) for n in col_names]
            # bool 以 0/1 落 INTEGER，读回时按契约还原
            for i, col in enumerate(tc.columns):
                if col.type == "bool" and params[i] is not None:
                    params[i] = int(params[i])
            self.conn.execute(
                f"INSERT INTO {_quote_ident(tc.table)}({col_sql}, _gw_batch_id, _gw_row_id) "
                f"VALUES({placeholders}, ?, ?)",
                (*params, plan.batch_id, row["row_id"]),
            )
            published.append({"row_id": row["row_id"], "table": row["table"]})
        return published

    def _build_receipt(
        self, plan: Plan, status: str, base_version: str, next_version: str,
        published: list[dict[str, Any]],
    ) -> dict[str, Any]:
        receipt = plan.to_receipt()
        receipt.update({
            "status": status,
            "candidate_count": len(plan.accepted) + len(plan.quarantined),
            "accepted_count": len(plan.accepted),
            "quarantined_count": len(plan.quarantined),
            "published_count": len(published),
            "published_rows": published,
            "quarantine": [asdict(q) for q in plan.quarantined],
            "reason_edges": [asdict(e) for e in plan.edges],
            "committed_from_version": base_version,
            "committed_version": next_version,
        })
        receipt["receipt_digest"] = digest_json(
            {k: v for k, v in receipt.items() if k != "receipt_digest"}
        )
        return receipt

    # ---------------- 读侧（检查器/演示用） ----------------
    def get_receipt(self, batch_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT receipt_json FROM _gw_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        return None if row is None else json.loads(row["receipt_json"])

    def list_batches(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT batch_id,mode,status,base_version,committed_version,plan_digest "
            "FROM _gw_batches ORDER BY rowid"
        ).fetchall()
        return [dict(r) for r in rows]

    def load_candidate_raw(self, batch_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT row_id,table_name,raw_json,ordinal FROM _gw_batch_candidates "
            "WHERE batch_id=? ORDER BY ordinal", (batch_id,)
        ).fetchall()
        return [
            {"row_id": r["row_id"], "table": r["table_name"],
             "values": json.loads(r["raw_json"])}
            for r in rows
        ]
