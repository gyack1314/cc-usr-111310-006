"""入仓关系质量门禁命令行：只读检查、闭包试算、原子提交、独立复核。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .checker import check_batch
from .contract import parse_contract
from .domain import summarize
from .engine import parse_manifest, plan_batch
from .io import load
from .store import Warehouse


def _print(obj: object) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_inspect(args: argparse.Namespace) -> int:
    v, digest = load(args.file)
    _print({"manifest_sha256": digest, "summary": summarize(v, Path(args.file).parent)})
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    v, digest = load(args.file)
    contract, existing, candidates, mode, batch_id, base_version, _contract_version = parse_manifest(v)
    plan = plan_batch(contract, candidates, existing, mode, batch_id=batch_id, base_version=base_version)
    receipt = plan.to_receipt()
    receipt["manifest_sha256"] = digest
    receipt["maximal_closure_evidence"] = {
        "fixpoint_passes": plan.passes,
        "accepted_fk_evidence": [
            {"row_id": r["row_id"], "fk_evidence": r["fk_evidence"],
             "fk_skipped_simple": r["fk_skipped_simple"]}
            for r in plan.accepted
        ],
        "quarantine": [
            {"row_id": q.row_id, "table": q.table, "wave": q.wave,
             "reason": q.reason, "details": q.details, "root_causes": plan.roots.get(q.row_id, [])}
            for q in plan.quarantined
        ],
    }
    _print(receipt)
    return 0


def cmd_commit(args: argparse.Namespace) -> int:
    v, _digest = load(args.file)
    contract, _existing, candidates, mode, batch_id, _declared_version, _cv = parse_manifest(v)
    with Warehouse(args.db) as wh:
        wh.initialize(contract)
        # 真实流程的基线版本取自当前库；清单里的 warehouse_version 仅作来源声明。
        base_version = wh.data_version
        v["warehouse_version"] = base_version
        plan = plan_batch(
            contract, candidates, wh.load_existing(contract), mode,
            batch_id=batch_id, base_version=base_version,
        )
        try:
            receipt = wh.commit(contract, v, plan)
        except Exception as exc:
            _print({"committed": False, "error_type": type(exc).__name__, "error": str(exc)})
            return 2
    _print({"committed": True, "receipt": receipt})
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    v, _digest = load(args.file)
    contract = parse_contract(v["contracts"])
    import sqlite3
    conn = sqlite3.connect(args.db)
    try:
        try:
            report = check_batch(conn, contract, args.batch_id)
        except Exception as exc:
            _print({"ok": False, "error_type": type(exc).__name__, "error": str(exc)})
            return 2
    finally:
        conn.close()
    _print(report)
    return 0 if report["ok"] else 1


def main() -> int:
    p = argparse.ArgumentParser(description="入仓关系契约批级门禁")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("inspect", help="只读摘要，不计算闭包")
    sp.add_argument("file")
    sp.set_defaults(func=cmd_inspect)

    sp = sub.add_parser("plan", help="计算最大闭合集合与根因证据，不写库")
    sp.add_argument("file")
    sp.set_defaults(func=cmd_plan)

    sp = sub.add_parser("commit", help="锁内重验证并原子发布/拒绝整批")
    sp.add_argument("file")
    sp.add_argument("--db", required=True)
    sp.set_defaults(func=cmd_commit)

    sp = sub.add_parser("check", help="独立检查器复核已提交批次")
    sp.add_argument("file")
    sp.add_argument("--db", required=True)
    sp.add_argument("--batch-id", required=True)
    sp.set_defaults(func=cmd_check)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
