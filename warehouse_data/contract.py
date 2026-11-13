"""关系契约解析与契约摘要。

契约是不可变配置：每表登记列类型/必填、主键、唯一键与外键。
外键目标必须精确匹配某张表已登记的主键或唯一键，MATCH 仅支持 SIMPLE。
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from .io import canonical

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
TYPES = ("long", "string", "bool")
MATCH_SIMPLE = "SIMPLE"


class ContractError(ValueError):
    """契约自身不合法，属于配置错误而非候选数据错误。"""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    target_table: str
    target_columns: tuple[str, ...]
    match: str


@dataclass(frozen=True)
class TableContract:
    table: str
    columns: tuple[Column, ...]
    primary: tuple[str, ...]
    unique: tuple[tuple[str, ...], ...]
    foreign_keys: tuple[ForeignKey, ...]

    @property
    def column_map(self) -> dict[str, Column]:
        return {c.name: c for c in self.columns}

    @property
    def registered_keys(self) -> tuple[tuple[str, ...], ...]:
        return (self.primary, *self.unique)


@dataclass(frozen=True)
class Contract:
    tables: tuple[TableContract, ...]
    digest: str

    @property
    def by_table(self) -> dict[str, TableContract]:
        return {t.table: t for t in self.tables}

    def to_json(self) -> dict[str, Any]:
        return {
            t.table: {
                "columns": {
                    c.name: {"type": c.type, "required": c.required} for c in t.columns
                },
                "primary": list(t.primary),
                "unique": [list(k) for k in t.unique],
                "foreign_keys": [
                    {
                        "columns": list(fk.columns),
                        "target": fk.target_table,
                        "target_columns": list(fk.target_columns),
                        "match": fk.match,
                    }
                    for fk in t.foreign_keys
                ],
            }
            for t in self.tables
        }


def _identifier(name: Any, what: str) -> str:
    if not isinstance(name, str) or not IDENT.match(name):
        raise ContractError(f"{what}标识符不合法: {name!r}")
    return name


def parse_contract(raw_contracts: list[dict[str, Any]]) -> Contract:
    """把清单中的 contracts 段解析成不可变 Contract，并计算契约摘要。"""
    if not isinstance(raw_contracts, list) or not raw_contracts:
        raise ContractError("contracts 必须是非空数组")
    tables: list[TableContract] = []
    seen_tables: set[str] = set()
    for raw in raw_contracts:
        if not isinstance(raw, dict):
            raise ContractError("表契约必须是对象")
        table = _identifier(raw.get("table"), "表")
        if table in seen_tables:
            raise ContractError(f"表契约重复: {table}")
        seen_tables.add(table)

        raw_cols = raw.get("columns")
        if not isinstance(raw_cols, dict) or not raw_cols:
            raise ContractError(f"表 {table} 必须声明至少一列")
        columns: list[Column] = []
        seen_cols: set[str] = set()
        for cname, spec in raw_cols.items():
            _identifier(cname, "列")
            if cname in seen_cols:
                raise ContractError(f"表 {table} 列重复: {cname}")
            seen_cols.add(cname)
            if not isinstance(spec, dict):
                raise ContractError(f"列 {table}.{cname} 规格必须是对象")
            ctype = spec.get("type")
            if ctype not in TYPES:
                raise ContractError(f"列 {table}.{cname} 类型未知: {ctype!r}")
            columns.append(Column(cname, ctype, bool(spec.get("required", False))))

        def parse_key(value: Any, what: str) -> tuple[str, ...]:
            if not isinstance(value, list) or not value:
                raise ContractError(f"表 {table} 的 {what} 必须是非空数组")
            key = tuple(_identifier(c, what) for c in value)
            if len(set(key)) != len(key):
                raise ContractError(f"表 {table} 的 {what} 含重复列")
            unknown = [c for c in key if c not in seen_cols]
            if unknown:
                raise ContractError(f"表 {table} 的 {what} 引用未知列: {unknown}")
            return key

        primary = parse_key(raw.get("primary"), "primary")
        col_by_name = {c.name: c for c in columns}
        for cname in primary:
            if not col_by_name[cname].required:
                raise ContractError(f"表 {table} 主键列 {cname} 必须 required 且不接受 null")
        uniques: list[tuple[str, ...]] = []
        for uk in raw.get("unique", []) or []:
            key = parse_key(uk, "unique")
            if key == primary or key in uniques:
                raise ContractError(f"表 {table} 唯一键重复登记: {key}")
            uniques.append(key)

        fks: list[ForeignKey] = []
        for fk_spec in raw.get("foreign_keys", []) or []:
            if not isinstance(fk_spec, dict):
                raise ContractError(f"表 {table} 外键必须是对象")
            cols = parse_key(fk_spec.get("columns"), "foreign_key")
            target = _identifier(fk_spec.get("target"), "外键目标表")
            tcols_raw = fk_spec.get("target_columns")
            if not isinstance(tcols_raw, list) or not tcols_raw:
                raise ContractError(f"表 {table} 外键目标列必须是非空数组")
            tcols = tuple(_identifier(c, "外键目标列") for c in tcols_raw)
            match = fk_spec.get("match", MATCH_SIMPLE)
            if match != MATCH_SIMPLE:
                raise ContractError(f"仅支持 MATCH SIMPLE，收到 {match!r}")
            fks.append(ForeignKey(cols, target, tcols, match))
        tables.append(
            TableContract(table, tuple(columns), primary, tuple(uniques), tuple(fks))
        )

    by_table = {t.table: t for t in tables}
    # 跨表校验放在所有表解析完之后：外键目标必须是登记主键/唯一键，列数类型对齐。
    for tc in tables:
        for fk in tc.foreign_keys:
            target_tc = by_table.get(fk.target_table)
            if target_tc is None:
                raise ContractError(f"表 {tc.table} 外键目标表不存在: {fk.target_table}")
            if fk.target_columns not in target_tc.registered_keys:
                raise ContractError(
                    f"表 {tc.table} 外键目标 {fk.target_table}{list(fk.target_columns)} "
                    "不是已登记主键或唯一键"
                )
            if len(fk.columns) != len(fk.target_columns):
                raise ContractError(f"表 {tc.table} 外键列数与目标列数不一致")
            src_map = tc.column_map
            dst_map = target_tc.column_map
            for s, d in zip(fk.columns, fk.target_columns):
                if src_map[s].type != dst_map[d].type:
                    raise ContractError(
                        f"表 {tc.table}.{s} 与目标 {fk.target_table}.{d} 类型不一致"
                    )

    normalized = {
        t.table: {
            "columns": {
                c.name: {"required": c.required, "type": c.type} for c in t.columns
            },
            "foreign_keys": [
                {
                    "columns": list(f.columns),
                    "match": f.match,
                    "target": f.target_table,
                    "target_columns": list(f.target_columns),
                }
                for f in t.foreign_keys
            ],
            "primary": list(t.primary),
            "unique": [list(k) for k in t.unique],
        }
        for t in tables
    }
    digest = hashlib.sha256(canonical(normalized).encode("utf-8")).hexdigest()
    return Contract(tuple(tables), digest)
