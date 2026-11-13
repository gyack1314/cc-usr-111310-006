"""读取冻结JSON资料与校验原始文件身份。"""
import hashlib,json
from pathlib import Path

def load(path):
    raw=Path(path).read_bytes();v=json.loads(raw.decode('utf8'),parse_constant=lambda x:(_ for _ in ()).throw(ValueError('非有限常量')))
    if not isinstance(v,dict):raise ValueError('顶层必须为对象')
    return v,hashlib.sha256(raw).hexdigest()

def integer(x,minimum=0):
    if type(x) is not int or x<minimum:raise ValueError('整数越界')
    return x

def unique(rows,key):
    seen=set()
    for row in rows:
        x=row[key]
        if not isinstance(x,str) or not x or x in seen:raise ValueError('身份缺失或重复')
        seen.add(x)
    return seen

def asset(root,ref):
    base=Path(root).resolve();p=(base/ref['path']).resolve()
    if not p.is_relative_to(base):raise ValueError('文件路径越界')
    raw=p.read_bytes()
    if len(raw)!=integer(ref['bytes']) or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('资产身份不符')
    return raw

def canonical(value):
    """键排序、无空白的规范化JSON，用于稳定摘要；ensure_ascii=False保持原值可复核。"""
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))

def digest_json(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()

def is_long(x):
    # bool 是 int 子类，long 严格不接收 bool
    return type(x) is int

def is_string(x):
    return type(x) is str

def is_bool(x):
    return type(x) is bool

def is_null(x):
    return x is None

TYPE_PREDICATES={'long':is_long,'string':is_string,'bool':is_bool}
