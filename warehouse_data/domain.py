"""核对原始配置及数量，不执行仓库变换。"""
from .io import unique
def summarize(v,root):
    tables=unique(v['contracts'],'table');unique(v['candidates'],'row_id')
    if v['mode'] not in ('atomic','closed-subset'):raise ValueError('模式未知')
    if any(r['table'] not in tables for r in v['candidates']):raise ValueError('候选表未知')
    return {'tables':len(tables),'candidates':len(v['candidates']),'existing_rows':sum(map(len,v['existing'].values()))}
