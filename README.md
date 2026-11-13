# 仓库关系契约的整批隔离闭包

此项目保存生产与供应链协同的湖仓原始资料，并提供批级关系契约质量门禁。`warehouse_data.io`读取JSON清单及核对文件身份，`warehouse_data.domain`检查基本形状并输出数量，`warehouse_data.contract`解析不可变契约并计算契约摘要，`warehouse_data.engine`求最大闭合集合与根因链（纯函数、顺序无关），`warehouse_data.store`在SQLite单事务内原子发布/拒绝并做锁内重验证，`warehouse_data.checker`是与引擎零代码共享的独立检查器。[资料约定](docs/data-contract.md)定义字段、类型、唯一键NULLS DISTINCT、外键MATCH SIMPLE、不动点闭包与原因链语义。设备、批次、组织、事件和成本均为合成，不连接真实工厂或生产仓库。

Python3.11以上，无第三方Python依赖。项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse_data tests
python -m warehouse_data.cli inspect data/normal.json
python -m warehouse_data.cli plan data/normal.json
python -m warehouse_data.cli commit data/normal.json --db /tmp/warehouse.sqlite
python -m warehouse_data.cli check data/normal.json --db /tmp/warehouse.sqlite --batch-id bc1
python demo_gate.py
```

- `inspect`只读原始资料，不生成仓库新版本或计算领域结果。
- `plan`计算最大闭合集合、不动点波次、每条接受行的外键证据与每条隔离行的终端根因，不写库。
- `commit`取写锁、校验数据版本并在锁内重新验证后原子发布；atomic模式遇任一隔离整批拒绝（`REJECTED_ATOMIC`），closed-subset只发布闭合集合（`PUBLISHED_CLOSED_SUBSET`/`PUBLISHED_FULL`）。计算后旧库变化会触发StaleVersionError，须基于新版本重算，不能用旧结果覆盖。
- `check`用独立检查器复核已提交批次：接受/隔离集合、引用可达性、根因链终点、隔离原文未被篡改、模式回执与版本链。
- `demo_gate.py`端到端演示双表合法环、同键两条内容不同新行整组隔离、三层坏父传播、复合外键部分null（MATCH SIMPLE）与空串非null、atomic/closed-subset回执差异、并发版本失效及隔离原文保全。

`invalid-config.json`有基本形状错误；其余清单有完整资料身份，但可能包含需要业务操作判断的冲突、缺口或不满足条件的边界。null与缺字段分别保留，有理数和decimal用字符串存储，不能先经float丢精度。隔离记录保存候选原文，门禁不会把隔离数据“修成”看似合法的正式数据。

Dockerfile固定Linux镜像，`docker build -t warehouse-material .`构建；`docker run --rm warehouse-material`显示摘要；`docker run --rm warehouse-material python -m unittest discover -s tests -v`运行已有检查。所有资料本地可读，不需要账号、外部数据库或在线数据源。
