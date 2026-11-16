# 多表异步入湖：公开上下文与交付约定

核账人员用 samples 的站点整数观测量与 qc 的质检状态做关联，累加质检为 ok 的观测量。同一源事务可能同时更正观测和质检；两张表独立落地，当前查询会在两次落地之间读到混合结果。本仓库是该现状的参考模型，不是一次真实生产事故的取证材料。

## 当前入口、数据与职责

仅依赖 Python 标准库和 SQLite。Catalog 与每个 Table 各有自己的 SQLite 连接和独立内存库，Warehouse 实例关闭即丢弃。持久化、重开恢复不在本题范围，不能把内存库误称为持久存储。
- warehouse.Warehouse() 创建应用；declare(tid, seq, tables)、land(tid, table, rows)、capture()、close() 是业务入口。
- tid 为非空字符串；seq 为 1 至 2^63-1 的整数（不接受布尔值），首次声明按源序号严格增加，允许序号有空洞。tables 是 samples/qc 的非空无重复子集。
- 一个涉及表只有一个完整片段。rows 是 [key,value] 的列表；key 为非空字符串，片段内不能重复。samples 的 value 是整数（不接受布尔值）或 null；qc 是 ok、bad 或 null。null 是删除版本，空列表是完整空片段。
- 同一声明或片段同内容重放幂等；片段行顺序不构成内容差异，异内容重放拒绝。Warehouse 只接受已声明的事务和涉及表，非法输入或冲突抛 ValueError。片段在单表内全成或全败。
- landing.Table.land 原子保存不可变片段和版本，返回 (tid, table, digest)；scan_versions 返回 (tid, seq, key, value) 列表。digest 是规范化片段的摘要，幂等落地与回执已实现。
- catalog.Catalog 保存声明及按 (tid,table) 去重的回执；它目前没有共同可见性目录。其 declaration/receipts 可点查，不提供目标快照算法。
- warehouse.Warehouse.land 先完成 Table.land，再调用 Catalog.record。声明、落地、capture、read 都串行调用。设计可以改变内部接线及增加元数据，保留上述 Warehouse 入口、片段格式和已有错误语义。
- query.Query.capture 目前只捕获表对象引用。Handle.read 每次独立扫描两表，按每键最大源序号取版本，再过滤墓碑。它不使用目录回执，也不固定逻辑视图。
- read 返回 {"samples":[[key,value],...],"qc":[[key,value],...],"total":整数}。两表列表按 key 排序，只含非墓碑胜者；total 是 samples 与 qc 按 key 关联后 qc=ok 的金额和，缺少任一侧不计入。负数及零保留，不做业务金额修正。

## 本轮目标契约

只交付带实际源码行号的 DESIGN.md 及可重跑观察证据，不实现多表可见性功能。限定两张湖表、一个串行可见性裁决者；两张表可以独立落地，源事务声明给出唯一事务ID、全局递增源序号及涉及表集合，每个涉及表恰有一个完整片段。

每个源事务只有全部声明片段成功落地后才有资格可见；capture取得当时所有已齐备事务构成的固定视图，不能等待较早但未齐备的事务，之后到达的片段不得改变旧句柄的查询结果。这里保证落地事务集合的一致性，不承诺该集合是源日志连续前缀。在本题无故障串行模型中，最后一次 land 成功返回即已完成本事务的可见性裁决，紧随其后的 capture 不能遗漏它。

每张表按业务主键，在句柄可见事务中选择源序号最大的版本，再执行原有查询与跨表关联；删除版本参与胜出，不能先丢弃删除再选旧值。较早事务迟到齐备不得覆盖已经可见的新值或复活已删除行。同一事务的两个表片段不能在任何新句柄中只出现一侧。空片段也必须有回执；只涉及一表的事务在该表片段成功落地后即齐备。

capture不得读取表载荷、枚举全部事务或复制全部历史；发布一笔新齐备事务只能处理其修改键及必要元数据路径，不能复制整表。载荷写入沿用基线机制，不得跨载荷I/O持目录写事务；查询可以扫描选定视图，不要求新增查询优化器。分析须区分源序号与齐备先后，并计入历史资格、索引路径、版本或节点的保存成本，不能把结构共享的节点复制写成零成本。

方案须定位声明、单表落地回执、可见性裁决、capture和跨表查询的调用链，比较至少两种同契约可行的状态组织，给出推荐依据、必要状态、接口交接、关键伪代码与成本。对两种组织分别解释迟到版本、墓碑与旧句柄的不变量和操作位置；不能用“取各表最新”或“最小完成序号前缀”作为可行替代方案。设计须明确裁决如何生效到查询，以及完整片段已落地但事务未齐备时查询如何处理其中较新版本。

不涉及跨进程、多裁决者、崩溃恢复、高可用、源日志连续前缀、跨事务因果闭包、载荷回收、模式演进或目标协议生产代码。允许添加观察和文档检查辅助代码，不得修改 landing.py、catalog.py、warehouse.py、query.py、scenarios.json、replay.py、checks.py、review.py 或删除原断言。

## 公开事件与期望

scenarios.json 是全部公开录制。每组 declarations 是 [tid,seq,tables] 列表，events 按数组先后执行；事件为 ["land",tid,table,rows]、["capture",句柄名] 或 ["read",句柄名]。步骤从 1 起数。每组从空仓开始，读输出固定为 {step,handle,result}；baseline_reads 是现状预期，target_reads 额外带 visible（按源序号排列的已齐备事务ID）。visible 是设计证据，不要求给生产 Handle 新增公开方法。

partial 中 T1=(x=10,ok) 齐备后取得 old；T2 的 samples=x20 先到，middle 此时目标仍读 10，而基线读 20。T2 的 qc=bad 到达后，旧句柄仍应读 10，新句柄 total=0。
overtake 中 T2 只落一侧、T3 的 y=30/ok 已齐备；H 必须含 T1、T3，total=40，不能被 T2 阻塞。T2 后续齐备不改变 H，新 J 的 total=30。
late_delete 中 T8 删除 x 且质检 bad 先齐备，T7 的 x=70/ok 后齐备；新旧句柄均不可复活 x，但其可见事务集合不同。
pending_delete 中 T2 的删除只落 samples；H 仍须保留 T1 的 x=10，随后 T2 齐备不能改 H，新 J 才读到删除。此例用于区分资格过滤与版本胜出的先后。

用公开事件顺序核对基线实际输出和方案预测，覆盖单侧先到、后续事务先齐备、同键删除与旧值迟到、旧句柄重复读取；保留业务源码和基础回归。DESIGN.md 须说明两种方案逐事件的关键状态、齐备集合、句柄表示及每次 read 的结果，标清哪些已运行、哪些仅是设计推演。除样例外，以伪代码访问对象和不变量说明资源界限，不要求实现性能基准或目标算法。

## 运行和交付核验

python3 checks.py 执行业务回归；python3 replay.py --check 重放并核对 baseline_reads，输出完整实测结果。现有 .pointwise-runtime.json 的 verify/demo 分别运行这两者，setup 为空；它们在初始仓库即可成功。

提交的 DESIGN.md 包含一个 visibility-evidence 围栏，内容为 JSON 对象：
- observations：python3 replay.py --check 输出的原样 JSON 对象，不可把目标值填作实测。
- predictions：按场景名映射到逐 read 的预测数组，字段与 target_reads 相同。公开 target_reads 是核账锚点，不是协议实现；复制结果不能替代两方案的状态推导。
文档代码定位采用 landing.py:行号、catalog.py:行号、warehouse.py:行号、query.py:行号，引用真实有关语句，不得只引用文件标题。

交付时将运行清单的 verify 改为 ["python3","review.py"]、demo 改为 ["python3","review.py","--show"]，其余字段保留。review.py 重跑业务回归和观察，核对文档中的实测、公开预测锚点及行号是否存在；demo 展示两者真实差异。该检查不验证伪代码、取舍或渐进复杂度，结构检查通过不能冒充协议正确；设计正确性、调用链关联与成本须独立人工审查。
