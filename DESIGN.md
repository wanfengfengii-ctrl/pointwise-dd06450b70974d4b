# 多表异步入湖的可见性裁决设计

## 0. 范围、结论与运行清单

本轮只交付设计、源码定位和可重跑观察，不实现多表可见性协议，也不修改
`landing.py`、`catalog.py`、`warehouse.py`、`query.py`、`scenarios.json`、
`replay.py`、`checks.py`、`review.py`。现有单表片段、回执、幂等重放和查询输出格式保持不变。
限定两张湖表 `samples`、`qc`，以及一个串行可见性裁决者；不讨论跨进程、多裁决者、崩溃恢复、
高可用、源日志连续前缀、跨事务因果闭包、载荷回收、模式演进或查询优化器。

核心问题不是“各表取最新版本”。观测和质检可以独立落地，基线的每个句柄只持有表对象引用
（`query.py:29-31`），每次 `read` 都重新扫描当前全部版本（`query.py:8-16`、`landing.py:56-59`），
因此两次落地之间能读到半笔更正。目标是把“哪些源事务已经齐备”固定成一个不可变逻辑视图，
再在该视图中按业务键选择最大源序号。

结论：在两种都满足契约的组织中，推荐 **方案 A：目录侧不可变胜者树 + epoch 根指针**。
方案 B（发布候选日志 + 读时 epoch 过滤）实现更简单、发布和存储更省，但旧句柄的读路径会随
可见发布历史增长；本任务明确要求旧句柄反复读取且结果固定，因此优先选择读路径有界的 A。

可重跑命令与结果：

- `python3 checks.py`：5 项基线回归通过。
- `python3 replay.py --check`：实际输出与 `scenarios.json` 的 `baseline_reads` 全等。
- `python3 review.py`：重跑回归与观察，核对本文实测、公开预测锚点和源码行号。
- `python3 review.py --show`：展示实测基线与目标预测的差异；这两个命令的接线见第 7 节。

`visibility-evidence` 中的 `observations` 是本次实际运行 `python3 replay.py --check` 的原样结果；
`predictions` 是按本设计逐事件推演的目标结果，与公开 `target_reads` 锚点一致。后者没有作为生产实现运行过，
只是设计推导，不能当作实测。

## 1. 基线调用链与失配点

### 1.1 声明、源序号与回执

- `warehouse.py:13-14` 把 `declare` 直接转给 `Catalog.declare`。
- `catalog.py:11` 保存 `declarations(tid, seq, tables)`；`catalog.py:12-13` 保存
  `receipts(tid, table_name, digest)`。声明确立唯一 `tid`、全局递增源序号和涉及表集合。
- `catalog.py:29-31` 用 `COALESCE(MAX(seq),0)` 要求新声明的 `seq` 大于此前最大声明序号，
  因此源序号在全仓唯一且严格递增；但源序号只描述源日志位置，不描述落地或齐备先后。
- `catalog.py:35-40` 的 `declaration(tid)` 按 `tid` 点查 `(seq, tables)`；`catalog.py:42-52`
  的 `record(receipt)` 按 `(tid, table)` 去重并检查 digest 冲突。这里的回执只回答“这张表的片段
  成功过”，并不回答“该事务声明的全部表是否都齐备”，也没有共同可见性目录（见 `catalog.py:1`
  与 `catalog.py:54-57`）。
- `warehouse.py:16-22` 的 `land` 是唯一落地位：先取声明（`warehouse.py:17`），写单表载荷
  （`warehouse.py:20`），再记录回执（`warehouse.py:21`），最后返回回执。载荷 I/O 已在
  `landing.py:50-53` 的 `with self.db` 中提交；回执和未来的裁决元数据在其后写入，不跨载荷 I/O
  持目录写事务。

### 1.2 单表片段与原子性

- `landing.py:20` 的 `fragments(tid, seq, digest)` 以 `tid` 为主键；`landing.py:21-22` 的
  `versions(tid, seq, key, payload)` 以 `(tid, key)` 为主键。一个源事务在一张表中恰有一个完整片段。
- `landing.py:25-41` 校验行：`[key,value]`、键非空且片段内不重复；`samples` 只接受整数或 null，
  `qc` 只接受 `ok`、`bad` 或 null。null 是删除版本，空列表是完整空片段。
- `landing.py:42-43` 规范化片段并算 digest；`landing.py:44-49` 对同内容重放返回同一回执，异内容抛
  `ValueError`；`landing.py:50-53` 在单个 SQLite 事务中同时写 `fragments` 与 `versions`，因此单表
  片段全成或全败。空片段也会写 `fragments`，所以空片段仍有回执。
- 基线的 `scan_versions`（`landing.py:56-59`）按 `seq,key` 返回全部已落地版本；它不知道事务是否齐备。
  这正是“完整片段已落地但事务未齐备”的版本仍可被基线查询读到的原因。

### 1.3 基线的可见性失配

- `query.Handle.__init__` 只保存表对象引用（`query.py:5-6`），`Query.capture` 原样返回这些引用
  （`query.py:29-31`），没有固定逻辑视图。
- `Handle.read` 对每张表独立扫描全部版本，按每键最大 `seq` 选胜者（`query.py:12-14`），之后才过滤
  null（`query.py:15-16`）。它既没有“已齐备事务”过滤，也没有跨表原子视图；后到的落地会改变旧句柄。
- 目标设计要保留 `Warehouse` 的公开入口、片段格式和错误语义，但在 `land` 返回前完成一次串行裁决，
  并让 `capture` 只取当时已齐备事务的固定快照。

## 2. 术语与全局不变量

- `seq`：源序号，声明时给定，唯一且递增；只用于同键版本的新旧比较。
- `ready`：事务声明的每个表都已有成功回执（空片段也算）。只涉及一表的事务在该片段落地后立即齐备。
- `epoch`：齐备发布顺序号，由串行裁决者递增分配；只用于定义“某句柄能看到哪些发布”，绝不用于比较版本新旧。
- `visible(E)`：截至 epoch `E` 已齐备的事务集合；它不必是源序号前缀。
- `handle`：`(epoch, 每表根)` 的不可变值；旧句柄只读，不随后续发布变化。方案 B 的句柄只含 `epoch`。
- `winner(table,key,E)`：先限定 `visible(E)` 中的版本，再取 `seq` 最大者；墓碑（null）是普通候选，
  先参与胜出，查询输出阶段才丢弃。

全局不变量：

1. **齐备才可见**：未齐备事务的任何片段都不进入任何 epoch 的胜者状态，即使其 `seq` 比当前可见版本大。
2. **同事务两表原子**：一个事务的两张表片段只会在同一个发布 epoch 中一起进入视图，不会出现半笔更正。
3. **旧句柄固定**：epoch 发布后不可修改；旧句柄读取只使用其捕获时的 epoch/根。
4. **墓碑先胜出**：删除版本参与每键最大 `seq` 竞争，不能在竞争前丢弃；只有胜者确定后才在输出中过滤 null。
5. **迟到旧值不复活**：较晚齐备但源序号更小的事务，不能覆盖已可见的更大源序号胜者，也不能复活被更大源序号墓碑删除的键。
6. **不等待早事务**：齐备判定只看本事务的声明表集合，不等待 `seq` 更小的未齐备事务；可见集合可以跳过它。
7. **capture 有界**：只读当前 epoch 指针/根，不读表载荷、不枚举全部事务、不复制历史。
8. **发布有界**：发布只处理该事务各片段的修改键及 O(1) 元数据路径，不复制整表。

## 3. 目标调用链与接口交接

两张表仍各自独立落地，串行裁决发生在 `Warehouse.land` 返回之前。正常路径：

```
declare(tid,seq,tables)
  -> Catalog.declare                         catalog.py:16-33
land(tid,table,rows)
  -> Catalog.declaration(tid)                warehouse.py:17, catalog.py:35-40
  -> Table.land(tid,seq,rows)                warehouse.py:20, landing.py:25-54
       (片段校验 -> digest -> 单表事务提交 fragments+versions)
  -> Catalog.record(receipt, keys, tombstones)
       (回执去重 -> 声明表集合是否全部有回执 -> 新齐备则分配 epoch 并发布)
  -> return receipt
capture()
  -> Query.capture                           warehouse.py:24-25, query.py:29-31
  -> Catalog.checkpoint()                    只读当前 epoch 与各表根/epoch
  -> Handle(epoch, roots, tables, catalog)
read(handle)
  -> Handle.read                              query.py:8-22
       在 epoch 视图中枚举每键胜者（含墓碑）
       -> Table.values_for(tid,keys)         新增只读点查（设计约定，本轮不实现）
       -> 丢弃墓碑 -> 排序 -> total
```

接口交接（实现时新增，不改变已有公开签名与错误语义）：

```text
Catalog.record(receipt, keys, tombstones) -> epoch | None
    keys/tombstones 由 Warehouse.land 从已校验的 rows 直接提取（只取键与 null 标志，
    不回读 landing 载荷库），因此目录写事务内没有载荷 I/O。
    同一 (tid,table,digest) 重放返回 None，不重复发布。

Catalog.checkpoint() -> (epoch, roots)
    单次元数据点查；roots 是各表根句柄（方案 A）或空字典（方案 B）。
    不触碰 Table、不枚举声明/事务、不复制历史。

Catalog.visible_tids(epoch) -> [tid]
    仅供证据输出与核账，按 seq 排序；生产 capture 不需要。

Table.values_for(tid, keys) -> {key: value}
    只读点查 versions(tid,key,payload)；用于句柄胜者取载荷，不参与胜出。
```

`Warehouse.land` 的内部顺序调整为：`Table.land` 的载荷事务先提交，随后 `Catalog.record`
在独立的目录事务中写回执并在“首次齐备”时发布。这样载荷 I/O 不跨目录写事务，且最后一次
`land` 成功返回前已经完成裁决，紧随其后的 `capture` 必然包含该事务。
`declare`、单表 `land` 的校验、幂等/冲突、`close` 语义均不变。

## 4. 方案 A：目录侧不可变胜者树 + epoch 根

### 4.1 必要状态

Catalog 内新增（仍是每个 Warehouse 实例内的内存 SQLite 状态；Warehouse 关闭即丢弃，
不涉及跨进程或崩溃恢复，也不声称是持久存储）：

```text
txn_ready(tid PRIMARY KEY, seq, epoch)          -- 每个首次齐备事务一行
epochs(epoch PRIMARY KEY)                       -- 单调递增的齐备发布号
root_refs(epoch, table_name, root_id,
          PRIMARY KEY(epoch, table_name))       -- 每个 epoch 每表一个不可变根
nodes(node_id PRIMARY KEY, key, win_seq, win_tid,
      tombstone, left_id, right_id)             -- 不可变胜者树节点
```

`nodes` 只保存业务键、胜出源序号、胜出事务、墓碑布尔和左右子节点号，不保存值载荷。
值仍只在 `landing.py:21-22` 的 `versions` 中，避免复制载荷。树按 `key` 组织，节点不可变：
发布只做“路径复制”，生成新节点并复用未修改子树，旧根及其可达节点保持原样供旧句柄读取。

### 4.2 发布（齐备裁决）

`Catalog.record` 在写入回执后，用声明表集合与 `receipts` 做点查比较：

```text
record(receipt, keys, tombstones):
    BEGIN
      old = SELECT digest FROM receipts WHERE (tid,table)
      若 old 存在:
          old == digest ? COMMIT; return None      -- 幂等重放，不重复发布
          else ValueError                           -- 回执冲突，保持基线语义
      INSERT receipt                                -- 空片段也写回执
      declared = declaration(tid).tables
      若 receipts(tid) 的键集合 != declared: COMMIT; return None   -- 仍未齐备
      epoch = MAX(epochs)+1
      for table in declared:                            -- 只处理涉及表
          root = latest_root(table)                     -- 该表最近 epoch 的根
          for (key,tomb) in keys_of(table):             -- 只处理本事务修改键
              old = lookup(root,key)
              if old is None or seq > old.win_seq:      -- 源序号比较，不是 epoch 比较
                  root = insert_path_copy(root,key,seq,tid,tomb)
              -- 否则键不变，root 可原样复用（迟到旧值不覆盖）
          new_roots[table] = root
      for table in TABLES not in declared:              -- 未涉及表沿用最近根
          new_roots[table] = latest_root(table)
      INSERT epochs(epoch); INSERT txn_ready(tid,seq,epoch)
      INSERT root_refs(epoch, table, new_roots[table]) for both tables  -- O(1)
    COMMIT
    return epoch
```

`root_refs` 对每个 epoch 都写两张表的根（未涉及表沿用最近根），因此 `capture` 只需按
`(epoch, table)` 两次点查即可，无需回溯查找历史根；这是每发布 O(1) 的元数据成本。

要点：

- 资格判断发生在版本竞争之前：只有本事务声明的表全部有回执才调用发布。较新 `seq` 的片段若属于
  未齐备事务，根本不会进入任何根。
- `epoch` 只表示齐备发布先后，节点胜出只比较 `seq`。因此迟到但 `seq` 更小的事务即使发布得更晚，
  `lookup` 宁可保留旧节点；若它的键原本不存在，则按 `seq` 正常插入。
- 同一事务的所有表在同一个目录事务里生成同一 epoch 的根；不存在只发布一侧的中间 epoch。
- 空片段没有键，仍写 `epochs` 与 `txn_ready`，两张表的根沿用上一 epoch；空片段回执与可见事务集合都正确。
- 只涉及一表的事务在 `Table.land` 回执后立即齐备并发布。
- 发布每键最多产生 `O(log N)` 个新节点（`N` 为当前表键数），未触及的子树共享；不复制整表。
  旧 epoch 根不修改，旧句柄安全。

### 4.3 capture、read 与不变量落点

```text
capture():
    epoch = MAX(epochs)                    -- 或维护的 current_epoch，一次元数据读
    roots = {table: root_of(epoch,table)}  -- 两个根号
    return Handle(epoch, roots, tables, catalog)   -- O(1)，无表访问、无事务枚举、无历史复制

Handle.read():
    for table in ("samples","qc"):
        winners = walk(root, table)        -- O(N_key) 枚举该表视图内全部键
        live = [key for winner if not winner.tombstone]
        values = table.values_for(tid, live)   -- W 次点查载荷，仅胜者
        ...
    total = sum(samples[k] for k if qc[k]=="ok")
```

- 迟到版本：`insert_path_copy` 前的 `seq > old.win_seq` 是唯一放行条件。较晚齐备的 T7（seq 7）
  对已被 T8（seq 8）墓碑占据的 `x` 不会产生新节点，旧根与被旧句柄读到的删除态都不变。
- 墓碑：`tombstone` 标志随发布进入节点并参与 `win_seq` 竞争；`read` 在胜者确定**之后**才丢弃
  null。因此不会“先丢墓碑再选旧值”而复活行。
- 旧句柄：句柄只持有 epoch 与根号；根不可变，后续发布只新增节点/根，旧根可达图不变。重复 `read`
  结果相同。旧句柄不会看到较新事务，也不会看到本事务未齐备的另一侧。
- 单侧已落地未齐备：其版本在 `versions` 中存在，但不在任何根中；新 `capture` 的根也不含它，
  查询对其中较新版本视而不见（`partial` step 7、`pending_delete` step 5 正是此点）。

## 5. 方案 B：发布候选日志 + 句柄 epoch 过滤

### 5.1 必要状态

```text
txn_ready(tid PRIMARY KEY, seq, epoch)
epochs(epoch PRIMARY KEY)
published(epoch, table_name, tid, key, win_seq, tombstone,
          PRIMARY KEY(epoch, table_name, key))      -- 每发布事务每表每修改键一行
current_epoch                                     -- 串行裁决者维护
```

`published` 只记录已齐备事务的修改键及元数据，不复制整表、不保存值；值仍按 `(tid,key)` 点查载荷库。
表级索引可加 `(table_name, key, epoch)`，用于读取时按 epoch 过滤。

### 5.2 发布

```text
record(receipt, keys, tombstones):
    ... 与 A 相同的回执去重与声明确认 ...
    若 receipts(tid) 覆盖 declared:
        epoch = current_epoch + 1
        for table in declared:
            for (key,tomb) in keys_of(table):
                INSERT published(epoch,table,tid,key,seq,tomb)
        current_epoch = epoch
        INSERT txn_ready(tid, seq, epoch)
```

发布成本严格为 O(修改键数)，与表规模和历史无关；不维护每键全局胜者，也不需要树或路径复制。

### 5.3 capture、read 与不变量落点

```text
capture():
    return Handle(current_epoch)        -- O(1)

Handle.read():
    for table in ("samples","qc"):
        for key in distinct published keys of table with epoch <= handle.epoch:
            winner = max(published rows of this key with epoch <= handle.epoch,
                         by=win_seq)                       -- 墓碑行参与竞争
            if winner.tombstone: continue                  -- 胜出后才过滤删除
            values[key] = fetch payload by (winner.tid,key) -- 仅胜者
    ...
```

- 迟到版本：过滤条件只有 `epoch <= E` 与 `max(win_seq)`。T7 虽在较晚 epoch 发布，其 `win_seq=7`
  小于 T8 的 `win_seq=8`，胜者仍是 T8 的墓碑；旧值不复活。
- 墓碑：`published` 保存 tombstone 行并参与 `win_seq` 最大竞争；输出阶段才丢弃。
- 旧句柄：按 `epoch <= E` 过滤，旧句柄看不到后续 epoch；同一键的历史候选仍然存在，但被 epoch 上限排除，
  结果固定。代价是要保留全部发布候选，且每次 `read` 需要扫描 `epoch <= E` 的该键候选（可走
  `(table,key,epoch)` 索引按 key 点查/范围扫）。
- 不变量同 A；区别在成本位置：B 把历史资格留在候选日志，A 把历史资格折叠进胜者节点。

### 5.4 为什么不能退化为“各表最新”或“最小完成序号前缀”

- “各表最新”忽略齐备资格：`partial` step 7 会读到 T2 的 samples=20，与仍不可见的 T2 另一侧混读。
- “最小完成序号前缀”会等待 `overtake` 中未齐备的 T2（seq 2），使 H 无法包含 T3；这既违约也不必要。
  `visible` 必须是“已齐备集合”，而不是源序号前缀。
- 两方案都先做资格过滤（事务是否齐备）再做 `win_seq` 竞争；顺序不能颠倒，`pending_delete` 正用于
  区分这两步：T2 的删除先落地但未齐备，H 仍取 T1 的 x=10；T2 齐备后新句柄 J 才取到删除。

## 6. 两方案对比、推荐与成本

### 6.1 正确性对照（逐场景）

| 场景 | 关键状态 | A 的根/节点 | B 的候选日志 | 目标读 |
|---|---|---|---|---|
| partial | T1 齐备→epoch1；T2 samples 落地但不发布 | epoch1 根含 x=10/ok | epoch1 仅 T1 键 | old/middle 均 10；T2 齐备后 new 为 20/bad,total 0 |
| overtake | T1、T3 齐备；T2 只落 samples | epoch1=T1，epoch2=T3；T2 无 epoch | 候选日志无 T2 | H 含 T1,T3→40；J 含 T2→30 |
| late_delete | T8 先齐备（墓碑+bad）；T7 后齐备 | T8 根 x 墓碑；T7 `7<8` 不改 | T7 候选 `win_seq=7` 被 T8 的 8 击败 | 新旧句柄均无 x |
| pending_delete | T2 只落 samples 删除，未齐备 | T2 无 epoch；J 时发布墓碑 | 同 A 资格过滤 | H 保留 x=10；J 删除 |

两种组织在四个场景的**结果**相同，差异只在状态形态和读写成本；逐事件状态与可见集合见第 8 节。

### 6.2 成本

记 `K` 为单片段修改键数，`N` 为单表当前键数，`V(E)` 为某表在 epoch `E` 之前发布的候选行数，
`W` 为句柄读出的非墓碑胜者数。

| 操作 | 方案 A | 方案 B |
|---|---|---|
| 发布写 | 每键 O(log N) 节点复制（结构共享，节点非零成本） | 每键 O(1) 行追加，K 行 |
| 发布元数据 | O(1) epoch/根指针 | O(1) epoch 指针 |
| capture | O(1)：epoch+两表根 | O(1)：epoch |
| 旧句柄 read | O(N) 遍历视图 + W 次载荷点查，与后续发布次数无关 | O(V(E)) 扫候选（索引下按 key 范围）+ W 次载荷点查，随历史发布增长 |
| 长期存储 | 每发布新增 O(K log N) 节点 + 每 epoch 两根 | 每发布新增 O(K) 行，保留全部候选 |
| 旧根保留 | 不可变节点共享，旧句柄只增引用 | 候选日志天然保留，无需根 |
| 载荷 I/O | 发布不读载荷；read 只点查胜者 | 同左 |

方案 A 的“结构共享”只共享未修改子树，路径上节点必须新建并保存，不能把节点复制写成零成本；
方案 B 的写入更省，但为旧句柄固定性付出随发布量增长的读扫描和长期候选存储。

**推荐 A**：本任务的核心证据是旧句柄反复读取且结果必须固定（`partial` step 9/10、`overtake` step 9、
`late_delete` step 7），A 的读成本与可见历史长度无关，且“资格过滤→源序号胜出”在发布时一次完成，
更直接地满足“新齐备事务只处理修改键”和“capture 不枚举事务”的界限。B 适合写多读少、旧句柄短命的场景，
但需要额外的候选回收策略，而回收不在本轮范围，长期成本不确定。

## 7. 文档核验、差异演示与运行接线

`review.py:13-40` 会：重跑 `checks.py` 回归；读取本文件唯一的 `visibility-evidence` 块；把
`observations` 与实时 `replay.check()` 比对；把 `predictions` 与 `scenarios.json` 的
`target_reads` 比对；检查四份源码均以 `file.py:line` 被引用且行号在文件内；并要求运行清单为
`verify=["python3","review.py"]`、`demo=["python3","review.py","--show"]`。本文已按此接线。

真实差异（`review.py --show` 输出）：

- `partial` step 7：实测 20/ok（半笔更正），目标 10/ok。
- `overtake` step 7：实测 50（T2 单侧 samples 参与），目标 40。
- `pending_delete` step 5：实测样本已被删除，目标仍为 x=10。
- `late_delete`：两者输出同为无 x，但可见事务集合不同（实测无此概念，目标 T8→T7+T8）。

`review.py` 只验证证据一致性，不证明协议正确；伪代码、取舍与复杂度仍需人工审查。

## 8. 沿公开时序的逐事件推演

以下“实测”来自 `python3 replay.py --check` 的真实输出（即第 10 节 `observations`），
“目标”是按第 4/5 节状态机推演的结果，**未实现运行**。两方案的目标输出相同，只列一次；
`visible` 是该句柄的齐备集合（按源序号排序），不是生产句柄的公开方法。
A 的句柄另记每个涉及表的根版本；B 的句柄只记 epoch，读取时以候选日志按 epoch 过滤。

### 8.1 partial

| 步 | 事件 | 裁决后状态（A：epoch/根；B：候选日志） | 句柄表示 | 该步 read | 实测 | 目标 |
|---|---|---|---|---|---|---|
| 1 | land T1 samples x=10 | T1 回执 1/2，未齐备，无 epoch；版本已落载荷 | — | — | — | — |
| 2 | land T1 qc x=ok | T1 齐备 → epoch1；A 根含 samples x(seq1,T1,live) 与 qc x(seq1,T1,ok)；B 发布 samples/qc 各一键 | — | — | — | — |
| 3 | capture old | 指针仍 epoch1 | old=(epoch1) | — | — | — |
| 4 | read old | — | — | x=10,ok；total10；visible[T1] | 10 | 10 |
| 5 | land T2 samples x=20 | T2 回执 1/2，未齐备；**不发布** | — | — | — | — |
| 6 | capture middle | epoch1 | middle=(epoch1) | — | — | — |
| 7 | read middle | — | — | T2 较新版本不在根中：x=10,ok；total10 | **20** | 10 |
| 8 | land T2 qc x=bad | T2 齐备 → epoch2；x 胜者 seq2；qc x=bad | — | — | — | — |
| 9 | read old | — | old=(epoch1) | 根不可变：x=10,ok；total10 | **20/bad,0** | 10 |
| 10 | read middle | — | middle=(epoch1) | 同上：10/ok；total10 | **20/bad,0** | 10 |
| 11 | capture new | epoch2 | new=(epoch2) | — | — | — |
| 12 | read new | — | — | x=20,bad；total0；visible[T1,T2] | 20/bad,0 | 20/bad,0 |

方案 B 的同序状态：step2 发布 epoch1 的 `(samples,x,T1,seq1,live)` 与 `(qc,x,T1,seq1,ok)`；
step5 只写 T2 samples 回执，`published` 无 T2 行；step7 句柄 epoch1 只看 T1 行，读 10；
step8 发布 epoch2 的 samples/qc 两行，step9/10 的 epoch1 句柄仍只取 T1 行；step12 以 `max(win_seq)`
取到 T2 的 20/bad。

### 8.2 overtake

| 步 | 事件 | 裁决后状态 | 句柄 | read | 实测 | 目标 |
|---|---|---|---|---|---|---|
| 1 | land T1 samples x=10 | T1 回执 1/2 | — | — | — | — |
| 2 | land T1 qc x=ok | T1 齐备 → epoch1，x=10/ok | — | — | — | — |
| 3 | land T2 samples x=20 | T2 回执 1/2，未齐备，不发布；x=20 不在根 | — | — | — | — |
| 4 | land T3 samples y=30 | T3 回执 1/2 | — | — | — | — |
| 5 | land T3 qc y=ok | T3 齐备 → epoch2，y=30/ok；不等待 T2 | — | — | — | — |
| 6 | capture H | epoch2 | H=(epoch2) | — | — | — |
| 7 | read H | — | — | x=10/ok（T1）、y=30/ok；total40；visible[T1,T3] | **50** | 40 |
| 8 | land T2 qc x=bad | T2 齐备 → epoch3；x 胜者 seq2=20，qc x=bad | — | — | — | — |
| 9 | read H | — | H=(epoch2) | 仍是 x=10/ok、y=30/ok；total40 | **30** | 40 |
| 10 | capture J | epoch3 | J=(epoch3) | — | — | — |
| 11 | read J | — | — | x=20/bad、y=30/ok；total30；visible[T1,T2,T3] | 30 | 30 |

方案 B：step3 T2 samples 不发布；step5 T3 发布 epoch2，H 只按 epoch2 过滤；step8 T2 发布 epoch3，
x 行 `win_seq=2` 大于 T1 的 1，故 J 读到 20/bad；H 的 epoch 上限排除 epoch3。

### 8.3 late_delete

| 步 | 事件 | 裁决后状态 | 句柄 | read | 实测 | 目标 |
|---|---|---|---|---|---|---|
| 1 | land T8 samples x=null | T8 回执 1/2；墓碑版本在载荷 | — | — | — | — |
| 2 | land T8 qc x=bad | T8 齐备 → epoch1；A 根 samples x=(seq8,tomb)，qc x=bad | — | — | — | — |
| 3 | capture H | epoch1 | H=(epoch1) | — | — | — |
| 4 | read H | — | — | 胜者为墓碑→过滤后 samples 空；qc x=bad；total0；visible[T8] | 空/bad,0 | 空/bad,0 |
| 5 | land T7 samples x=70 | T7 回执 1/2，未齐备 | — | — | — | — |
| 6 | land T7 qc x=ok | T7 齐备 → epoch2；x 旧根胜者 seq8，7≤8，**键不变、子树复用** | — | — | — | — |
| 7 | read H | — | H=(epoch1) | 同 step4；旧值 70 不复活 | 空/bad,0 | 空/bad,0 |
| 8 | capture J | epoch2 | J=(epoch2) | — | — | — |
| 9 | read J | — | — | 输出同 step4，但 visible[T7,T8]（B 的候选日志含 T7 行且被 seq8 击败） | 空/bad,0 | 空/bad,0 |

方案 B：step2 发布 epoch1 的 samples `(x,T8,seq8,tomb)` 与 qc `(x,T8,seq8,bad)`；step6 发布 epoch2 的
T7 两行，但 `max(win_seq)` 仍选 T8；旧句柄按 epoch1 过滤候选，新句柄按 epoch2 过滤，两者结果相同而
可见集合不同（T8 vs T7+T8）。

### 8.4 pending_delete

| 步 | 事件 | 裁决后状态 | 句柄 | read | 实测 | 目标 |
|---|---|---|---|---|---|---|
| 1 | land T1 samples x=10 | T1 回执 1/2 | — | — | — | — |
| 2 | land T1 qc x=ok | T1 齐备 → epoch1，x=10/ok | — | — | — | — |
| 3 | land T2 samples x=null | T2 回执 1/2，未齐备；删除版本只在载荷，不入根 | — | — | — | — |
| 4 | capture H | epoch1 | H=(epoch1) | — | — | — |
| 5 | read H | — | — | 资格过滤先于胜出：T2 的墓碑不可见，仍 x=10/ok；total10 | **空/ok,0** | 10 |
| 6 | land T2 qc x=bad | T2 齐备 → epoch2；samples x 胜者 seq2 墓碑，qc x=bad | — | — | — | — |
| 7 | read H | — | H=(epoch1) | 仍 x=10/ok；total10 | **空/bad,0** | 10 |
| 8 | capture J | epoch2 | J=(epoch2) | — | — | — |
| 9 | read J | — | — | samples 空（墓碑胜出后过滤）、qc x=bad；total0；visible[T1,T2] | 空/bad,0 | 空/bad,0 |

方案 B：step3 只写 T2 samples 回执，`published` 无 T2 墓碑行，所以 H 看不到删除；step6 T2 齐备后
发布 epoch2 的 samples 墓碑与 qc bad，J 以 `win_seq=2` 的墓碑胜出并过滤，H 被 epoch1 上限固定。

### 8.5 逐事件结论

- `partial` step7 与 `pending_delete` step5 证明“先资格过滤、后源序号胜出”：已落地但未齐备的较新/删除版本
  对查询不可见。
- `overtake` step7 证明不能等 `seq` 更小的 T2，也不能用“最小完成序号前缀”。
- `late_delete` step6/7 证明迟到旧值不复活，且旧句柄的根/epoch 不变；两方案输出相同但状态不同。
- `partial` step9/10、`overtake` step9、`pending_delete` step7 证明旧句柄重复读取固定。
- 实测与目标的差异恰好是基线“每表最新”的可见半笔更正；差异不是靠改预测值消除的。

## 9. 资源界限（访问对象与不变量）

```text
publish(tid):                      # 仅由串行 land 调用，载荷事务已提交
    receipts = catalog.receipts(tid)         # O(声明表数) 点查，表数 ≤ 2
    if keys(receipts) != declared(tid): return  # 不扫描 versions，不枚举其他事务
    for table in declared:                   # ≤ 2
        root = catalog.latest_root(table)    # O(1)
        for key in fragment_keys(tid, table):# 仅本事务修改键 K
            if catalog.lookup(root,key) 无胜者 或 seq 更大:
                root = catalog.path_copy(root,key)  # O(log N) 新节点，其余共享
        catalog.store_root(epoch, table, root)      # O(1)
```

- `capture` 只做 `max(epoch)`/当前指针读取，加两表根号，O(1)；不调用 `Table.scan_versions`
  （`landing.py:56-59`），不枚举 `declarations`/`receipts` 全表，不复制历史。
- 发布只读取本事务回执与本片段键（`K` 行）；A 新增 `O(K log N)` 节点并复用旧子树，
  B 新增 `O(K)` 候选行。两者都持有值载荷之外的最小元数据，载荷仍在 `versions`。
- 载荷写入在 `landing.py:50-53` 的单表事务内完成并提交；`Catalog.record` 之后才开目录写事务，
  所以没有跨载荷 I/O 的目录写锁。
- 旧句柄只持不可变根号/epoch，读取不产生写、不改变其他句柄；保留成本是旧根可达节点（A）
  或历史候选行（B），已在第 6 节计入。

## 10. 实测证据（visibility-evidence）

以下 `observations` 为 `python3 replay.py --check` 的原样 JSON；`predictions` 为第 8 节推导的目标读数，
字段与公开 `target_reads` 相同。它们只作核账锚点与设计证据，不是已实现协议。

```visibility-evidence
{
  "observations": {
    "late_delete": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 4
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 7
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 9
      }
    ],
    "overtake": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ],
            [
              "y",
              30
            ]
          ],
          "total": 50
        },
        "step": 7
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ],
            [
              "y",
              30
            ]
          ],
          "total": 30
        },
        "step": 9
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ],
            [
              "y",
              30
            ]
          ],
          "total": 30
        },
        "step": 11
      }
    ],
    "partial": [
      {
        "handle": "old",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 4
      },
      {
        "handle": "middle",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ]
          ],
          "total": 20
        },
        "step": 7
      },
      {
        "handle": "old",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ]
          ],
          "total": 0
        },
        "step": 9
      },
      {
        "handle": "middle",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ]
          ],
          "total": 0
        },
        "step": 10
      },
      {
        "handle": "new",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ]
          ],
          "total": 0
        },
        "step": 12
      }
    ],
    "pending_delete": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 5
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 7
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 9
      }
    ]
  },
  "predictions": {
    "late_delete": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 4,
        "visible": [
          "T8"
        ]
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 7,
        "visible": [
          "T8"
        ]
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 9,
        "visible": [
          "T7",
          "T8"
        ]
      }
    ],
    "overtake": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ],
            [
              "y",
              30
            ]
          ],
          "total": 40
        },
        "step": 7,
        "visible": [
          "T1",
          "T3"
        ]
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ],
            [
              "y",
              30
            ]
          ],
          "total": 40
        },
        "step": 9,
        "visible": [
          "T1",
          "T3"
        ]
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ],
            [
              "y",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ],
            [
              "y",
              30
            ]
          ],
          "total": 30
        },
        "step": 11,
        "visible": [
          "T1",
          "T2",
          "T3"
        ]
      }
    ],
    "partial": [
      {
        "handle": "old",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 4,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "middle",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 7,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "old",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 9,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "middle",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 10,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "new",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [
            [
              "x",
              20
            ]
          ],
          "total": 0
        },
        "step": 12,
        "visible": [
          "T1",
          "T2"
        ]
      }
    ],
    "pending_delete": [
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 5,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "H",
        "result": {
          "qc": [
            [
              "x",
              "ok"
            ]
          ],
          "samples": [
            [
              "x",
              10
            ]
          ],
          "total": 10
        },
        "step": 7,
        "visible": [
          "T1"
        ]
      },
      {
        "handle": "J",
        "result": {
          "qc": [
            [
              "x",
              "bad"
            ]
          ],
          "samples": [],
          "total": 0
        },
        "step": 9,
        "visible": [
          "T1",
          "T2"
        ]
      }
    ]
  }
}
```
