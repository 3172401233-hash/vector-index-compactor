# vector-index-compactor

离线向量数据入湖与压缩库。输入 embedding 批次（文档 ID、版本、分区时间、删除标记），
落盘为不可变 segment，通过原子发布的 manifest 对下游提供一致的读取视图；
小段过多时执行 compaction 重新打包有效向量，墓碑在保留窗口过后清理。

## 使用

```js
const { VectorLake } = require('./src/vector-lake');

const lake = new VectorLake('/data/vector-lake', {
  compactionThreshold: 8,        // 活跃 segment 达到该数量后自动 compaction
  tombstoneRetentionMs: 86400000, // 墓碑保留窗口
  readerGraceMs: 60000,           // compaction 后旧 segment 的读取宽限期
});
await lake.open(); // 启动时恢复：清理临时文件与孤儿 segment，回退到最新有效 manifest

await lake.ingest([
  { docId: 'doc-1', version: 3, partition: 'p-2026-11-16', vector: [0.1, 0.2] },
  { docId: 'doc-2', version: 5, partition: 'p-2026-11-16', deleted: true }, // 墓碑
]);

const snap = lake.snapshot();      // 固定在某个 manifest 上的只读视图
await snap.get('doc-1');           // 单文档有效版本（被墓碑遮蔽时返回 null）
await snap.all();                  // 全部有效向量，按 docId 排序

await lake.compact();              // 手动触发；默认达到阈值时自动触发
await lake.gc();                   // 宽限期过后回收被替换的 segment 文件
```

运行测试：`npm test`（即 `node --test`）。

## Segment

- 每个 ingest 批次先去重（同一 `docId` 保留最高 `version`，同版本后者胜出），
  归一化后按 `(docId, version)` 排序，写成 `segments/seg-NNNNNN.json`。
- Segment 一旦发布即不可变，只增不改；内容采用键序固定的规范化 JSON，
  相同输入序列产生字节级一致的文件（确定性写盘）。
- 每条记录带 `ts`（入湖时间，可注入时钟），墓碑清理以它为准。
- 校验和只覆盖记录内容（不含 segment 自增 ID），因此重复投递的相同批次
  会被识别并整体跳过，不会产生新 segment。
- 读取时逐条校验记录摘要，摘要不匹配抛出 `SegmentCorruptionError`。

## Manifest

- `manifest-NNNNNN.json` 描述一个完整快照：序号、活跃 segment 列表及各自的
  校验和、记录数、墓碑数。`CURRENT` 文件指向当前生效的 manifest。
- 发布流程：先写 segment（临时文件 + rename），再写新 manifest，最后更新
  `CURRENT`，全部通过临时文件原子替换完成。下游只能读到完整发布的
  manifest；compaction 整理期间，旧 manifest 及其 segment 继续服务读取。
- 打开时按 `CURRENT` → 序号倒序逐个校验 manifest 摘要，跳过损坏文件，
  回退到最新有效 manifest；全部损坏则从空湖开始。
- 读取视图按 manifest 中 segment 顺序合并：同一 `docId` 取最高 `version`，
  同版本取顺序靠后的 segment；跨分区搬迁自然只保留最新分区记录。

## 墓碑与清理规则

- 删除通知写成墓碑记录（`deleted: true`，向量为空），与数据记录一样参与
  版本合并：版本更高的墓碑遮蔽旧版本数据，读取时不可见。
- Compaction 时：有效数据记录重新打包进新 segment；墓碑在
  `now - ts < tombstoneRetentionMs` 的保留窗口内继续保留（防止旧 segment
  中的旧版本复活），超过窗口后与旧版本一起物理删除。
- Compaction 替换下来的旧 segment 不立即删除，进入 `readerGraceMs` 宽限期，
  保证持有旧快照的并发读取不受影响；宽限期过后由 `gc()` 回收。
- Compaction 中断（如发布 manifest 前崩溃）：旧 manifest 仍然完整有效，
  残留的临时文件与未被任何 manifest 引用的孤儿 segment 在下次 `open()` 时清理。
