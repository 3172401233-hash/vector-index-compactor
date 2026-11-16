'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { VectorLake, SegmentCorruptionError } = require('../src/vector-lake');

function makeClock(start = 1_000_000) {
  let now = start;
  return {
    now: () => now,
    advance(ms) { now += ms; },
  };
}

async function makeLake(t, options = {}) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'vector-lake-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const clock = options.clock ?? makeClock();
  const lake = new VectorLake(dir, {
    compactionThreshold: 100,
    readerGraceMs: 1000,
    tombstoneRetentionMs: 5000,
    ...options,
    now: clock.now,
  });
  await lake.open();
  return { lake, dir, clock };
}

function rec(docId, version, partition, vector, extra = {}) {
  return { docId, version, partition, vector, ...extra };
}

async function effectiveIds(snapshot) {
  return (await snapshot.all()).map((r) => r.docId);
}

test('ingest publishes immutable segments and snapshots expose effective vectors', async (t) => {
  const { lake } = await makeLake(t);
  await lake.ingest([
    rec('a', 1, 'p-2026-11-15', [1, 0]),
    rec('b', 1, 'p-2026-11-15', [0, 1]),
  ]);
  await lake.ingest([rec('c', 1, 'p-2026-11-16', [1, 1])]);
  const snap = lake.snapshot();
  assert.deepEqual(await effectiveIds(snap), ['a', 'b', 'c']);
  assert.deepEqual((await snap.get('a')).vector, [1, 0]);
  assert.equal(lake.manifest.segments.length, 2);
});

test('re-ingesting an identical batch is deduplicated and publishes no new segment', async (t) => {
  const { lake } = await makeLake(t);
  const batch = [rec('a', 1, 'p1', [1, 2]), rec('b', 2, 'p1', [3, 4])];
  const first = await lake.ingest(batch);
  const second = await lake.ingest(batch);
  assert.equal(first.deduplicated, false);
  assert.equal(second.deduplicated, true);
  assert.equal(lake.manifest.segments.length, 1);
  assert.deepEqual(await effectiveIds(lake.snapshot()), ['a', 'b']);
});

test('multiple versions of one document resolve to the highest version', async (t) => {
  const { lake } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1, 0])]);
  await lake.ingest([rec('a', 3, 'p1', [3, 0])]);
  await lake.ingest([rec('a', 2, 'p1', [2, 0])]);
  const got = await lake.snapshot().get('a');
  assert.equal(got.version, 3);
  assert.deepEqual(got.vector, [3, 0]);
});

test('within one batch the highest version wins', async (t) => {
  const { lake } = await makeLake(t);
  await lake.ingest([
    rec('a', 1, 'p1', [1, 0]),
    rec('a', 5, 'p1', [5, 0]),
    rec('a', 3, 'p1', [3, 0]),
  ]);
  assert.equal((await lake.snapshot().get('a')).version, 5);
});

test('cross-partition move keeps only the newest partition record', async (t) => {
  const { lake } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p-2026-11-15', [1, 1])]);
  await lake.ingest([rec('a', 2, 'p-2026-11-16', [2, 2])]);
  const got = await lake.snapshot().get('a');
  assert.equal(got.partition, 'p-2026-11-16');
  assert.deepEqual(got.vector, [2, 2]);
  const result = await lake.compact({ force: true });
  assert.equal(result.keptRecords, 1);
  const compacted = await lake.snapshot().get('a');
  assert.equal(compacted.partition, 'p-2026-11-16');
});

test('tombstones hide documents and are purged only after the retention window', async (t) => {
  const { lake, clock } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1, 0]), rec('b', 1, 'p1', [0, 1])]);
  await lake.ingest([rec('a', 2, 'p1', [], { deleted: true })]);
  assert.equal(await lake.snapshot().get('a'), null);
  assert.deepEqual(await effectiveIds(lake.snapshot()), ['b']);

  clock.advance(1000);
  let result = await lake.compact({ force: true });
  assert.equal(result.keptRecords, 2, 'tombstone retained inside retention window');
  assert.equal(lake.manifest.segments[0].tombstoneCount, 1);
  assert.equal(await lake.snapshot().get('a'), null);

  clock.advance(10_000);
  result = await lake.compact({ force: true });
  assert.equal(result.keptRecords, 1, 'expired tombstone purged');
  assert.equal(lake.manifest.segments[0].tombstoneCount, 0);
  assert.deepEqual(await effectiveIds(lake.snapshot()), ['b']);
});

test('compaction preserves the effective vector set while reducing segment count', async (t) => {
  const { lake } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1, 0]), rec('b', 1, 'p1', [0, 1])]);
  await lake.ingest([rec('a', 2, 'p2', [2, 0])]);
  await lake.ingest([rec('c', 1, 'p1', [1, 1])]);
  await lake.ingest([rec('b', 2, 'p1', [], { deleted: true })]);
  const before = await lake.snapshot().all();
  assert.equal(lake.manifest.segments.length, 4);

  const result = await lake.compact({ force: true });
  assert.equal(result.before, 4);
  assert.equal(result.after, 1);
  assert.equal(lake.manifest.segments.length, 1);

  const after = await lake.snapshot().all();
  assert.deepEqual(after.map((r) => [r.docId, r.version, r.partition, r.vector]),
    before.map((r) => [r.docId, r.version, r.partition, r.vector]));
});

test('auto compaction triggers when small segments exceed the threshold', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'vector-lake-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const clock = makeClock();
  const lake = new VectorLake(dir, { compactionThreshold: 3, now: clock.now });
  await lake.open();
  await lake.ingest([rec('a', 1, 'p1', [1])]);
  await lake.ingest([rec('b', 1, 'p1', [2])]);
  assert.equal(lake.manifest.segments.length, 2);
  const res = await lake.ingest([rec('c', 1, 'p1', [3])]);
  assert.equal(lake.manifest.segments.length, 1, 'compacted down to one segment');
  assert.ok(res.compacted);
  assert.deepEqual(await effectiveIds(lake.snapshot()), ['a', 'b', 'c']);
});

test('interrupted compaction keeps the old manifest and orphans are reclaimed on open', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'vector-lake-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const clock = makeClock();
  const lake = new VectorLake(dir, {
    compactionThreshold: 100,
    now: clock.now,
    hooks: {
      beforeManifestPublish: () => { throw new Error('simulated crash'); },
    },
  });
  await lake.open();
  await lake.ingest([rec('a', 1, 'p1', [1, 0])]);
  await lake.ingest([rec('b', 1, 'p1', [0, 1])]);
  const manifestBefore = lake.manifest.file;
  await assert.rejects(() => lake.compact({ force: true }), /simulated crash/);

  const reopened = new VectorLake(dir, { compactionThreshold: 100, now: clock.now });
  await reopened.open();
  assert.equal(reopened.manifest.file, manifestBefore, 'old manifest still published');
  assert.deepEqual(await effectiveIds(reopened.snapshot()), ['a', 'b']);
  const segmentFiles = await fs.readdir(path.join(dir, 'segments'));
  assert.equal(segmentFiles.length, 2, 'orphan segment from failed compaction removed');

  const result = await reopened.compact({ force: true });
  assert.equal(result.after, 1);
  assert.deepEqual(await effectiveIds(reopened.snapshot()), ['a', 'b']);
});

test('corrupt CURRENT pointer falls back to the newest valid manifest', async (t) => {
  const { lake, dir } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1])]);
  await lake.ingest([rec('b', 1, 'p1', [2])]);
  const goodSequence = lake.manifest.sequence;
  await fs.writeFile(path.join(dir, 'CURRENT'), 'manifest-999999.json');

  const reopened = new VectorLake(dir, { compactionThreshold: 100 });
  await reopened.open();
  assert.equal(reopened.manifest.sequence, goodSequence);
  assert.deepEqual(await effectiveIds(reopened.snapshot()), ['a', 'b']);
});

test('corrupt newest manifest is skipped in favour of the previous valid one', async (t) => {
  const { lake, dir } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1])]);
  await lake.ingest([rec('b', 1, 'p1', [2])]);
  const newest = path.join(dir, lake.manifest.file);
  await fs.writeFile(newest, '{"format":1,"sequence":2,"segments":"tampered"}');

  const reopened = new VectorLake(dir, { compactionThreshold: 100 });
  await reopened.open();
  assert.equal(reopened.manifest.sequence, 1);
  assert.deepEqual(await effectiveIds(reopened.snapshot()), ['a']);
});

test('corrupt segment content is detected via checksum', async (t) => {
  const { lake, dir } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1])]);
  const segFile = path.join(dir, 'segments', lake.manifest.segments[0].file);
  await fs.writeFile(segFile, '{"format":1,"id":"seg-000001","records":[]}');
  await assert.rejects(() => lake.snapshot().all(), SegmentCorruptionError);
});

test('old snapshots keep reading during compaction; gc reclaims after the grace window', async (t) => {
  const { lake, dir, clock } = await makeLake(t);
  await lake.ingest([rec('a', 1, 'p1', [1, 0])]);
  await lake.ingest([rec('b', 1, 'p1', [0, 1])]);
  const oldSnap = lake.snapshot();
  const oldFiles = lake.manifest.segments.map((s) => s.file);

  await lake.compact({ force: true });
  assert.deepEqual(await effectiveIds(oldSnap), ['a', 'b'], 'old snapshot still readable');
  for (const file of oldFiles) {
    await fs.access(path.join(dir, 'segments', file));
  }

  assert.equal(await lake.gc(), 0, 'grace window not elapsed');
  clock.advance(2000);
  assert.equal(await lake.gc(), 2, 'retired segments reclaimed after grace');
  for (const file of oldFiles) {
    await assert.rejects(() => fs.access(path.join(dir, 'segments', file)));
  }
  assert.deepEqual(await effectiveIds(lake.snapshot()), ['a', 'b']);
});

test('concurrent ingests are serialized and all batches are visible', async (t) => {
  const { lake } = await makeLake(t);
  await Promise.all([
    lake.ingest([rec('a', 1, 'p1', [1])]),
    lake.ingest([rec('b', 1, 'p1', [2])]),
    lake.ingest([rec('c', 1, 'p1', [3])]),
    lake.ingest([rec('a', 2, 'p1', [9])]),
  ]);
  const snap = lake.snapshot();
  assert.deepEqual(await effectiveIds(snap), ['a', 'b', 'c']);
  assert.deepEqual((await snap.get('a')).vector, [9]);
});

test('identical input sequences produce byte-identical segments and manifests', async (t) => {
  async function build() {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'vector-lake-'));
    t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const clock = makeClock(42);
    const lake = new VectorLake(dir, { compactionThreshold: 100, now: clock.now });
    await lake.open();
    await lake.ingest([rec('b', 1, 'p1', [0, 1]), rec('a', 1, 'p1', [1, 0])]);
    clock.advance(10);
    await lake.ingest([rec('a', 2, 'p2', [2, 0]), rec('c', 1, 'p1', [1, 1])]);
    clock.advance(10);
    await lake.compact({ force: true });
    return dir;
  }
  const dir1 = await build();
  const dir2 = await build();
  const files1 = (await fs.readdir(path.join(dir1, 'segments'))).sort();
  const files2 = (await fs.readdir(path.join(dir2, 'segments'))).sort();
  assert.deepEqual(files1, files2);
  for (const file of files1) {
    const [a, b] = await Promise.all([
      fs.readFile(path.join(dir1, 'segments', file), 'utf8'),
      fs.readFile(path.join(dir2, 'segments', file), 'utf8'),
    ]);
    assert.equal(a, b, `segment ${file} bytes differ`);
  }
  const manifests1 = (await fs.readdir(dir1)).filter((f) => f.startsWith('manifest-')).sort();
  const manifests2 = (await fs.readdir(dir2)).filter((f) => f.startsWith('manifest-')).sort();
  assert.deepEqual(manifests1, manifests2);
  for (const file of manifests1) {
    const [a, b] = await Promise.all([
      fs.readFile(path.join(dir1, file), 'utf8'),
      fs.readFile(path.join(dir2, file), 'utf8'),
    ]);
    assert.equal(a, b, `manifest ${file} bytes differ`);
  }
});

test('invalid records are rejected', async (t) => {
  const { lake } = await makeLake(t);
  await assert.rejects(() => lake.ingest([{ docId: '', version: 1, partition: 'p1', vector: [1] }]));
  await assert.rejects(() => lake.ingest([rec('a', -1, 'p1', [1])]));
  await assert.rejects(() => lake.ingest([rec('a', 1, 'p1', [Number.NaN])]));
  await assert.rejects(() => lake.ingest([]));
});
