'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

function canonicalize(value) {
  if (Array.isArray(value)) {
    return '[' + value.map(canonicalize).join(',') + ']';
  }
  if (value !== null && typeof value === 'object') {
    return '{' + Object.keys(value).sort()
      .map((k) => JSON.stringify(k) + ':' + canonicalize(value[k]))
      .join(',') + '}';
  }
  return JSON.stringify(value);
}

function sha256(content) {
  return crypto.createHash('sha256').update(content, 'utf8').digest('hex');
}

function pad(num) {
  return String(num).padStart(6, '0');
}

async function writeAtomic(file, content) {
  const tmp = `${file}.tmp-${process.pid}`;
  await fs.writeFile(tmp, content, 'utf8');
  await fs.rename(tmp, file);
}

async function unlinkMaybe(file) {
  try {
    await fs.unlink(file);
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
}

class SegmentCorruptionError extends Error {
  constructor(segmentId, expected, actual) {
    super(`segment ${segmentId} checksum mismatch: expected ${expected}, got ${actual}`);
    this.name = 'SegmentCorruptionError';
  }
}

function validateRecord(record) {
  if (typeof record.docId !== 'string' || record.docId.length === 0) {
    throw new TypeError('record.docId must be a non-empty string');
  }
  if (!Number.isInteger(record.version) || record.version < 0) {
    throw new TypeError('record.version must be a non-negative integer');
  }
  if (typeof record.partition !== 'string' || record.partition.length === 0) {
    throw new TypeError('record.partition must be a non-empty string');
  }
  if (record.deleted !== undefined && typeof record.deleted !== 'boolean') {
    throw new TypeError('record.deleted must be a boolean');
  }
  if (!record.deleted) {
    if (!Array.isArray(record.vector) || !record.vector.every((n) => Number.isFinite(n))) {
      throw new TypeError('record.vector must be an array of finite numbers');
    }
  }
}

function dedupBatch(records) {
  const byDoc = new Map();
  for (const record of records) {
    const current = byDoc.get(record.docId);
    if (!current || record.version >= current.version) {
      byDoc.set(record.docId, record);
    }
  }
  return [...byDoc.values()];
}

function compareRecords(a, b) {
  if (a.docId !== b.docId) return a.docId < b.docId ? -1 : 1;
  return a.version - b.version;
}

function mergeSegmentRecords(segmentDatas) {
  const byDoc = new Map();
  for (const data of segmentDatas) {
    for (const record of data.records) {
      const current = byDoc.get(record.docId);
      if (!current || record.version >= current.version) {
        byDoc.set(record.docId, record);
      }
    }
  }
  return byDoc;
}

class Snapshot {
  constructor(lake, manifest) {
    this.lake = lake;
    this.manifest = manifest;
  }

  get sequence() {
    return this.manifest ? this.manifest.sequence : 0;
  }

  async effectiveMap() {
    const datas = [];
    const segments = this.manifest ? this.manifest.segments : [];
    for (const seg of segments) {
      datas.push(await this.lake._readSegment(seg));
    }
    return mergeSegmentRecords(datas);
  }

  async get(docId) {
    const map = await this.effectiveMap();
    const record = map.get(docId);
    if (!record || record.deleted) return null;
    return record;
  }

  async all() {
    const map = await this.effectiveMap();
    return [...map.values()]
      .filter((record) => !record.deleted)
      .sort(compareRecords);
  }
}

class VectorLake {
  constructor(dir, options = {}) {
    this.dir = dir;
    this.segmentsDir = path.join(dir, 'segments');
    this.compactionThreshold = options.compactionThreshold ?? 8;
    this.autoCompact = options.autoCompact ?? true;
    this.tombstoneRetentionMs = options.tombstoneRetentionMs ?? 24 * 60 * 60 * 1000;
    this.readerGraceMs = options.readerGraceMs ?? 60 * 1000;
    this.clock = options.now ?? (() => Date.now());
    this.hooks = options.hooks ?? {};
    this.manifest = null;
    this.retired = [];
    this._segmentSeq = 0;
    this._mutex = Promise.resolve();
  }

  async open() {
    await fs.mkdir(this.segmentsDir, { recursive: true });
    const manifests = await this._loadValidManifests();
    this.manifest = manifests.length > 0 ? manifests[0] : null;
    await this._collectGarbage(manifests);
    for (const m of manifests) {
      for (const seg of m.segments) {
        const num = Number(seg.id.replace(/^seg-/, ''));
        if (Number.isInteger(num) && num > this._segmentSeq) this._segmentSeq = num;
      }
    }
    return this;
  }

  snapshot() {
    return new Snapshot(this, this.manifest);
  }

  async ingest(records) {
    if (!Array.isArray(records) || records.length === 0) {
      throw new TypeError('ingest expects a non-empty array of records');
    }
    for (const record of records) validateRecord(record);
    return this._withLock(async () => {
      const ts = this.clock();
      const normalized = dedupBatch(records).map((record) => ({
        docId: record.docId,
        version: record.version,
        partition: record.partition,
        vector: record.deleted ? [] : record.vector,
        deleted: record.deleted === true,
        ts: record.ts ?? ts,
      }));
      const liveSegments = this.manifest ? this.manifest.segments : [];
      const segment = await this._writeSegment(normalized);
      if (liveSegments.some((seg) => seg.checksum === segment.checksum)) {
        await unlinkMaybe(path.join(this.segmentsDir, segment.file));
        return { segment: null, deduplicated: true };
      }
      await this._publishManifest([...liveSegments, segment]);
      let compacted = null;
      if (this.autoCompact && this.manifest.segments.length >= this.compactionThreshold) {
        compacted = await this._compactLocked();
      }
      return { segment: segment.id, deduplicated: false, compacted };
    });
  }

  async compact(options = {}) {
    return this._withLock(() => this._compactLocked(options));
  }

  async gc() {
    return this._withLock(async () => {
      const now = this.clock();
      const due = this.retired.filter((entry) => now >= entry.retireAt);
      this.retired = this.retired.filter((entry) => now < entry.retireAt);
      for (const entry of due) {
        await unlinkMaybe(path.join(this.segmentsDir, entry.file));
      }
      return due.length;
    });
  }

  async _compactLocked(options = {}) {
    const segments = this.manifest ? this.manifest.segments : [];
    if (segments.length === 0) return null;
    if (!options.force && segments.length < this.compactionThreshold) return null;
    const datas = [];
    for (const seg of segments) {
      datas.push(await this._readSegment(seg));
    }
    const now = this.clock();
    const kept = [...mergeSegmentRecords(datas).values()]
      .filter((record) => !record.deleted || now - record.ts < this.tombstoneRetentionMs)
      .sort(compareRecords);
    const newSegments = [];
    if (kept.length > 0) {
      newSegments.push(await this._writeSegment(kept));
    }
    if (this.hooks.beforeManifestPublish) {
      await this.hooks.beforeManifestPublish();
    }
    await this._publishManifest(newSegments);
    for (const seg of segments) {
      this.retired.push({ file: seg.file, retireAt: now + this.readerGraceMs });
    }
    return { before: segments.length, after: newSegments.length, keptRecords: kept.length };
  }

  async _writeSegment(records) {
    this._segmentSeq += 1;
    const id = `seg-${pad(this._segmentSeq)}`;
    const sorted = [...records].sort(compareRecords);
    const checksum = sha256(canonicalize({ format: 1, records: sorted }));
    const content = canonicalize({ format: 1, id, checksum, records: sorted });
    const file = `${id}.json`;
    await writeAtomic(path.join(this.segmentsDir, file), content);
    return {
      id,
      file,
      recordCount: sorted.length,
      tombstoneCount: sorted.filter((record) => record.deleted).length,
      checksum,
    };
  }

  async _readSegment(seg) {
    const content = await fs.readFile(path.join(this.segmentsDir, seg.file), 'utf8');
    const parsed = JSON.parse(content);
    const actual = sha256(canonicalize({ format: 1, records: parsed.records }));
    if (actual !== seg.checksum || parsed.checksum !== seg.checksum) {
      throw new SegmentCorruptionError(seg.id, seg.checksum, actual);
    }
    return parsed;
  }

  async _publishManifest(segments) {
    const sequence = (this.manifest ? this.manifest.sequence : 0) + 1;
    const body = { format: 1, sequence, createdAt: this.clock(), segments };
    const checksum = sha256(canonicalize(body));
    const file = `manifest-${pad(sequence)}.json`;
    await writeAtomic(path.join(this.dir, file), canonicalize({ ...body, checksum }));
    await writeAtomic(path.join(this.dir, 'CURRENT'), file);
    this.manifest = { ...body, checksum, file };
  }

  async _loadValidManifests() {
    const entries = await fs.readdir(this.dir);
    const files = entries.filter((f) => /^manifest-\d+\.json$/.test(f)).sort().reverse();
    let current = null;
    try {
      current = (await fs.readFile(path.join(this.dir, 'CURRENT'), 'utf8')).trim();
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
    const ordered = current && files.includes(current)
      ? [current, ...files.filter((f) => f !== current)]
      : files;
    const valid = [];
    for (const file of ordered) {
      try {
        const parsed = JSON.parse(await fs.readFile(path.join(this.dir, file), 'utf8'));
        const { checksum, ...body } = parsed;
        if (typeof checksum !== 'string') continue;
        if (sha256(canonicalize(body)) !== checksum) continue;
        valid.push({ ...body, checksum, file });
      } catch {
        // skip unreadable or malformed manifest
      }
    }
    valid.sort((a, b) => b.sequence - a.sequence);
    return valid;
  }

  async _collectGarbage(validManifests) {
    const referenced = new Set();
    for (const m of validManifests) {
      for (const seg of m.segments) referenced.add(seg.file);
    }
    for (const entry of await fs.readdir(this.segmentsDir)) {
      if (entry.includes('.tmp') || !referenced.has(entry)) {
        await unlinkMaybe(path.join(this.segmentsDir, entry));
      }
    }
    for (const entry of await fs.readdir(this.dir)) {
      if (entry.includes('.tmp')) {
        await unlinkMaybe(path.join(this.dir, entry));
      }
    }
  }

  _withLock(fn) {
    const result = this._mutex.then(fn, fn);
    this._mutex = result.then(() => undefined, () => undefined);
    return result;
  }
}

module.exports = { VectorLake, Snapshot, SegmentCorruptionError, canonicalize, sha256 };
