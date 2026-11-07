// Streaming .tar.gz importer for memory-constrained browsers (including iOS).
// The original importer buffered the entire compressed archive, then the entire
// decompressed TAR, then copied each file. This version keeps only small chunks
// in memory and writes each TAR entry to OPFS as it arrives.

function shouldIgnoreEntry(name) {
  const parts = name.split('/');
  const base = parts[parts.length - 1];
  return !name || base === '.DS_Store' || base.startsWith('._');
}

class StreamBuffer {
  constructor(reader) {
    this.reader = reader;
    this.parts = [];
    this.length = 0;
    this.done = false;
  }

  async fill(minBytes) {
    while (this.length < minBytes && !this.done) {
      const { value, done } = await this.reader.read();
      if (done) {
        this.done = true;
        break;
      }
      if (value && value.byteLength) {
        this.parts.push(value);
        this.length += value.byteLength;
      }
    }
  }

  async readExact(size) {
    if (size === 0) return new Uint8Array(0);
    await this.fill(size);
    if (this.length < size) throw new Error('Unexpected end of TAR archive');
    const out = new Uint8Array(size);
    let offset = 0;
    while (offset < size) {
      const first = this.parts[0];
      const take = Math.min(first.byteLength, size - offset);
      out.set(first.subarray(0, take), offset);
      offset += take;
      this.length -= take;
      if (take === first.byteLength) this.parts.shift();
      else this.parts[0] = first.subarray(take);
    }
    return out;
  }

  async copyTo(size, sink) {
    let remaining = size;
    while (remaining > 0) {
      if (this.length === 0) await this.fill(1);
      if (this.length === 0) throw new Error('Unexpected end of TAR file data');
      const first = this.parts[0];
      const take = Math.min(first.byteLength, remaining, 1024 * 1024);
      const chunk = first.subarray(0, take);
      await sink(chunk);
      remaining -= take;
      this.length -= take;
      if (take === first.byteLength) this.parts.shift();
      else this.parts[0] = first.subarray(take);
    }
  }

  async skip(size) {
    await this.copyTo(size, async () => {});
  }
}

function parseOctal(bytes) {
  const text = new TextDecoder().decode(bytes).replace(/\0/g, '').trim();
  if (!text) return 0;
  const value = parseInt(text, 8);
  if (!Number.isFinite(value) || value < 0) throw new Error('Invalid TAR entry size');
  return value;
}

function readString(header, start, length) {
  let end = start;
  while (end < start + length && header[end] !== 0) end++;
  return new TextDecoder().decode(header.subarray(start, end));
}

async function createEntryWriter(name) {
  const root = await navigator.storage.getDirectory();
  const parts = name.split('/').filter(Boolean);
  if (!parts.length) return null;
  let dir = root;
  for (let i = 0; i < parts.length - 1; i++) {
    dir = await dir.getDirectoryHandle(parts[i], { create: true });
  }
  const handle = await dir.getFileHandle(parts[parts.length - 1], { create: true });

  // OPFS sync handles are available in dedicated workers and avoid Safari's
  // large temporary-file behavior with createWritable().
  if (handle.createSyncAccessHandle) {
    try {
      const access = await handle.createSyncAccessHandle();
      access.truncate(0);
      let position = 0;
      return {
        async write(chunk) {
          access.write(chunk, { at: position });
          position += chunk.byteLength;
        },
        async close() {
          access.truncate(position);
          access.flush();
          access.close();
        },
        async abort() { try { access.close(); } catch {} }
      };
    } catch (error) {
      console.warn('[import] sync OPFS handle unavailable; using writable stream', error);
    }
  }

  const writable = await handle.createWritable();
  return {
    async write(chunk) { await writable.write(chunk); },
    async close() { await writable.close(); },
    async abort() { try { await writable.abort(); } catch {} }
  };
}

async function extractTarStream(stream) {
  const reader = stream.getReader();
  const input = new StreamBuffer(reader);
  const decoder = new TextDecoder();
  let pendingLongName = null;
  let entries = 0;

  while (true) {
    const header = await input.readExact(512);
    if (header.every(byte => byte === 0)) break;

    const type = String.fromCharCode(header[156] || 48);
    const rawName = readString(header, 0, 100);
    const prefix = readString(header, 345, 155);
    const size = parseOctal(header.subarray(124, 136));
    const rawPath = pendingLongName || (prefix ? `${prefix}/${rawName}` : rawName);
    pendingLongName = null;
    const name = rawPath.replace(/\0/g, '').replace(/\/$/, '');

    if (type === 'L') {
      const longName = await input.readExact(size);
      pendingLongName = decoder.decode(longName).replace(/\0/g, '');
      const padding = (512 - (size % 512)) % 512;
      if (padding) await input.skip(padding);
      continue;
    }

    const isRegular = type === '0' || type === '\0' || type === '';
    if (!isRegular || shouldIgnoreEntry(name)) {
      await input.skip(size);
    } else {
      const sink = await createEntryWriter(name);
      try {
        if (sink) await input.copyTo(size, chunk => sink.write(chunk));
        else await input.skip(size);
        if (sink) await sink.close();
      } catch (error) {
        if (sink) await sink.abort();
        throw error;
      }
      entries++;
      if (entries % 10 === 0) {
        self.postMessage({
          type: 'progress',
          phase: 'extracting',
          pct: Math.min(90, 30 + Math.floor(entries / 10)),
          done: entries,
          total: 0
        });
      }
    }

    const padding = (512 - (size % 512)) % 512;
    if (padding) await input.skip(padding);
  }

  return entries;
}

self.onmessage = async event => {
  const { file } = event.data;
  try {
    if (!file || typeof file.stream !== 'function') {
      throw new Error('Selected file cannot be streamed by this browser');
    }
    if (typeof DecompressionStream === 'undefined') {
      throw new Error('This browser does not support streaming GZip decompression');
    }

    self.postMessage({ type: 'progress', phase: 'reading', pct: 0 });
    let loaded = 0;
    const progressStream = new TransformStream({
      transform(chunk, controller) {
        loaded += chunk.byteLength;
        const pct = Math.min(10, Math.round((loaded / file.size) * 10));
        self.postMessage({ type: 'progress', phase: 'reading', pct });
        controller.enqueue(chunk);
      }
    });

    self.postMessage({ type: 'progress', phase: 'decompressing', pct: 10 });
    const decompressed = file.stream()
      .pipeThrough(progressStream)
      .pipeThrough(new DecompressionStream('gzip'));

    self.postMessage({ type: 'progress', phase: 'extracting', pct: 30 });
    const count = await extractTarStream(decompressed);

    const root = await navigator.storage.getDirectory();
    const marker = await root.getFileHandle('_game_ready', { create: true });
    const writable = await marker.createWritable();
    await writable.write(new TextEncoder().encode('v4'));
    await writable.close();

    self.postMessage({ type: 'progress', phase: 'extracting', pct: 95, done: count, total: count });
    self.postMessage({ type: 'done' });
  } catch (err) {
    self.postMessage({ type: 'error', message: err && err.message ? err.message : String(err) });
  }
};
