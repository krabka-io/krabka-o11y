import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Lab } from '../src/lab/runtime.js';

const deferred = () => Promise.withResolvers();
const hash = async bytes => Buffer.from(await crypto.subtle.digest('SHA-256', bytes)).toString('hex');

async function fixture(t) {
  const bytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const sha256 = await hash(bytes);
  const lab = new Lab();
  lab.manifest = {
    modules: ['broker', 'format', 'http', 'metrics'].map(name => ({ name, file: `${name}.wasm`, bytes: bytes.length, sha256 })),
    qualificationModules: [{ name: 'network', file: 'network.wasm', bytes: bytes.length, sha256 }],
  };
  const requests = [];
  t.mock.method(globalThis, 'fetch', (url, { signal }) => {
    const response = deferred();
    requests.push({ url, signal, response });
    signal.addEventListener('abort', () => response.reject(signal.reason), { once: true });
    return response.promise;
  });
  return { lab, bytes, requests };
}

// Advance promise continuations without a timing-based concurrency assertion.
const tick = () => new Promise(resolve => setImmediate(resolve));

test('downloads at most two modules through hashing and excludes qualification modules', async t => {
  const { lab, bytes, requests } = await fixture(t);
  const digests = [];
  t.mock.method(crypto.subtle, 'digest', () => {
    const digest = deferred();
    digests.push(digest);
    return digest.promise;
  });
  const loading = lab.downloadModules();
  assert.equal(requests.length, 2);
  requests[0].response.resolve(new Response(bytes));
  requests[1].response.resolve(new Response(bytes));
  await tick();
  assert.equal(digests.length, 2);
  assert.equal(requests.length, 2, 'Hashing must retain its concurrency slot');
  const digest = Buffer.from(lab.manifest.modules[0].sha256, 'hex');
  digests[1].resolve(digest);
  await tick();
  assert.equal(requests.length, 3, 'A completed slot should immediately start the next download');
  digests[0].resolve(digest);
  await tick();
  requests[2].response.resolve(new Response(bytes));
  requests[3].response.resolve(new Response(bytes));
  await tick();
  digests[2].resolve(digest);
  digests[3].resolve(digest);
  const modules = await loading;
  assert.deepEqual(Object.keys(modules).sort(), lab.manifest.modules.map(module => module.name).sort());
  assert.ok(Object.values(modules).every(module => Buffer.from(module).equals(bytes)));
  assert.ok(requests.every(({ url }) => !url.includes('network')));
});

for (const mode of ['http', 'integrity', 'cancel']) {
  test(`${mode} failure cancels sibling downloads and leaves queued modules untouched`, async t => {
    const { lab, requests } = await fixture(t);
    const loading = lab.downloadModules();
    const expected = mode === 'http' ? /broker download failed \(HTTP 503\)/
      : mode === 'integrity' ? /broker did not match the build manifest/ : /cancelled/;
    const rejected = assert.rejects(loading, expected);
    if (mode === 'cancel') lab.abort.abort(new Error('cancelled'));
    else requests[0].response.resolve(new Response('bad bytes', { status: mode === 'http' ? 503 : 200 }));
    await rejected;
    await tick();
    assert.equal(requests.length, 2);
    assert.ok(requests.every(({ signal }) => signal.aborted));
    assert.equal(lab.ready, false);
    await lab.stop();
    const restarted = new Lab();
    assert.equal(restarted.abort.signal.aborted, false);
    await restarted.parallel([1], async () => {});
  });
}

test('service startup slots stay occupied until all listeners are ready', async () => {
  const lab = new Lab();
  const listeners = new Map();
  let spawns = 0;
  lab.sandbox = {
    command: () => ({ spawn: async () => {
      spawns++;
      return { wait: () => new Promise(() => {}), kill: async () => {} };
    } }),
    ports: { wait: port => {
      const listener = deferred();
      listeners.set(port, listener);
      return listener.promise;
    } },
  };
  const starting = lab.parallel([[3200, 4318], 3100, 4040], port => lab.spawn('traces', 'service', 'guest', [], port));
  await tick();
  assert.equal(spawns, 2);
  listeners.get(3200).resolve();
  await tick();
  assert.equal(spawns, 2, 'Trace ingestion listener must also be ready');
  listeners.get(4318).resolve();
  await tick();
  assert.equal(spawns, 3);
  listeners.get(3100).resolve();
  listeners.get(4040).resolve();
  await starting;
});

test('a late concurrent spawn is killed when startup is cancelled', async () => {
  const lab = new Lab();
  const spawned = [deferred(), deferred()];
  let calls = 0;
  let killed = 0;
  lab.sandbox = { command: () => ({ spawn: () => spawned[calls++].promise }) };
  const starting = lab.parallel([1, 2, 3], port => lab.spawn('logs', 'logs', 'guest', [], port));
  const rejected = assert.rejects(starting, /cancelled/);
  lab.abort.abort(new Error('cancelled'));
  spawned[0].resolve({ kill: async () => { killed++; } });
  await rejected;
  assert.equal(killed, 1);
  spawned[1].resolve({ kill: async () => { killed++; } });
  await tick();
  assert.equal(calls, 2);
  assert.equal(killed, 2);
  assert.deepEqual(lab.processes, []);
});

test('an unexpected service exit preserves its role diagnostic and aborts queued work', async () => {
  const lab = new Lab();
  const exited = deferred();
  const states = [];
  lab.status = (...args) => states.push(args);
  let spawns = 0;
  lab.sandbox = {
    command: () => ({ spawn: async () => {
      spawns++;
      return { wait: () => exited.promise };
    } }),
    ports: { wait: () => new Promise(() => {}) },
  };
  const starting = lab.parallel([1, 2, 3], port => lab.spawn('logs', 'logs', 'guest', [], port));
  const rejected = assert.rejects(starting, /logs exited \(17\). Check the service logs/);
  await tick();
  exited.resolve({ exitCode: 17 });
  await rejected;
  assert.equal(spawns, 2);
  assert.ok(states.some(([signal, message]) => signal === 'failed' && message.includes('logs exited (17)')));
  assert.equal(lab.ready, false);
});
