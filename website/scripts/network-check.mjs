import assert from 'node:assert/strict';
import { openLab } from './lab-browser.mjs';

const { lab, browser } = await openLab();
const runtimeErrors = [];
let deadline;

try {
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on('pageerror', error => runtimeErrors.push(error.stack || error.message));
  page.on('console', message => { if (message.type() === 'error') runtimeErrors.push(message.text()); });
  const response = await page.goto(lab.href, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  assert.ok(response?.ok(), `Lab HTTP ${response?.status()}`);

  let isolation;
  for (let reloads = 0; reloads <= 2; reloads++) {
    isolation = await page.evaluate(async helperUrl => {
      const { ensureCrossOriginIsolation } = await import(helperUrl);
      return ensureCrossOriginIsolation({ reload: false });
    }, new URL('coi.js', lab).href);
    if (isolation.isolated) break;
    assert.equal(isolation.reason, 'reload the page to apply cross-origin isolation', isolation.reason);
    assert.ok(reloads < 2, 'Scoped service worker did not isolate the lab after two reloads');
    await page.reload({ waitUntil: 'domcontentloaded', timeout: 30_000 });
  }
  await page.waitForFunction(() => globalThis.crossOriginIsolated, null, { timeout: 10_000 });
  assert.ok(['service-worker', 'headers'].includes(isolation.via), `Unexpected isolation: ${JSON.stringify(isolation)}`);
  if (isolation.via === 'service-worker') {
    const scope = await page.evaluate(async () => (await navigator.serviceWorker.getRegistration(location.href))?.scope);
    assert.equal(scope, new URL('./', lab).href, 'Isolation service worker must be scoped to the lab');
  }

  const output = await Promise.race([
    page.evaluate(async labUrl => {
      const response = await fetch(new URL('krabka-lab-network.wasm', labUrl));
      if (!response.ok) throw new Error(`Network guest HTTP ${response.status}; build the WASI lab artifacts first`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      if (![0, 97, 115, 109, 1, 0, 0, 0].every((byte, index) => bytes[index] === byte)) {
        throw new Error('Network guest is not a WebAssembly module');
      }
      const { Wasmer } = await import(new URL('sdk/dist/index.js', labUrl).href);
      const client = new Wasmer({ parallelism: 2, cache: { namespace: 'krabka-o11y-network-check' } });
      let sandbox;
      let result;
      let cleanupMs;
      try {
        await client.ready();
        const name = 'krabka-lab-network';
        const pkg = await client.packages.create({ modules: { [name]: bytes }, commands: { [name]: { module: name } } });
        sandbox = await client.sandboxes.create({ packages: [pkg], network: { mode: 'http', peers: [] } });
        const output = await sandbox.command(name, ['--self-test']).run({ check: false, timeoutMs: 60_000, outputBytes: 1024 * 1024 });
        const started = performance.now();
        const waiting = await sandbox.command(name, ['--deadline-test']).spawn({ stdin: 'closed', stdout: 'pipe', stderr: 'pipe', timeoutMs: 3000, outputBytes: 1024 * 1024 });
        const phase = await waiting.stdout.lines()[Symbol.asyncIterator]().next();
        if (phase.value !== 'deadline-ready') throw new Error(`Guest did not enter its blocking wait: ${JSON.stringify(phase)}`);
        const observed = performance.now();
        const timedOut = await waiting.wait({ check: false });
        const deadline = { reason: timedOut.reason, exitCode: timedOut.exitCode, elapsedMs: performance.now() - started, waitMs: performance.now() - observed };
        result = { exitCode: output.exitCode, reason: output.reason, stdout: output.stdout.text(), stderr: output.stderr.text(), deadline };
      } finally {
        const started = performance.now();
        let closeDeadline;
        try {
          await Promise.race([
            (async () => { await sandbox?.close(); await client.close(); })(),
            new Promise((_, reject) => { closeDeadline = setTimeout(() => reject(new Error('SDK cleanup exceeded 10 seconds with a native atomic waiter')), 10_000); }),
          ]);
        } finally {
          clearTimeout(closeDeadline);
        }
        cleanupMs = performance.now() - started;
      }
      return { ...result, cleanupMs };
    }, lab.href),
    new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error('Browser network guest exceeded 120 seconds')), 120_000); }),
  ]);
  assert.equal(output.reason, 'exited', `Guest ${output.reason}: ${output.stderr}`);
  assert.equal(output.exitCode, 0, `Guest exit ${output.exitCode}: ${output.stderr}\n${output.stdout}`);
  assert.ok(output.stdout.trim(), 'The actual guest must emit its self-test results');
  const result = JSON.parse(output.stdout.trim());
  assert.equal(result.tcpPorts?.length, 2, 'Guest must report two TCP listeners');
  assert.ok(result.tcpPorts.every(port => Number.isInteger(port) && port > 0 && port <= 65535), 'TCP bind(0) must allocate real ports');
  assert.notEqual(result.tcpPorts[0], result.tcpPorts[1], 'TCP bind(0) listeners must have distinct ports');
  assert.equal(result.tcpEchoBytes, 2 * Buffer.byteLength('krabka-o11y TCP → π\n'), 'Both TCP listeners must echo the complete UTF-8 payload');
  assert.equal(result.udpPort, result.tcpPorts[0], 'TCP and UDP must have independent port namespaces');
  assert.ok(Number.isInteger(result.udpSourcePort) && result.udpSourcePort > 0 && result.udpSourcePort <= 65535, 'UDP bind(0) must allocate a real source port');
  assert.notEqual(result.udpSourcePort, result.udpPort, 'UDP sender and receiver must have distinct ports');
  assert.equal(result.udpDatagrams, 3);
  assert.equal(result.udpPeekBytes, 5);
  assert.equal(result.udpTruncatedBytes, 2);
  assert.equal(result.udpReplyBytes, 4);
  assert.equal(result.zeroLengthDatagram, true);
  assert.equal(result.udpReadTimedOut, true, 'An empty UDP receive must honor its actual deadline');
  assert.equal(result.asyncEchoBytes, 2 * Buffer.byteLength('krabka-o11y TCP → π\n'), 'Tokio must echo both complete frames on a persistent connection');
  assert.ok(Number.isInteger(result.asyncTimerTicks) && result.asyncTimerTicks >= 2, 'Short Tokio timers must advance during TCP exchanges');
  assert.equal(result.parkedLockValue, 401, 'Timed mutex contention and concurrent handoffs must retain every update');
  assert.equal(output.deadline.reason, 'timeout', 'The SDK must enforce command deadlines while guest threads block');
  assert.equal(output.deadline.exitCode, 137);
  assert.ok(output.deadline.elapsedMs < 8000, `A 3 second command deadline took ${output.deadline.elapsedMs} ms`);
  assert.ok(output.deadline.waitMs > 0 && output.deadline.waitMs < 5000, 'The SDK must cancel the guest after observing its blocking wait');
  assert.ok(output.cleanupMs < 10_000, `Closing a killed guest with a blocking child took ${output.cleanupMs} ms`);
  assert.equal(runtimeErrors.length, 0, `Browser runtime errors:\n${runtimeErrors.slice(0, 20).join('\n')}`);
  console.log(`Actual WASIX network guest passed (${isolation.via}, exit 0, cleanup ${Math.round(output.cleanupMs)} ms): ${JSON.stringify(result)}`);
} catch (error) {
  if (runtimeErrors.length) console.error(`Browser runtime errors:\n${runtimeErrors.slice(0, 20).join('\n')}`);
  throw error;
} finally {
  clearTimeout(deadline);
  await browser.close();
}
