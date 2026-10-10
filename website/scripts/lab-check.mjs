import assert from 'node:assert/strict';
import fs from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { openLab } from './lab-browser.mjs';

const { lab, browser } = await openLab();
const runtimeErrors = [];
const consoleErrors = [];
const screenshots = new URL('../.tools/screenshots/', import.meta.url);
fs.mkdirSync(screenshots, { recursive: true });
const totalTimeoutMs = 15 * 60_000;
const deadline = Date.now() + totalTimeoutMs;
const tenant = `lab-check-${Date.now().toString(16)}`;
const otherTenant = `${tenant}-other`;
const traceID = '1a4b8c6d0e2f4a6789abcdef01234567';
const profileType = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds';
const queries = {
  metrics: 'lab_requests_total',
  logs: '{service="checkout"}',
  traces: '{ resource.service.name = "checkout" }',
  profiles: `${profileType}{service_name="checkout"}`,
};
let page;
let timer;
let stage = 'Open lab';
let lastStatus;
let lastProgress = 0;

function timeout(maximum) {
  const remaining = deadline - Date.now();
  if (remaining <= 0) throw new Error(`Lab check exceeded ${totalTimeoutMs / 60_000} minutes during: ${stage}`);
  return Math.min(maximum, remaining);
}

function progress(message) {
  console.log(`[lab-check] ${message}`);
}

async function healthy() {
  assert.equal(runtimeErrors.length, 0, `Browser runtime errors:\n${runtimeErrors.join('\n')}`);
  const state = await page.evaluate(() => ({
    status: document.getElementById('lab-status').textContent,
    indicator: document.getElementById('lab-indicator').dataset.status,
    error: document.getElementById('lab-error').textContent,
  }));
  if (state.status !== lastStatus || Date.now() - lastProgress > 15_000) {
    progress(state.status);
    if (state.status === lastStatus) {
      const logs = await page.locator('#lab-logs-output').textContent();
      progress(`Guest stdout/stderr (last 30 lines):\n${logs.split('\n').slice(-30).join('\n')}`);
    }
    lastStatus = state.status;
    lastProgress = Date.now();
  }
  assert.notEqual(state.indicator, 'failed', `${state.status}: ${state.error}`);
  return state;
}

async function startStack(firstStart = false) {
  stage = firstStart ? 'First Start click and isolation reload' : 'Start production stack';
  const navigation = firstStart
    ? page.waitForEvent('framenavigated', { predicate: frame => frame === page.mainFrame(), timeout: timeout(30_000) })
    : Promise.resolve();
  // Only the app installs the worker and resumes this one click after its reload.
  await Promise.all([navigation, page.locator('#lab-start').click({ timeout: timeout(30_000) })]);
  await page.waitForFunction(() => globalThis.crossOriginIsolated, null, { timeout: timeout(30_000) });
  await page.waitForLoadState('domcontentloaded', { timeout: timeout(30_000) });
  const isolation = await page.evaluate(async () => ({
    scope: (await navigator.serviceWorker.getRegistration(location.href))?.scope,
    controller: navigator.serviceWorker.controller?.scriptURL,
  }));
  assert.equal(isolation.scope, lab.href, 'Isolation worker must be scoped to the lab');
  assert.ok(isolation.controller, 'The isolation worker must control the page');
  assert.equal(new URL(isolation.controller).pathname, new URL('coi-sw.js', lab).pathname);
  progress(firstStart ? 'First Start click resumed after the scoped isolation worker reload.' : 'Restart uses the existing scoped isolation worker.');
  stage = 'Start production stack';
  for (;;) {
    timeout(1);
    const state = await healthy();
    if (state.indicator === 'ready') break;
    await delay(1000);
  }
  assert.equal(await page.locator('#lab-status').textContent(), 'Stack ready');
  assert.equal(await page.locator('[data-service][data-status="ready"]').count(), 5, 'Broker and all four signals must be ready');
  assert.ok(await page.locator('#lab-seed').isEnabled(), 'Ready stack must enable ingestion');
  assert.ok(await page.locator('#lab-run').isEnabled(), 'Ready stack must enable queries');
}

async function query(signal, text = queries[signal], queryTenant = tenant, allowApiError = false) {
  await healthy();
  await page.locator(`#tab-${signal}`).click({ timeout: timeout(10_000) });
  await page.locator('#lab-tenant').fill(queryTenant);
  await page.locator('#lab-query').fill(text);
  await page.locator('#lab-run').click({ timeout: timeout(10_000) });
  await page.waitForFunction(() => !document.getElementById('lab-run').disabled
    || document.getElementById('lab-indicator').dataset.status === 'failed', null, { timeout: timeout(65_000) });
  await healthy();
  const response = await page.evaluate(() => ({
    meta: document.getElementById('lab-response-meta').textContent,
    raw: document.getElementById('lab-result').textContent,
    error: document.getElementById('lab-error').hidden ? '' : document.getElementById('lab-error').textContent,
  }));
  const status = Number(/^HTTP (\d{3})\b/.exec(response.meta)?.[1]);
  assert.ok(Number.isInteger(status), `${signal}: no actual HTTP response: ${JSON.stringify(response)}`);
  if (!allowApiError) {
    assert.equal(response.error, '', `${signal}: ${response.error}`);
    assert.ok(status >= 200 && status < 300, `${signal}: HTTP ${status}: ${response.raw}`);
  }
  let data;
  try { data = JSON.parse(response.raw); } catch { data = response.raw; }
  return { ...response, status, data };
}

function metricValues(data) {
  assert.equal(data.status, 'success');
  assert.equal(data.data?.resultType, 'vector');
  assert.ok(Array.isArray(data.data.result));
  return data.data.result;
}

function logLines(data) {
  assert.equal(data.status, 'success');
  assert.equal(data.data?.resultType, 'streams');
  assert.ok(Array.isArray(data.data.result));
  return data.data.result.flatMap(stream => {
    assert.equal(stream.stream.service, 'checkout');
    return stream.values.map(([time, line]) => { assert.ok(BigInt(time) > 0n); return line; });
  });
}

function profileBars(data) {
  assert.ok(Array.isArray(data.flamebearer?.names));
  assert.ok(Array.isArray(data.flamebearer?.levels));
  const bars = [];
  for (const [depth, level] of data.flamebearer.levels.entries()) {
    assert.ok(Array.isArray(level) && level.length % 4 === 0, 'Flamebearer levels contain [offset,total,self,nameIndex] groups');
    for (let index = 0; index < level.length; index += 4) {
      const [offset, total, self, nameIndex] = level.slice(index, index + 4);
      assert.ok([offset, total, self, nameIndex].every(Number.isSafeInteger));
      assert.ok(nameIndex >= 0 && nameIndex < data.flamebearer.names.length);
      bars.push({ name: data.flamebearer.names[nameIndex], depth, total, self });
    }
  }
  return bars;
}

function seeded(signal, data) {
  if (signal === 'metrics') {
    const series = metricValues(data);
    const values = series.map(row => {
      assert.equal(row.metric.__name__, 'lab_requests_total');
      assert.ok(Number(row.value[0]) > 0);
      return [row.metric.service, Number(row.value[1])];
    }).sort(([left], [right]) => left.localeCompare(right));
    assert.deepEqual(values, [['checkout', 42], ['payments', 18]]);
  } else if (signal === 'logs') {
    assert.deepEqual(logLines(data).sort(), ['checkout error: payment retry', 'checkout request completed']);
  } else if (signal === 'traces') {
    assert.ok(Array.isArray(data.traces));
    assert.equal(data.traces.length, 1);
    const trace = data.traces[0];
    assert.equal(trace.traceID, traceID);
    assert.equal(trace.rootServiceName, 'checkout');
    assert.equal(trace.rootTraceName, 'POST /checkout');
    assert.equal(trace.durationMs, 180);
    assert.ok(BigInt(trace.startTimeUnixNano) > 0n);
  } else {
    // The UI's real pprof has two CPU samples: 24ms and 48ms, in nanoseconds.
    assert.equal(data.flamebearer?.numTicks, 72_000_000);
    assert.equal(data.flamebearer.maxSelf, 48_000_000);
    assert.equal(data.metadata?.name, profileType);
    assert.equal(data.metadata.units, 'nanoseconds');
    const bars = profileBars(data);
    for (const expected of [
      { name: 'checkout', depth: 1, total: 72_000_000, self: 0 },
      { name: 'validate_cart', depth: 2, total: 24_000_000, self: 24_000_000 },
      { name: 'charge_payment', depth: 2, total: 48_000_000, self: 48_000_000 },
    ]) assert.deepEqual(bars.find(bar => bar.name === expected.name), expected);
  }
}

function empty(signal, data) {
  if (signal === 'metrics') assert.deepEqual(metricValues(data), []);
  else if (signal === 'logs') assert.deepEqual(logLines(data), []);
  else if (signal === 'traces') assert.deepEqual(data.traces, []);
  else {
    assert.equal(data.flamebearer?.numTicks, 0);
    const names = profileBars(data).map(bar => bar.name);
    assert.ok(!names.some(name => ['checkout', 'validate_cart', 'charge_payment'].includes(name)), 'Empty tenant must not expose seeded profile frames');
  }
}

async function stopStack() {
  await page.locator('#lab-stop').click({ timeout: timeout(10_000) });
  await page.waitForFunction(() => !document.getElementById('lab-start').disabled, null, { timeout: timeout(10_000) });
  assert.ok(await page.locator('#lab-error').isHidden(), `Stop cleanup failed: ${await page.locator('#lab-error').textContent()}`);
  assert.equal(await page.locator('#lab-status').textContent(), 'Stopped');
  assert.equal(await page.locator('#lab-indicator').getAttribute('data-status'), '');
  assert.ok(await page.locator('#lab-stop').isDisabled());
  assert.ok(await page.locator('#lab-seed').isDisabled());
  assert.ok(await page.locator('#lab-run').isDisabled());
  assert.deepEqual(await page.locator('[data-service] small').allTextContents(), Array(5).fill('Stopped'));
  progress('Stopped stack; controls and service cards cleared.');
}

async function check() {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: 1 });
  page = await context.newPage();
  page.setDefaultTimeout(15_000);
  page.on('pageerror', error => runtimeErrors.push(error.stack || error.message));
  page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  const response = await page.goto(lab.href, { waitUntil: 'domcontentloaded', timeout: timeout(30_000) });
  assert.ok(response?.ok(), `Lab HTTP ${response?.status()}`);
  assert.equal(response.headers()['cross-origin-opener-policy'], undefined, 'Use the ordinary preview without isolation headers');
  assert.equal(response.headers()['cross-origin-embedder-policy'], undefined, 'The real scoped worker must supply isolation');
  assert.equal(await page.evaluate(() => globalThis.crossOriginIsolated), false, 'Fresh ordinary preview context must start without isolation');
  assert.equal(await page.evaluate(() => navigator.serviceWorker.controller?.scriptURL), undefined, 'Fresh context must start without a worker controller');

  await startStack(true);
  stage = 'Ingest actual sample telemetry';
  await page.locator('#tab-metrics').click();
  await page.locator('#lab-tenant').fill(tenant);
  await page.locator('#lab-seed').click();
  await page.waitForFunction(() => !document.getElementById('lab-seed').disabled
    || document.getElementById('lab-indicator').dataset.status === 'failed', null, { timeout: timeout(120_000) });
  await healthy();
  assert.ok(await page.locator('#lab-error').isHidden(), `Sample ingestion failed: ${await page.locator('#lab-error').textContent()}`);
  const logs = await page.locator('#lab-logs-output').textContent();
  for (const endpoint of ['4041/api/v1/push/influx/write?precision=ms', '3100/loki/api/v1/push', '4318/api/v2/spans', '4040/push.v1.PusherService/Push']) {
    assert.ok(logs.split('\n').some(line => line.startsWith(`[sample] ${endpoint}: HTTP 2`)), `No successful actual ingestion response for ${endpoint}`);
  }
  progress(`All four ingestion APIs accepted telemetry for ${tenant}.`);

  stage = 'Read seeded data through all four UI query tabs';
  const pending = new Set(Object.keys(queries));
  const mismatches = {};
  const publishedBy = Date.now() + timeout(180_000);
  while (pending.size) {
    for (const signal of [...pending]) {
      const response = await query(signal);
      try { seeded(signal, response.data); }
      catch (error) {
        mismatches[signal] = `${error.message}\nLast actual response: ${response.raw}`;
        continue;
      }
      pending.delete(signal);
      progress(`${signal}: expected seeded values returned by the actual public API.`);
      if (signal === 'metrics') {
        assert.equal(await page.locator('[data-service][data-status="ready"]').count(), 5, 'Running screenshot requires every service to be ready');
        await page.evaluate(() => document.fonts.ready);
        await page.evaluate(() => scrollTo({ top: 0, left: 0, behavior: 'instant' }));
        await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
        await page.screenshot({ path: fileURLToPath(new URL('lab-running-desktop.png', screenshots)), fullPage: true });
        progress('Saved lab-running-desktop.png with validated seeded metrics and all five services ready.');
      }
    }
    if (pending.size) {
      assert.ok(Date.now() < publishedBy, `Seeded data did not appear:\n${[...pending].map(signal => `${signal}: ${mismatches[signal]}`).join('\n')}`);
      progress(`Waiting for builders/query stores: ${[...pending].join(', ')}`);
      await delay(2000);
    }
  }

  stage = 'Verify query evaluation and tenant isolation';
  const sum = await query('metrics', 'sum(lab_requests_total)');
  assert.equal(metricValues(sum.data).length, 1);
  assert.equal(Number(sum.data.data.result[0].value[1]), 60, 'PromQL aggregation must calculate 42+18');
  const filtered = await query('logs', '{service="checkout"} |= "error"');
  assert.deepEqual(logLines(filtered.data), ['checkout error: payment retry']);
  for (const signal of Object.keys(queries)) {
    seeded(signal, (await query(signal)).data);
    empty(signal, (await query(signal, queries[signal], otherTenant)).data);
    progress(`${signal}: seeded tenant remains populated; separate tenant returns no samples.`);
  }

  stage = 'Verify actual malformed-query error';
  const invalid = await query('metrics', 'sum(', tenant, true);
  assert.equal(invalid.status, 400);
  assert.equal(invalid.data.status, 'error');
  assert.equal(invalid.data.errorType, 'bad_data');
  assert.ok(typeof invalid.data.error === 'string' && invalid.data.error.length > 0);
  assert.match(invalid.error, /Query returned HTTP 400/);
  progress('Malformed PromQL returned the actual HTTP 400 bad_data response shown in the UI.');

  stage = 'Stop and restart cleanup';
  await stopStack();
  await startStack();
  stage = 'Verify fresh data after restart';
  for (const signal of Object.keys(queries)) empty(signal, (await query(signal)).data);
  progress('Restarted stack returned no previous tenant data on any tab.');

  stage = 'Verify shutdown error reporting';
  const cleanupError = 'Injected close failure after worker cleanup';
  await page.evaluate(async ({ sdkUrl, message }) => {
    const { Wasmer } = await import(sdkUrl);
    const close = Wasmer.prototype.close;
    globalThis.restoreLabCheckClose = () => { Wasmer.prototype.close = close; delete globalThis.restoreLabCheckClose; };
    Wasmer.prototype.close = async function () { await close.call(this); throw new Error(message); };
  }, { sdkUrl: new URL('sdk/dist/index.js', lab).href, message: cleanupError });
  try {
    await page.locator('#lab-stop').click({ timeout: timeout(10_000) });
    await page.waitForFunction(() => !document.getElementById('lab-start').disabled, null, { timeout: timeout(10_000) });
    assert.equal(await page.locator('#lab-status').textContent(), 'Stop failed');
    assert.equal(await page.locator('#lab-indicator').getAttribute('data-status'), 'failed');
    assert.ok(await page.locator('#lab-error').isVisible());
    assert.equal(await page.locator('#lab-error').textContent(), cleanupError);
    assert.deepEqual(await page.locator('[data-service] small').allTextContents(), Array(5).fill('Stop failed'));
    assert.ok(await page.locator('#lab-stop').isDisabled() && await page.locator('#lab-seed').isDisabled() && await page.locator('#lab-run').isDisabled());
    progress('Actual workers closed; an injected cleanup error was shown as Stop failed.');
  } finally {
    await page.evaluate(() => globalThis.restoreLabCheckClose());
  }
  assert.equal(runtimeErrors.length, 0, `Browser runtime errors:\n${runtimeErrors.join('\n')}`);
  assert.equal(consoleErrors.length, 0, `Browser console errors:\n${consoleErrors.join('\n')}`);
  progress('PASS: real browser stack, telemetry, all four queries, tenant isolation, API errors, stop cleanup, and fresh restart.');
}

try {
  await Promise.race([
    check(),
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Lab check exceeded ${totalTimeoutMs / 60_000} minutes during: ${stage}`)), timeout(totalTimeoutMs)); }),
  ]);
} catch (error) {
  console.error(`[lab-check] FAILED during: ${stage}\n${error.stack || error.message}`);
  if (runtimeErrors.length || consoleErrors.length) console.error(`Browser errors:\n${[...runtimeErrors, ...consoleErrors].slice(-100).join('\n')}`);
  if (page && !page.isClosed()) {
    try {
      console.error(`Last UI response:\n${await page.locator('#lab-result').textContent({ timeout: 3000 })}`);
      console.error(`Guest stdout/stderr:\n${await page.locator('#lab-logs-output').textContent({ timeout: 3000 })}`);
    } catch (diagnosticError) { console.error(`Could not read browser diagnostics: ${diagnosticError.message}`); }
  }
  throw error;
} finally {
  clearTimeout(timer);
  if (page && !page.isClosed()) {
    try {
      if (await page.locator('#lab-stop').isEnabled()) {
        await page.locator('#lab-stop').click({ timeout: 3000 });
        await page.waitForFunction(() => !document.getElementById('lab-start').disabled, null, { timeout: 10_000 });
      }
    } catch (error) { console.error(`Stop cleanup: ${error.message}`); }
  }
  await browser.close();
}
