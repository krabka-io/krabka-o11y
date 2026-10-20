import assert from 'node:assert/strict';
import fs from 'node:fs';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// Run against the ordinary preview, with no other browser checks running.
// Linux /proc RSS includes guest workers and WASM memory, unlike JS heap metrics.
const { values } = parseArgs({ options: {
  'baseline-runtime': { type: 'string' },
  manifest: { type: 'string', default: fileURLToPath(new URL('../static/lab/manifest.json', import.meta.url)) },
  url: { type: 'string', default: 'http://127.0.0.1:4322/krabka-o11y/lab/' },
} });
assert.ok(values['baseline-runtime'], 'Supply --baseline-runtime with the pre-change runtime.js file.');
assert.equal(process.platform, 'linux', 'Aggregate browser RSS measurement requires Linux /proc.');
assert.equal(new URL(values.url).pathname, '/krabka-o11y/lab/');
const root = new URL('../dist/lab/', import.meta.url);
const savedManifest = fs.readFileSync(new URL('manifest.json', root));
const original = JSON.parse(fs.readFileSync(values.manifest));

function memory(pid) {
  const processes = new Map();
  for (const entry of fs.readdirSync('/proc')) {
    if (!/^\d+$/.test(entry)) continue;
    try {
      const status = fs.readFileSync(`/proc/${entry}/status`, 'utf8');
      processes.set(Number(entry), {
        parent: Number(/^PPid:\s+(\d+)/m.exec(status)?.[1]),
        rss: Number(/^VmRSS:\s+(\d+)/m.exec(status)?.[1] ?? 0) * 1024,
      });
    } catch (error) {
      // Processes may exit between listing /proc and reading their status.
      if (error.code !== 'ENOENT' && error.code !== 'ESRCH') throw error;
    }
  }
  const children = new Set([pid]);
  for (let count = -1; count !== children.size;) {
    count = children.size;
    for (const [id, process] of processes) if (children.has(process.parent)) children.add(id);
  }
  return [...children].reduce((sum, id) => sum + (processes.get(id)?.rss ?? 0), 0);
}

async function measure(page, pid) {
  let peak = memory(pid);
  let deadline;
  const sampling = setInterval(() => { peak = Math.max(peak, memory(pid)); }, 100);
  try {
    const result = await Promise.race([
      page.evaluate(async () => {
        performance.clearResourceTimings();
        globalThis.benchmarkLab = new BenchmarkLab();
        const start = performance.now();
        await benchmarkLab.start();
        const readyMs = performance.now() - start;
        const resources = performance.getEntriesByType('resource');
        const modules = resources.filter(resource => /^krabka-.*\.wasm$/.test(new URL(resource.name).pathname.split('/').pop()));
        return {
          readyMs,
          pageTransferBytes: resources.reduce((sum, resource) => sum + resource.transferSize, 0),
          moduleTransferBytes: modules.reduce((sum, resource) => sum + resource.transferSize, 0),
          moduleBytes: modules.reduce((sum, resource) => sum + resource.decodedBodySize, 0),
          moduleFiles: modules.map(resource => new URL(resource.name).pathname.split('/').pop()).sort(),
        };
      }),
      new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error('Startup exceeded 180 seconds')), 180_000); }),
    ]);
    return { ...result, peakAggregateBrowserRssBytes: Math.max(peak, memory(pid)) };
  } finally {
    clearTimeout(deadline);
    clearInterval(sampling);
    await page.evaluate(() => globalThis.benchmarkLab?.stop());
  }
}

try {
  fs.copyFileSync(values['baseline-runtime'], new URL('baseline-runtime.js', root));
  fs.copyFileSync(new URL('../src/lab/runtime.js', import.meta.url), new URL('candidate-runtime.js', root));
  for (const variant of ['baseline', 'candidate']) {
    const manifest = structuredClone(original);
    manifest.modules.push(...(manifest.qualificationModules ?? []));
    if (variant === 'candidate') {
      manifest.qualificationModules = manifest.modules.filter(module => module.name === 'krabka-lab-network');
      manifest.modules = manifest.modules.filter(module => module.name !== 'krabka-lab-network');
      manifest.qualificationBytes = manifest.qualificationModules.reduce((sum, module) => sum + module.bytes, 0);
    } else {
      delete manifest.qualificationModules;
      delete manifest.qualificationBytes;
    }
    manifest.totalBytes = manifest.modules.reduce((sum, module) => sum + module.bytes, 0);
    fs.writeFileSync(new URL('manifest.json', root), JSON.stringify(manifest));
    const server = await chromium.launchServer({ headless: true, args: ['--no-sandbox'] });
    try {
      const browser = await chromium.connect(server.wsEndpoint());
      const context = await browser.newContext();
      const page = await context.newPage();
      page.on('pageerror', error => console.error(error.message));
      await page.goto(values.url);
      await page.evaluate(async () => (await import('./coi.js')).ensureCrossOriginIsolation({ reload: false }));
      await page.reload();
      await page.waitForFunction(() => crossOriginIsolated);
      await page.evaluate(async variant => { globalThis.BenchmarkLab = (await import(`./${variant}-runtime.js`)).Lab; }, variant);
      // Fresh Chromium profile, then a fresh sandbox in that same page/cache.
      for (const cache of ['cold', 'warm']) {
        const result = await measure(page, server.process().pid);
        assert.deepEqual(result.moduleFiles, manifest.modules.map(module => module.file).sort());
        console.log(JSON.stringify({ variant, cache, browser: browser.version(), ...result }));
      }
      await browser.close();
    } finally { await server.close(); }
  }
} finally {
  fs.writeFileSync(new URL('manifest.json', root), savedManifest);
  for (const variant of ['baseline', 'candidate']) fs.rmSync(new URL(`${variant}-runtime.js`, root), { force: true });
}
