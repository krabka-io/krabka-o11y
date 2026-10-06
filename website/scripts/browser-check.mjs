import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const BASE = '/krabka-o11y/';
const { values } = parseArgs({ options: {
  url: { type: 'string', default: 'http://127.0.0.1:4321/krabka-o11y/' },
  output: { type: 'string', default: fileURLToPath(new URL('../.tools/screenshots/', import.meta.url)) },
} });
const home = new URL(values.url);
assert.equal(home.pathname, BASE, `--url must point to ${BASE}`);
const output = path.resolve(values.output);
fs.mkdirSync(output, { recursive: true });
const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || process.env.CHROMIUM_PATH;
const browser = await chromium.launch({ executablePath, headless: true, args: ['--no-sandbox'] });
const runtimeErrors = [];

async function noOverflow(page, label) {
  const widths = await page.evaluate(() => ({
    viewport: document.documentElement.clientWidth,
    document: document.documentElement.scrollWidth,
    body: document.body.scrollWidth,
  }));
  assert.ok(widths.document <= widths.viewport + 1 && widths.body <= widths.viewport + 1,
    `${label}: horizontal overflow ${JSON.stringify(widths)}`);
}

async function screenshot(page, filename) {
  await page.evaluate(() => document.fonts.ready);
  // Chromium can paint offscreen fixed elements into a full-page capture when scrolled.
  await page.evaluate(() => scrollTo({ top: 0, left: 0, behavior: 'instant' }));
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await page.screenshot({ path: path.join(output, filename), fullPage: true });
}

async function copyCode(page, selector, expectedAttribute) {
  const button = page.locator(selector).first();
  if (!await button.count()) return false;
  const expected = (await button.getAttribute(expectedAttribute)).replaceAll('\u007f', '\n');
  await button.click();
  await page.waitForFunction(expected => navigator.clipboard.readText().then(text => text === expected), expected);
  assert.equal(await page.evaluate(() => navigator.clipboard.readText()), expected, 'Copy button must copy the displayed commands');
  return true;
}

async function canceledStartup(phase) {
  const context = await browser.newContext();
  const page = await context.newPage();
  const lab = new URL('lab/', home);
  const helper = new URL('coi.js', lab).href;
  const release = Promise.withResolvers();
  const navigation = Promise.withResolvers();
  const downloads = [];
  let navigations = 0;
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => runtimeErrors.push(`${phase}: ${error.stack || error.message}`));
  await page.addInitScript(pauseRegistration => {
    const state = globalThis.__cancelStartup = { completed: false, registrations: 0 };
    // Observe the actual Start handler's returned task; no synthetic helper or runtime.
    const add = EventTarget.prototype.addEventListener;
    EventTarget.prototype.addEventListener = function (type, listener, options) {
      if (this.id === 'lab-start' && type === 'click' && typeof listener === 'function') {
        const actual = listener;
        listener = function (...args) {
          const result = actual.apply(this, args);
          Promise.resolve(result).then(() => { state.completed = true; }, error => { state.error = error.message; state.completed = true; });
          return result;
        };
      }
      return add.call(this, type, listener, options);
    };
    const register = navigator.serviceWorker.register.bind(navigator.serviceWorker);
    navigator.serviceWorker.register = async (...args) => {
      state.registrations++;
      const registration = await register(...args);
      if (pauseRegistration) {
        const paused = new Promise(resolve => { state.release = resolve; });
        state.registered = true;
        await paused;
      }
      return registration;
    };
  }, phase === 'registration');
  await context.route(url => url.pathname.startsWith(`${BASE}lab/`) &&
    (url.pathname.endsWith('.wasm') || url.pathname.endsWith('/manifest.json') || url.pathname.startsWith(`${BASE}lab/sdk/`)), route => {
    downloads.push(new URL(route.request().url()).pathname);
    return route.abort();
  });
  if (phase === 'import') await page.route(helper, async route => { await release.promise; await route.continue(); });
  try {
    assert.ok((await page.goto(lab.href, { waitUntil: 'domcontentloaded' }))?.ok());
    assert.equal(await page.evaluate(() => globalThis.crossOriginIsolated), false, 'Cancellation checks require a fresh ordinary preview context');
    page.on('framenavigated', frame => {
      if (frame === page.mainFrame()) { navigations++; navigation.resolve(); }
    });
    const requested = phase === 'import' ? page.waitForRequest(helper) : null;
    await page.locator('#lab-start').click();
    if (requested) await requested;
    else await page.waitForFunction(() => globalThis.__cancelStartup.registered);
    await page.locator('#lab-stop').click();
    await page.waitForFunction(() => !document.getElementById('lab-start').disabled);
    assert.equal(await page.locator('#lab-status').textContent(), 'Stopped');
    if (phase === 'import') release.resolve();
    else await page.evaluate(() => globalThis.__cancelStartup.release());
    await Promise.race([
      page.waitForFunction(() => globalThis.__cancelStartup.completed),
      navigation.promise.then(() => { throw new Error(`Stop during ${phase} must not reload or restart the page`); }),
    ]);
    const state = await page.evaluate(() => ({
      ...globalThis.__cancelStartup,
      resume: sessionStorage.getItem('krabka-o11y-start-after-isolation'),
    }));
    assert.equal(navigations, 0, `${phase}: canceled startup must not navigate`);
    assert.equal(state.resume, null, `${phase}: canceled startup must not restore its resume flag`);
    assert.equal(state.registrations, phase === 'registration' ? 1 : 0, `${phase}: only the deliberately paused real registration may run`);
    assert.equal(state.error, undefined, `${phase}: canceled Start handler must settle cleanly`);
    assert.deepEqual(downloads, [], `${phase}: canceled startup must not request production modules or their runtime`);
    assert.equal(await page.locator('#lab-status').textContent(), 'Stopped');
    assert.ok(await page.locator('#lab-start').isEnabled() && await page.locator('#lab-stop').isDisabled());
    assert.ok(await page.locator('#lab-error').isHidden());
    console.log(`Stop during ${phase}: real pending operation settled without navigation, resume flag, or WASI downloads.`);
  } finally {
    release.resolve();
    await context.close();
  }
}

try {
  for (const [name, viewport] of [['desktop', { width: 1440, height: 1000 }], ['mobile', { width: 390, height: 844 }]]) {
    const context = await browser.newContext({ viewport, deviceScaleFactor: 1, colorScheme: 'dark', permissions: ['clipboard-read', 'clipboard-write'] });
    const page = await context.newPage();
    const labDownloads = [];
    page.setDefaultTimeout(15000);
    page.on('pageerror', error => runtimeErrors.push(`${name} ${page.url()}: ${error.stack || error.message}`));
    page.on('console', message => { if (message.type() === 'error') runtimeErrors.push(`${name} ${page.url()}: ${message.text()}`); });
    page.on('request', request => {
      const pathname = new URL(request.url()).pathname;
      if (pathname.startsWith(`${BASE}lab/`) && (pathname.endsWith('.wasm') || pathname.endsWith('/manifest.json') || pathname.startsWith(`${BASE}lab/sdk/`))) labDownloads.push(pathname);
    });
    try {
      const homeResponse = await page.goto(home.href, { waitUntil: 'domcontentloaded', timeout: 30000 });
      assert.ok(homeResponse?.ok(), `Homepage HTTP ${homeResponse?.status()}`);
      await screenshot(page, `home-${name}.png`);
      await noOverflow(page, `Homepage ${name}`);
      const navigation = page.getByRole('navigation', { name: 'Main navigation', exact: true });
      const documentation = navigation.getByRole('link', { name: 'Documentation', exact: true });
      const lab = navigation.getByRole('link', { name: /^Lab\b/ });
      assert.ok(await documentation.isVisible() && await lab.isVisible(), `${name}: primary navigation must remain visible`);
      assert.equal(await documentation.getAttribute('href'), `${BASE}docs/overview/`);
      assert.equal(await lab.getAttribute('href'), `${BASE}lab/`);
      await documentation.focus();
      await page.keyboard.press('Tab');
      assert.equal(await page.locator(':focus').getAttribute('href'), `${BASE}lab/`, `${name}: navigation must support keyboard focus`);
      const homeCopy = await copyCode(page, '.copy-command', 'data-command');

      const docsResponse = await page.goto(new URL('docs/getting_started/', home).href, { waitUntil: 'domcontentloaded', timeout: 30000 });
      assert.ok(docsResponse?.ok(), `Documentation HTTP ${docsResponse?.status()}`);
      await screenshot(page, `docs-${name}.png`);
      await noOverflow(page, `Documentation ${name}`);
      assert.ok(await page.getByRole('heading', { name: 'Getting Started', level: 1, exact: true }).isVisible());
      for (const signal of ['Metrics', 'Logs', 'Traces', 'Profiles']) {
        assert.equal(await page.getByRole('heading', { name: signal, level: 2, exact: true }).count(), 1, `${signal} quickstart must be present`);
      }
      const content = await page.locator('.sl-markdown-content').innerText();
      assert.ok(content.includes('bazel run //bazel/images/krabka:load') && content.includes('X-Scope-OrgID'), 'Documentation must contain actual deployment and tenancy instructions');
      const menu = page.getByRole('button', { name: 'Menu', exact: true });
      if (name === 'mobile') {
        assert.ok(await menu.isVisible(), 'Mobile docs menu must have an accessible button');
        await menu.focus();
        await page.keyboard.press('Enter');
        await page.locator('#starlight__sidebar:popover-open').waitFor({ state: 'visible' });
      }
      const sidebar = page.getByRole('navigation', { name: 'Main', exact: true });
      assert.equal(await sidebar.getByRole('link', { name: 'Overview', exact: true }).getAttribute('href'), `${BASE}docs/overview/`);
      const docsLab = sidebar.getByRole('link', { name: 'Observability lab', exact: true });
      assert.ok(await docsLab.isVisible(), 'Documentation lab navigation must be accessible');
      assert.equal(await docsLab.getAttribute('href'), `${BASE}lab/`);
      const theme = page.getByRole('combobox', { name: /theme/i }).first();
      const darkBackground = await page.evaluate(() => getComputedStyle(document.documentElement).getPropertyValue('--sl-color-bg'));
      await theme.selectOption('light');
      await page.locator('html[data-theme="light"]').waitFor();
      assert.notEqual(await page.evaluate(() => getComputedStyle(document.documentElement).getPropertyValue('--sl-color-bg')), darkBackground, 'Theme switch must change the rendered palette');
      await theme.selectOption('dark');
      await page.locator('html[data-theme="dark"]').waitFor();
      if (name === 'mobile') {
        await page.keyboard.press('Escape');
        await page.locator('#starlight__sidebar:popover-open').waitFor({ state: 'hidden' });
      }
      const docsCopy = await copyCode(page, 'button[title="Copy to clipboard"]', 'data-code');
      await page.evaluate(() => scrollTo(0, 0));
      const searchButton = page.getByRole('button', { name: 'Search', exact: true });
      await searchButton.click();
      const dialog = page.getByRole('dialog', { name: 'Search', exact: true });
      await dialog.waitFor({ state: 'visible' });
      await dialog.getByRole('textbox', { name: /Search/i }).fill('disaster recovery');
      const result = dialog.getByRole('link', { name: /Disaster Recovery/i }).first();
      await result.waitFor({ state: 'visible' });
      assert.equal(new URL(await result.getAttribute('href'), home).pathname, `${BASE}docs/disaster_recovery/`, 'Search must return a real documentation result');
      await page.keyboard.press('Escape');
      await dialog.waitFor({ state: 'hidden' });
      assert.ok(await searchButton.evaluate(element => document.activeElement === element), 'Escape must return focus to Search');

      const labResponse = await page.goto(new URL('lab/', home).href, { waitUntil: 'domcontentloaded', timeout: 30000 });
      assert.ok(labResponse?.ok(), `Lab HTTP ${labResponse?.status()}`);
      assert.ok(await page.getByRole('heading', { name: /Run the stack\./, level: 1 }).isVisible());
      const start = page.getByRole('button', { name: 'Start the stack', exact: true });
      assert.ok(await start.isVisible() && await start.isEnabled(), 'Idle lab must offer Start');
      for (const label of ['Send sample telemetry', 'Run query →', 'Stop']) {
        const control = page.getByRole('button', { name: label, exact: true });
        assert.ok(await control.isVisible() && await control.isDisabled(), `Idle lab must disable ${label}`);
      }
      assert.equal(await page.getByRole('status').textContent(), 'Stopped');
      assert.deepEqual(await page.locator('[data-service] small').allTextContents(), Array(5).fill('Stopped'));
      const tabs = page.getByRole('tablist', { name: 'Signal', exact: true });
      assert.deepEqual(await tabs.getByRole('tab').allTextContents(), ['Metrics', 'Logs', 'Traces', 'Profiles']);
      assert.equal(await tabs.getByRole('tab', { name: 'Metrics', exact: true }).getAttribute('aria-selected'), 'true');
      await tabs.getByRole('tab', { name: 'Metrics', exact: true }).focus();
      for (const [key, selectedName, queryLabel] of [
        ['ArrowRight', 'Logs', 'LogQL query'],
        ['ArrowRight', 'Traces', 'TraceQL query'],
        ['ArrowLeft', 'Logs', 'LogQL query'],
        ['Home', 'Metrics', 'PromQL query'],
        ['ArrowLeft', 'Profiles', 'Profile selector'],
        ['ArrowRight', 'Metrics', 'PromQL query'],
        ['End', 'Profiles', 'Profile selector'],
        ['Home', 'Metrics', 'PromQL query'],
      ]) {
        await page.keyboard.press(key);
        const selected = tabs.getByRole('tab', { name: selectedName, exact: true });
        assert.equal(await selected.getAttribute('aria-selected'), 'true', `${key} must select ${selectedName}`);
        assert.equal(await tabs.getByRole('tab', { selected: true }).count(), 1, 'Exactly one signal tab must be selected');
        assert.ok(await selected.evaluate(element => document.activeElement === element), `${key} must focus ${selectedName}`);
        assert.ok(await page.getByRole('tabpanel', { name: selectedName, exact: true }).isVisible(), 'Query panel must be associated with the selected signal');
        assert.ok(await page.getByRole('textbox', { name: queryLabel, exact: true }).isVisible(), `${selectedName} must expose its query input`);
      }
      await screenshot(page, `lab-${name}.png`);
      await noOverflow(page, `Lab ${name}`);
      assert.deepEqual(labDownloads, [], 'Idle lab and keyboard navigation must not download production modules or their runtime');
      assert.equal(runtimeErrors.length, 0, `Browser runtime errors:\n${runtimeErrors.slice(0, 20).join('\n')}`);
      console.log(`${name} ${viewport.width}x${viewport.height}: homepage/docs/lab overflow, base navigation, keyboard menu and signal tabs, idle lab controls, real content, theme, search/Escape, clipboard (${homeCopy ? 'homepage' : 'homepage absent'}, ${docsCopy ? 'docs' : 'docs absent'}) passed.`);
    } finally { await context.close(); }
  }
  const cancellationFailures = [];
  for (const phase of ['import', 'registration']) {
    try { await canceledStartup(phase); } catch (error) { cancellationFailures.push(`${phase}: ${error.message}`); }
  }
  assert.deepEqual(cancellationFailures, [], `Canceled startup regressions:\n${cancellationFailures.join('\n')}`);
  assert.equal(runtimeErrors.length, 0, `Browser runtime errors:\n${runtimeErrors.slice(0, 20).join('\n')}`);
  console.log(`Saved six screenshots to ${output}. Lab production modules and runtime were not downloaded.`);
} catch (error) {
  if (runtimeErrors.length) console.error(`Browser runtime errors:\n${runtimeErrors.slice(0, 20).join('\n')}`);
  throw error;
} finally { await browser.close(); }
