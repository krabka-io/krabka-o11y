import { Lab, signals } from './runtime.js';

const element = id => document.getElementById(`lab-${id}`);
let lab;
let signal = 'metrics';
let busy;
const logs = [];
const resumeKey = 'krabka-o11y-start-after-isolation';

function controls() {
  element('start').disabled = Boolean(lab) || busy;
  element('stop').disabled = !lab;
  element('seed').disabled = !lab?.ready || busy;
  element('run').disabled = !lab?.ready || busy;
}

function error(message) {
  element('error').textContent = message;
  element('error').hidden = !message;
}

function log(role, text) {
  logs.push(`[${role}] ${text}`);
  // ponytail: retain the last 500 lines; download support can extend long investigations.
  if (logs.length > 500) logs.splice(0, logs.length - 500);
  element('logs-output').textContent = logs.join('\n');
}

function status(service, text) {
  const card = document.querySelector(`[data-service="${service}"]`);
  if (card) {
    card.querySelector('small').textContent = text;
    card.dataset.status = text === 'Ready' ? 'ready' : text.startsWith('Exited') ? 'failed' : 'starting';
  }
  element('status').textContent = service === 'download' ? text : `${service}: ${text}`;
  if (service === 'failed') { error(text); element('indicator').dataset.status = 'failed'; }
  controls();
}

async function task(work) {
  const operation = {};
  busy = operation;
  controls();
  error('');
  try { await work(); } catch (cause) {
    if (busy === operation) { error(cause.message); log('lab', cause.stack ?? cause.message); }
  } finally {
    if (busy === operation) { busy = undefined; controls(); }
  }
}

element('start').addEventListener('click', () => {
  if (lab || busy) return;
  const running = new Lab({
    log: (...args) => { if (lab === running) log(...args); },
    status: (...args) => { if (lab === running) status(...args); },
  });
  lab = running;
  return task(async () => {
    try {
      const base = import.meta.env.BASE_URL;
      const { ensureCrossOriginIsolation } = await import(/* @vite-ignore */ `${base}lab/coi.js`);
      if (lab !== running) return;
      if (!globalThis.crossOriginIsolated) sessionStorage.setItem(resumeKey, '1');
      const isolation = await ensureCrossOriginIsolation({ signal: running.abort.signal });
      if (lab !== running || isolation.reloading) return;
      sessionStorage.removeItem(resumeKey);
      if (!isolation.isolated) throw new Error(`The browser cannot run shared WASI memory: ${isolation.reason}.`);
      const manifest = await running.start();
      if (lab !== running || !running.ready) return;
      element('status').textContent = 'Stack ready';
      element('indicator').dataset.status = 'ready';
      element('runtime-note').textContent = `Running build ${manifest.source.slice(0, 12)}${manifest.sourceDirty ? ' with local changes' : ''}. Send telemetry to populate the stack. Closing or stopping clears this sandbox.`;
    } catch (cause) {
      sessionStorage.removeItem(resumeKey);
      try { await running.stop(); } finally {
        if (lab === running) {
          lab = undefined;
          element('status').textContent = 'Start failed';
          element('indicator').dataset.status = 'failed';
        }
      }
      throw cause;
    }
  });
});

element('stop').addEventListener('click', () => {
  const running = lab;
  if (!running) return;
  sessionStorage.removeItem(resumeKey);
  lab = undefined;
  document.querySelectorAll('[data-service]').forEach(card => { card.dataset.status = 'starting'; card.querySelector('small').textContent = 'Stopping'; });
  element('status').textContent = 'Stopping…';
  element('indicator').dataset.status = 'starting';
  return task(async () => {
    let finished = 'Stopped';
    try { await running.stop(); } catch (cause) { finished = 'Stop failed'; throw cause; } finally {
      const state = finished === 'Stopped' ? '' : 'failed';
      document.querySelectorAll('[data-service]').forEach(card => { card.dataset.status = state; card.querySelector('small').textContent = finished; });
      element('status').textContent = finished;
      element('indicator').dataset.status = state;
    }
  });
});

element('seed').addEventListener('click', () => {
  const running = lab;
  if (!running?.ready || busy) return;
  return task(async () => {
    await running.seed(element('tenant').value);
    if (lab !== running || !running.ready) return;
    element('status').textContent = 'Telemetry accepted. Builders are publishing blocks.';
    await runQuery(running);
  });
});

async function runQuery(running) {
  const requestedSignal = signal;
  const started = performance.now();
  const response = await running.query(requestedSignal, element('query').value, element('tenant').value);
  if (lab !== running || signal !== requestedSignal) return;
  element('response-meta').textContent = `HTTP ${response.status} · ${Math.round(performance.now() - started)} ms`;
  element('result').textContent = typeof response.data === 'string' ? response.data : JSON.stringify(response.data, null, 2);
  element('chart').replaceChildren();
  element('chart').hidden = true;
  if (response.status < 200 || response.status >= 300) throw new Error(`Query returned HTTP ${response.status}. The API response is shown.`);
  const results = response.data?.data?.result;
  if (signal === 'metrics' && Array.isArray(results) && results.length) {
    const values = results.map(series => ({ label: series.metric?.service ?? series.metric?.__name__ ?? 'result', value: Number(series.value?.[1]) })).filter(row => Number.isFinite(row.value));
    const max = Math.max(1, ...values.map(row => Math.abs(row.value)));
    for (const row of values) {
      const bar = document.createElement('div');
      bar.className = 'lab-bar';
      const label = document.createElement('span');
      label.textContent = row.label;
      const fill = document.createElement('span');
      fill.className = 'lab-bar-fill';
      fill.style.width = `${Math.abs(row.value) / max * 100}%`;
      const value = document.createElement('strong');
      value.textContent = String(row.value);
      bar.append(label, fill, value);
      element('chart').append(bar);
    }
    element('chart').hidden = !values.length;
  }
}

element('run').addEventListener('click', () => {
  const running = lab;
  if (running?.ready && !busy) return task(() => runQuery(running));
});
element('example').addEventListener('change', () => { element('query').value = signals[signal].examples[Number(element('example').value)][1]; });

function select(next) {
  signal = next;
  document.querySelectorAll('[role="tab"]').forEach(tab => { const selected = tab.dataset.signal === signal; tab.setAttribute('aria-selected', String(selected)); tab.tabIndex = selected ? 0 : -1; });
  element('query-body').setAttribute('aria-labelledby', `tab-${signal}`);
  element('query-label').textContent = signals[signal].label;
  element('endpoint').textContent = signals[signal].endpoint;
  element('example').replaceChildren(...signals[signal].examples.map(([label], index) => new Option(label, String(index))));
  element('query').value = signals[signal].examples[0][1];
  element('chart').hidden = true;
}

const tabs = [...document.querySelectorAll('[role="tab"]')];
for (const tab of tabs) {
  tab.addEventListener('click', () => select(tab.dataset.signal));
  tab.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const index = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (tabs.indexOf(tab) + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    select(tabs[index].dataset.signal);
    tabs[index].focus();
  });
}
window.addEventListener('pagehide', () => {
  const running = lab;
  lab = undefined;
  busy = undefined;
  void running?.stop().catch(cause => log('lab', cause.message));
});
if (sessionStorage.getItem(resumeKey) === '1') {
  sessionStorage.removeItem(resumeKey);
  element('start').click();
}
