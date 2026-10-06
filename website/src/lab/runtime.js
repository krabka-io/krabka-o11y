const base = import.meta.env?.BASE_URL ?? '/krabka-o11y/';
const broker = '127.0.0.1:9092';
const cluster = '61cbf2b0-9443-4bba-8f4f-71cba8f932d7';
const store = signal => `file:///workspace/store/${signal}`;
const admin = port => ['--admin-listen-addr', `127.0.0.1:${port}`];

// These are the production commands and roles, using the sandbox's TCP and filesystem.
const services = [
  ['metrics', 'metrics-builder', 'krabka-metrics', ['--target', 'block-builder', '--bootstrap', broker, '--object-store-url', store('metrics'), '--block-builder-flush-max-age', '1s', ...admin(9406)], 9406],
  ['metrics', 'metrics-compactor', 'krabka-metrics', ['--target', 'compactor', '--bootstrap', broker, '--object-store-url', store('metrics'), ...admin(9407)], 9407],
  ['metrics', 'metrics-distributor', 'krabka-metrics', ['--target', 'distributor', '--bootstrap', broker, '--listen', '127.0.0.1:4041', '--object-store-url', store('metrics'), ...admin(9408)], 4041],
  ['metrics', 'metrics-query', 'krabka-metrics-service', ['--target', 'query-frontend', '--listen', '127.0.0.1:9090', '--wal-bootstrap', broker, '--object-store-url', store('metrics'), '--cold-cache-ttl', '1s', ...admin(9409)], 9090],
  ['metrics', 'metrics-ruler', 'krabka-metrics-service', ['--target', 'ruler', '--listen', '127.0.0.1:9091', '--wal-bootstrap', broker, '--object-store-url', store('metrics'), ...admin(9410)], 9410],
  ['logs', 'logs', 'krabka-observability', ['--target', 'all', '--listen-addr', '127.0.0.1:3100', '--wal-bootstrap-server', broker, '--object-store-url', store('logs'), '--data-root', '/workspace/logs', '--querier-index-source', 'tenant-object-store-shards', '--index-prefix', 'logs', ...admin(9412)], 3100],
  ['traces', 'traces', 'krabka-traces', ['--target', 'all', '--listen', '127.0.0.1:3200', '--bootstrap', broker, '--object-store-url', store('traces'), '--block-builder-flush-max-age', '1s', ...admin(9413)], [3200, 4318]],
  ['profiles', 'profiles', 'krabka-profiles', ['--target', 'all', '--listen', '127.0.0.1:4040', '--bootstrap', broker, '--object-store-url', store('profiles'), '--block-builder-flush-max-age', '1s', ...admin(9414)], 4040],
];

export const signals = {
  metrics: { label: 'PromQL query', endpoint: 'GET /api/v1/query', examples: [['Requests by service', 'lab_requests_total'], ['Sum all requests', 'sum(lab_requests_total)']] },
  logs: { label: 'LogQL query', endpoint: 'GET /loki/api/v1/query_range', examples: [['Checkout logs', '{service="checkout"}'], ['Only errors', '{service="checkout"} |= "error"']] },
  traces: { label: 'TraceQL query', endpoint: 'GET /api/search', examples: [['Checkout traces', '{ resource.service.name = "checkout" }'], ['Slow requests', '{ duration > 100ms }']] },
  profiles: { label: 'Profile selector', endpoint: 'GET /pyroscope/render', examples: [['Checkout CPU profile', 'process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="checkout"}']] },
};

export class Lab {
  constructor({ log = () => {}, status = () => {} } = {}) {
    this.log = log;
    this.status = status;
    this.processes = [];
    this.abort = new AbortController();
    this.ready = false;
    this.stopping = false;
  }

  async start() {
    this.abort.signal.throwIfAborted();
    if (this.started) throw new Error('This lab has already been started. Create a new lab to restart it.');
    this.started = true;
    const { Wasmer } = typeof window === 'undefined'
      ? await import('@wasmer/sdk')
      : await import(/* @vite-ignore */ `${base}lab/sdk/dist/index.js`);
    this.abort.signal.throwIfAborted();
    this.status('download', 'Preparing the WASI runtime…');
    this.client = new Wasmer({ parallelism: 2, cache: { namespace: 'krabka-o11y-lab-v1' } });
    await this.wait(this.client.ready());
    const response = await fetch(`${base}lab/manifest.json`, { signal: this.abort.signal });
    if (!response.ok) throw new Error(`Could not download lab manifest (HTTP ${response.status}).`);
    this.manifest = await response.json();
    const modules = {};
    for (const module of this.manifest.modules) {
      this.abort.signal.throwIfAborted();
      this.status('download', `Downloading ${module.name} (${Math.ceil(module.bytes / 1_000_000)} MB)…`);
      const response = await fetch(`${base}lab/${module.file}`, { signal: this.abort.signal });
      if (!response.ok) throw new Error(`${module.name} download failed (HTTP ${response.status}).`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      const digest = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(value => value.toString(16).padStart(2, '0')).join('');
      if (digest !== module.sha256) throw new Error(`${module.name} did not match the build manifest.`);
      modules[module.name] = bytes;
    }
    this.abort.signal.throwIfAborted();
    this.status('download', 'Preparing the service commands…');
    const pkg = await this.wait(this.client.packages.create({ modules, commands: Object.fromEntries(Object.keys(modules).map(name => [name, { module: name }])) }));
    const sandbox = await this.client.sandboxes.create({ packages: [pkg], network: { mode: 'http', peers: [] }, env: { RUST_LOG: 'info', TMPDIR: '/workspace/tmp', SSL_CERT_FILE: '/workspace/certs/ca.pem', TOKIO_WORKER_THREADS: '2', RAYON_NUM_THREADS: '2' } });
    if (this.abort.signal.aborted) {
      await sandbox.close();
      this.abort.signal.throwIfAborted();
    }
    this.sandbox = sandbox;
    for (const directory of ['/workspace/tmp', '/workspace/certs', '/workspace/broker', '/workspace/logs', ...['metrics', 'logs', 'traces', 'profiles'].map(signal => `/workspace/store/${signal}`)]) {
      this.abort.signal.throwIfAborted();
      await this.wait(this.sandbox.fs.mkdir(directory, { recursive: true }));
    }
    // Supply the pinned Mozilla roots because WASIX has no host certificate store.
    const certificates = await fetch(`${base}lab/certs/ca.pem`, { signal: this.abort.signal });
    if (!certificates.ok) throw new Error(`Certificate bundle download failed (HTTP ${certificates.status}).`);
    const certificateBytes = new Uint8Array(await certificates.arrayBuffer());
    const certificateHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', certificateBytes))].map(value => value.toString(16).padStart(2, '0')).join('');
    if (certificateHash !== 'a8e00c3793f619a1b7a6cd50211ec0081836f83cef4b237e8cb52876e72da9c2') throw new Error('Certificate bundle did not match its pinned source.');
    await this.wait(this.sandbox.fs.writeFile('/workspace/certs/ca.pem', certificateBytes));
    this.status('broker', 'Formatting storage');
    await this.run('krabka-format', ['--log-dir', '/workspace/broker', '--cluster-id', cluster, '--node-id', '1', '--standalone', '--controller-listener', '127.0.0.1:9093']);
    await this.spawn('broker', 'broker', 'krabka-broker', ['--log-dir', '/workspace/broker', '--broker-id', '1', '--cluster-id', cluster, '--listen-addr', broker, '--controller-listen-addr', '127.0.0.1:9093', '--metrics-listen-addr', '127.0.0.1:9404', '--health-listen-addr', '127.0.0.1:9405', '--offsets-topic-replication-factor', '1', '--transaction-state-replication-factor', '1', '--transaction-state-min-isr', '1'], 9092);
    this.status('broker', 'Provisioning WAL topics');
    await this.run('krabka-o11y-bootstrap', ['--bootstrap', broker]);
    this.status('broker', 'Ready');
    for (const [signal, role, command, args, port] of services) await this.spawn(signal, role, command, args, port);
    this.abort.signal.throwIfAborted();
    for (const signal of Object.keys(signals)) this.status(signal, 'Ready');
    this.ready = true;
    return this.manifest;
  }

  async wait(promise) {
    const signal = this.abort.signal;
    let onAbort;
    const aborted = new Promise((_, reject) => {
      onAbort = () => reject(signal.reason);
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
    try {
      const result = await Promise.race([promise, aborted]);
      signal.throwIfAborted();
      return result;
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  async run(command, args = [], options = {}) {
    this.abort.signal.throwIfAborted();
    const output = await this.wait(this.sandbox.command(command, args, { cwd: '/workspace' }).run({ timeoutMs: 60_000, outputBytes: 8 * 1024 * 1024, ...options }));
    if (output.stderr.bytes.length) this.log(command, output.stderr.text());
    return output.text();
  }

  async spawn(signal, role, command, args, port) {
    this.abort.signal.throwIfAborted();
    this.status(signal, `Starting ${role}`);
    const process = await this.sandbox.command(command, args, { cwd: '/workspace' }).spawn({ stdin: 'closed', stdout: 'pipe', stderr: 'pipe', outputBytes: 64 * 1024 });
    if (this.abort.signal.aborted) {
      await process.kill();
      this.abort.signal.throwIfAborted();
    }
    this.processes.push(process);
    for (const stream of [process.stdout, process.stderr]) if (stream) {
      (async () => { for await (const line of stream.lines()) if (!this.stopping) this.log(role, line); })().catch(error => { if (!this.stopping) this.log(role, error.message); });
    }
    const exit = process.wait({ check: false }).then(output => {
      if (!this.stopping) {
        this.ready = false;
        this.status(signal, `Exited (${output.exitCode})`);
        const error = new Error(`${role} exited (${output.exitCode}). Check the service logs.`);
        this.abort.abort(error);
        throw error;
      }
    });
    // Keep watching after readiness. A service exit must never leave a green indicator.
    exit.catch(error => {
      if (!this.stopping) {
        this.ready = false;
        this.abort.abort(error);
        this.status('failed', error.message);
      }
    });
    for (const listener of [port].flat()) {
      await this.wait(Promise.race([this.sandbox.ports.wait(listener, { timeoutMs: 90_000 }), exit]));
    }
  }

  async request(port, path, { method = 'GET', tenant = 'lab', body, contentType } = {}) {
    const raw = await this.run('krabka-lab-http', [], { stdin: JSON.stringify({ method, url: `http://127.0.0.1:${port}${path}`, tenant, body, contentType }) });
    const response = JSON.parse(raw);
    let data;
    try { data = JSON.parse(response.body); } catch { data = response.body; }
    return { ...response, data };
  }

  async query(signal, query, tenant = 'lab') {
    if (!this.ready) throw new Error('Start the stack before querying it.');
    const end = Date.now();
    const start = (this.seedTime ?? end) - 5 * 60_000;
    const params = new URLSearchParams({ query });
    if (signal === 'metrics') return this.request(9090, `/api/v1/query?${params}`, { tenant });
    if (signal === 'logs') {
      params.set('start', String(BigInt(start) * 1_000_000n));
      params.set('end', String(BigInt(end + 1000) * 1_000_000n));
      params.set('limit', '100');
      return this.request(3100, `/loki/api/v1/query_range?${params}`, { tenant });
    }
    if (signal === 'traces') {
      const search = new URLSearchParams({ q: query, start: String(Math.floor(start / 1000)), end: String(Math.ceil(end / 1000)), limit: '20' });
      return this.request(3200, `/api/search?${search}`, { tenant });
    }
    params.set('from', String(start));
    params.set('until', String(end + 1000));
    return this.request(4040, `/pyroscope/render?${params}`, { tenant });
  }

  async seed(tenant = 'lab') {
    if (!this.ready) throw new Error('Start the stack before sending telemetry.');
    this.seedTime = Date.now();
    const now = this.seedTime;
    const requests = [
      [4041, '/api/v1/push/influx/write?precision=ms', 'text/plain', `lab_requests_total,service=checkout value=42 ${now}\nlab_requests_total,service=payments value=18 ${now}\n`],
      [3100, '/loki/api/v1/push', 'application/json', JSON.stringify({ streams: [{ stream: { service: 'checkout', environment: 'lab' }, values: [[String(BigInt(now) * 1_000_000n), 'checkout request completed'], [String(BigInt(now) * 1_000_000n + 1n), 'checkout error: payment retry']] }] })],
      [4318, '/api/v2/spans', 'application/json', JSON.stringify([{ traceId: '1a4b8c6d0e2f4a6789abcdef01234567', id: '01ab23cd45ef6789', name: 'POST /checkout', timestamp: now * 1000, duration: 180_000, localEndpoint: { serviceName: 'checkout' }, tags: { 'http.method': 'POST', 'http.status_code': '200' } }])],
      [4040, '/push.v1.PusherService/Push', 'application/json', JSON.stringify({ series: [{ labels: [{ name: '__name__', value: 'process_cpu' }, { name: 'service_name', value: 'checkout' }], samples: [{ rawProfile: await sampleProfile(now), ID: crypto.randomUUID() }] }] })],
    ];
    for (const [port, path, contentType, body] of requests) {
      const response = await this.request(port, path, { method: 'POST', tenant, contentType, body });
      this.log('sample', `${port}${path}: HTTP ${response.status}`);
      if (response.status < 200 || response.status >= 300) throw new Error(`Ingestion on ${port} failed (HTTP ${response.status}): ${response.body}`);
    }
    return now;
  }

  async stop() {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.ready = false;
    this.abort.abort();
    this.stopPromise = (async () => {
      try { await this.sandbox?.close(); } finally {
        try { await this.client?.close(); } finally { this.processes = []; }
      }
    })();
    return this.stopPromise;
  }
}

// Actual pprof protobuf with CPU samples, function names and a recorded time.
export async function sampleProfile(time) {
  const varint = input => {
    let value = BigInt(input);
    const bytes = [];
    do { bytes.push(Number(value & 127n) | (value > 127n ? 128 : 0)); value >>= 7n; } while (value);
    return bytes;
  };
  const number = (field, value) => [...varint(field * 8), ...varint(value)];
  const message = (field, bytes) => [...varint(field * 8 + 2), ...varint(bytes.length), ...bytes];
  const strings = ['', 'cpu', 'nanoseconds', 'checkout', 'validate_cart', 'charge_payment', 'checkout.rs'];
  const bytes = [
    ...message(1, [...number(1, 1), ...number(2, 2)]),
    ...message(2, [...message(1, [...varint(2), ...varint(1)]), ...message(2, varint(24_000_000))]),
    ...message(2, [...message(1, [...varint(3), ...varint(1)]), ...message(2, varint(48_000_000))]),
    ...[1, 2, 3].flatMap(id => message(4, [...number(1, id), ...message(4, [...number(1, id), ...number(2, 10 * id)])])),
    ...[1, 2, 3].flatMap(id => message(5, [...number(1, id), ...number(2, id + 2), ...number(3, id + 2), ...number(4, 6)])),
    ...strings.flatMap(text => message(6, [...new TextEncoder().encode(text)])),
    ...number(9, BigInt(time) * 1_000_000n), ...number(10, 1_000_000_000),
    ...message(11, [...number(1, 1), ...number(2, 2)]), ...number(12, 1_000_000),
  ];
  const gzip = new Blob([new Uint8Array(bytes)]).stream().pipeThrough(new CompressionStream('gzip'));
  return btoa(String.fromCharCode(...new Uint8Array(await new Response(gzip).arrayBuffer())));
}
