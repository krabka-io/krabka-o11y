import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';

const BASE = '/krabka-o11y/';
const DEFAULT_DIST = fileURLToPath(new URL('../dist/', import.meta.url));
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm', '.gz': 'application/gzip', '.xml': 'application/xml; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.ico': 'image/x-icon',
  '.woff': 'font/woff', '.woff2': 'font/woff2', '.txt': 'text/plain; charset=utf-8',
};

function withinRoot(filename, root) {
  const relative = path.relative(root, filename);
  return relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

export async function startPreview({ directory = DEFAULT_DIST, port = 4322, isolate = false } = {}) {
  const root = await fs.promises.realpath(directory);
  const server = http.createServer(async (request, response) => {
    const headers = { 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' };
    if (isolate) {
      headers['Cross-Origin-Opener-Policy'] = 'same-origin';
      headers['Cross-Origin-Embedder-Policy'] = 'require-corp';
    }
    const send = (status, message, extra = {}) => {
      response.writeHead(status, { ...headers, 'Content-Type': 'text/plain; charset=utf-8', ...extra });
      response.end(request.method === 'HEAD' ? undefined : message);
    };
    if (!['GET', 'HEAD'].includes(request.method)) return send(405, 'Method not allowed', { Allow: 'GET, HEAD' });
    let pathname;
    try { pathname = decodeURIComponent((request.url ?? '/').split(/[?#]/, 1)[0]); }
    catch { return send(400, 'Invalid URL'); }
    if (pathname.includes('\0') || pathname.includes('\\') || pathname.split('/').includes('..')) return send(403, 'Path traversal rejected');
    if (pathname === BASE.slice(0, -1)) {
      const query = new URL(request.url, 'http://127.0.0.1').search;
      return send(302, 'Redirecting', { Location: BASE + query });
    }
    if (!pathname.startsWith(BASE)) return send(404, 'Not found');
    let filename = path.resolve(root, pathname.slice(BASE.length));
    if (!withinRoot(filename, root)) return send(403, 'Path traversal rejected');
    try {
      let stat = await fs.promises.stat(filename);
      if (stat.isDirectory()) {
        filename = path.join(filename, 'index.html');
        stat = await fs.promises.stat(filename);
      }
      if (!stat.isFile()) return send(404, 'Not found');
      if (!withinRoot(await fs.promises.realpath(filename), root)) return send(403, 'Path traversal rejected');
      const compressedWasm = filename.endsWith('.wasm.gz');
      response.writeHead(200, { ...headers,
        'Content-Type': compressedWasm ? 'application/wasm' : (MIME[path.extname(filename)] ?? 'application/octet-stream'),
        'Content-Length': stat.size,
        ...(compressedWasm ? { 'Content-Encoding': 'gzip' } : {}),
      });
      if (request.method === 'HEAD') response.end();
      else fs.createReadStream(filename).on('error', error => response.destroy(error)).pipe(response);
    } catch (error) {
      if (error.code === 'ENOENT' || error.code === 'ENOTDIR') send(404, 'Not found');
      else { console.error(error); send(500, 'Preview error'); }
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  return server;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: {
    dir: { type: 'string', default: DEFAULT_DIST }, port: { type: 'string', default: '4322' },
    isolate: { type: 'boolean', default: false },
  } });
  const port = Number(values.port);
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('--port must be an integer from 0 to 65535');
  const server = await startPreview({ directory: values.dir, port, isolate: values.isolate });
  console.log(`Preview: http://127.0.0.1:${server.address().port}${BASE} (isolation ${values.isolate ? 'enabled' : 'disabled'})`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => server.close(() => process.exit(0)));
}
