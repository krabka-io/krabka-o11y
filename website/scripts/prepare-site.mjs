import { cpSync, existsSync, mkdirSync, readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { syncDocs } from './sync-docs.mjs';

syncDocs();
// Preserve the SDK's relative worker/module URLs. Bundling its entrypoint alone
// would leave the dynamically imported worker dependencies behind.
const root = new URL('../', import.meta.url);
const destination = new URL('static/lab/sdk/', root);
mkdirSync(destination, { recursive: true });
const rebuilt = new URL('.tools/wasmer-sdk/', root);
const marker = new URL('.krabka-runtime-build.json', rebuilt);
let sdk = new URL('node_modules/@wasmer/sdk/', root);
if (existsSync(marker)) {
  const record = JSON.parse(readFileSync(marker, 'utf8'));
  const hash = path => createHash('sha256').update(readFileSync(path)).digest('hex');
  if (record.revision !== '362e0db28fea57fb23af8d34cffefcb7333a883d' ||
      record.coreRevision !== '5bd2b7d3182e816bd0ea330acc88e4e4733e167f' ||
      record.networkPatch !== hash(new URL('wasi/wasmer-network.patch', root)) ||
      record.addressPatch !== hash(new URL('wasi/wasmer-address.patch', root)) ||
      record.udpTimeoutPatch !== hash(new URL('wasi/wasmer-udp-timeout.patch', root)) ||
      record.wasmSha256 !== hash(new URL('js/pkg/wasmer_sdk_js_bg.wasm', rebuilt))) {
    throw new Error('The browser runtime changed; rebuild it with npm run build:lab.');
  }
  sdk = new URL('js/', rebuilt);
}
for (const directory of ['dist', 'pkg']) {
  cpSync(fileURLToPath(new URL(directory, sdk)), fileURLToPath(new URL(directory, destination)), { recursive: true });
}
cpSync(fileURLToPath(new URL('node_modules/@wasmer/sdk/LICENSE', root)), fileURLToPath(new URL('LICENSE', destination)));
