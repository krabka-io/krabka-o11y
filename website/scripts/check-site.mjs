import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const BASE = '/krabka-o11y/';
const ORIGIN = 'https://site.invalid';
const DEFAULT_DIST = fileURLToPath(new URL('../dist/', import.meta.url));
const rustdocRoute = route => /^(?:api(?:\/|$)|krabka_[a-z\d_]+(?:\/|$))/.test(route);

function htmlFiles(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))
    .flatMap(entry => entry.isDirectory() ? htmlFiles(path.join(directory, entry.name))
      : entry.isFile() && entry.name.endsWith('.html') ? [path.join(directory, entry.name)] : []);
}

function entities(value) {
  const named = { amp: '&', quot: '"', apos: "'", lt: '<', gt: '>' };
  return value.replace(/&(#x[\da-f]+|#\d+|amp|quot|apos|lt|gt);/gi, (match, entity) => {
    if (entity[0] !== '#') return named[entity.toLowerCase()];
    const number = entity[1].toLowerCase() === 'x' ? parseInt(entity.slice(2), 16) : Number(entity.slice(1));
    return number <= 0x10ffff ? String.fromCodePoint(number) : match;
  });
}

// ponytail: attributes in trusted generated HTML; use a parser for arbitrary HTML input.
function parsePage(filename) {
  const content = fs.readFileSync(filename, 'utf8');
  const ids = new Set();
  const links = [];
  let description = '';
  let rustdoc = false;
  const markup = content.replace(/<!--[\s\S]*?-->/g, '').replace(/(<script\b[^>]*>)[\s\S]*?<\/script\s*>/gi, '$1</script>');
  for (const tag of markup.matchAll(/<([a-z][\w:-]*)\b((?:"[^"]*"|'[^']*'|[^'">])*)>/gi)) {
    const attributes = Object.fromEntries([...tag[2].matchAll(/([a-z][\w:-]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/gi)]
      .map(attribute => [attribute[1].toLowerCase(), entities(attribute[2] ?? attribute[3] ?? attribute[4])]));
    if (attributes.id) ids.add(attributes.id);
    if (tag[1].toLowerCase() === 'a' && attributes.name) ids.add(attributes.name);
    if (tag[1].toLowerCase() === 'meta' && attributes.name?.toLowerCase() === 'description') description = attributes.content ?? '';
    if (tag[1].toLowerCase() === 'meta' && attributes.name === 'generator' && attributes.content === 'rustdoc') rustdoc = true;
    for (const key of ['href', 'src']) if (attributes[key]) links.push(attributes[key]);
  }
  const title = entities(content.match(/<title\b[^>]*>([\s\S]*?)<\/title\s*>/i)?.[1] ?? '').replace(/<[^>]*>/g, '').trim();
  return { title, description: description.trim(), rustdoc, ids, links };
}

function withinRoot(filename, root) {
  const relative = path.relative(root, filename);
  return relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

export function checkSite(dist = DEFAULT_DIST, { rustdoc = false } = {}) {
  const result = { pages: 0, links: 0, skippedRustdocLinks: 0, failures: [] };
  if (!fs.existsSync(dist)) {
    result.failures.push({ source: '<build>', target: '', reason: `Build directory does not exist: ${dist}` });
    return result;
  }
  const root = fs.realpathSync(dist);
  const parsed = new Map();
  const page = filename => {
    if (!parsed.has(filename)) parsed.set(filename, parsePage(filename));
    return parsed.get(filename);
  };
  for (const filename of htmlFiles(root)) {
    const source = path.relative(root, filename).split(path.sep).join('/');
    // Audit site-authored pages and their Rust API destinations. Rustdoc owns
    // the inherited dependency documentation inside its unmodified tree.
    if (/^(?:krabka_[a-z\d_]+|src)\//.test(source)) continue;
    result.pages++;
    const current = page(filename);
    const fail = (target, reason) => result.failures.push({ source, target, reason });
    if (!current.title) fail('', 'Empty page title');
    if (!current.description && !current.rustdoc) fail('', 'Empty page description');
    const sourceUrl = ORIGIN + BASE + source.replace(/(?:^|\/)index\.html$/, match => match.startsWith('/') ? '/' : '');
    for (const target of new Set(current.links)) {
      if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(target)) continue;
      result.links++;
      let url;
      let pathname;
      let anchor;
      try {
        url = new URL(target, sourceUrl);
        pathname = decodeURIComponent(url.pathname);
        anchor = decodeURIComponent(url.hash.slice(1)).split(':~:text=')[0];
      } catch {
        fail(target, 'Invalid local URL');
        continue;
      }
      if (pathname === BASE.slice(0, -1)) pathname = BASE;
      if (!pathname.startsWith(BASE)) {
        fail(target, `Local URL leaves ${BASE}`);
        continue;
      }
      const route = pathname.slice(BASE.length);
      const resolved = path.resolve(root, route);
      if (!withinRoot(resolved, root)) {
        fail(target, 'Target escapes build directory');
        continue;
      }
      if (source.startsWith('docs/') && pathname.endsWith('.md')) {
        fail(target, 'Raw Markdown link in rendered documentation');
        continue;
      }
      if (!rustdoc && rustdocRoute(route)) {
        result.skippedRustdocLinks++;
        continue;
      }
      const candidates = [resolved, path.join(resolved, 'index.html'), `${resolved}.html`];
      const destination = candidates.find(candidate => fs.existsSync(candidate) && fs.statSync(candidate).isFile());
      if (!destination) {
        fail(target, 'Target does not exist');
        continue;
      }
      if (!withinRoot(fs.realpathSync(destination), root)) {
        fail(target, 'Target escapes build directory through a symlink');
        continue;
      }
      if (anchor && destination.endsWith('.html')) {
        const targetPage = page(destination);
        // Rustdoc's source viewer resolves #start-end through its own script.
        const range = /^([1-9]\d*)-([1-9]\d*)$/.exec(anchor);
        const sourceRange = targetPage.rustdoc && path.relative(root, destination).startsWith(`src${path.sep}`)
          && range && Number(range[1]) <= Number(range[2])
          && targetPage.ids.has(range[1]) && targetPage.ids.has(range[2]);
        if (!targetPage.ids.has(anchor) && !sourceRange) fail(target, `Missing anchor #${anchor}`);
      }
    }
  }
  if (!result.pages) result.failures.push({ source: '<build>', target: '', reason: 'No HTML pages found' });
  return result;
}

function selfTest() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-o11y-site-'));
  const root = path.join(fixture, 'dist');
  const write = (filename, content) => {
    fs.mkdirSync(path.dirname(path.join(root, filename)), { recursive: true });
    fs.writeFileSync(path.join(root, filename), content);
  };
  const head = '<title>Fixture</title><meta name="description" content="Useful fixture.">';
  try {
    write('index.html', head + '<a href="/krabka-o11y/docs/guide/?mode=1&amp;copy=1#ready">Guide</a>'
      + '<script src="/krabka-o11y/assets/app.js?v=2"></script><a href="/krabka-o11y/api/">API</a>'
      + '<a href="/krabka-o11y/krabka_promql/">Rustdoc</a><a href="https://example.com/missing">External</a>'
      + '<a href="mailto:team@example.com">Email</a><img src="data:image/png;base64,AA=="><a href="/krabka-o11y">Home</a>');
    write('docs/guide/index.html', head + '<h2 id="ready">Ready</h2><a href="#ready">Anchor</a><a href="../../?mode=1">Home</a>');
    write('assets/app.js', '// fixture');
    const valid = checkSite(root);
    assert.equal(valid.failures.length, 0);
    assert.equal(valid.skippedRustdocLinks, 2);
    assert.equal(checkSite(root, { rustdoc: true }).failures.length, 2);
    write('api/index.html', head);
    write('krabka_promql/index.html', head);
    assert.equal(checkSite(root, { rustdoc: true }).failures.length, 0);
    write('src/krabka_promql/lib.rs.html', '<title>Source</title><meta name=generator content=rustdoc><a id=1 href=#1>1</a><a id=2 href=#2>2</a><a href=#1-2>Range</a><span id="impl-Example<T>">Quoted angle brackets</span>');
    write('api/index.html', head + '<a href="../src/krabka_promql/lib.rs.html#1-2">Source range</a><a href="../src/krabka_promql/lib.rs.html#impl-Example%3CT%3E">Impl</a>');
    assert.equal(checkSite(root, { rustdoc: true }).failures.length, 0);
    write('src/krabka_promql/broken.rs.html', '<title>Source</title><meta name=generator content=rustdoc><a id=1 href=#1-2>Missing range end</a>');
    write('api/index.html', head + '<a href="../src/krabka_promql/broken.rs.html#1-2">Broken range</a>');
    assert.equal(checkSite(root, { rustdoc: true }).failures.length, 1);
    fs.unlinkSync(path.join(root, 'src/krabka_promql/broken.rs.html'));
    write('api/index.html', head);
    fs.writeFileSync(path.join(fixture, 'outside.js'), '// outside');
    fs.symlinkSync(path.join(fixture, 'outside.js'), path.join(root, 'escape.js'));
    write('docs/broken/index.html', '<title> </title><meta content=" " name="description">'
      + '<a href="../guide/?mode=2#absent">Anchor</a><a href="/krabka-o11y/missing/?keep=1#x">Missing</a>'
      + '<a href="../../../outside">Escape</a><a href="guide.md">Markdown</a><script src="/krabka-o11y/escape.js"></script>'
      + '<script src="/krabka-o11y/krabka_missing.js"></script>');
    const failures = checkSite(root).failures;
    assert.equal(failures.length, 8);
    assert.ok(failures.some(failure => failure.reason === 'Empty page title'));
    assert.ok(failures.some(failure => failure.reason === 'Empty page description'));
    assert.ok(failures.some(failure => failure.reason === 'Missing anchor #absent'));
    assert.ok(failures.some(failure => failure.target === '/krabka-o11y/missing/?keep=1#x' && failure.reason === 'Target does not exist'));
    assert.ok(failures.some(failure => failure.reason.startsWith('Local URL leaves')));
    assert.ok(failures.some(failure => failure.reason.startsWith('Raw Markdown')));
    assert.ok(failures.some(failure => failure.reason.includes('symlink')));
    assert.ok(failures.some(failure => failure.target === '/krabka-o11y/krabka_missing.js' && failure.reason === 'Target does not exist'));
    console.log('Site checker self-test passed.');
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.includes('--self-test')) selfTest();
  else {
    const rustdoc = args.includes('--rustdoc');
    const result = checkSite(args.find(argument => !argument.startsWith('--')) ?? DEFAULT_DIST, { rustdoc });
    console.log(`Checked ${result.pages} website HTML pages and ${result.links} local references.`);
    if (!rustdoc) console.log(`Limit: Rustdoc /api/ and /krabka_<crate>/ routes excluded (${result.skippedRustdocLinks} references); use --rustdoc on the combined artifact.`);
    if (result.failures.length) {
      const grouped = new Map();
      for (const failure of result.failures) {
        const key = `${failure.target}\0${failure.reason}`;
        if (!grouped.has(key)) grouped.set(key, { ...failure, count: 0 });
        grouped.get(key).count++;
      }
      console.error(`Site check failed: ${result.failures.length} failures (${grouped.size} distinct targets/reasons).`);
      for (const failure of [...grouped.values()].slice(0, 20)) {
        console.error(`${failure.source}: ${failure.target || '(metadata)'} — ${failure.reason}${failure.count > 1 ? ` (${failure.count} pages)` : ''}`);
      }
      if (grouped.size > 20) console.error(`${grouped.size - 20} further targets/reasons omitted.`);
      process.exitCode = 1;
    } else console.log('Site links, anchors, assets, and metadata passed.');
  }
}
