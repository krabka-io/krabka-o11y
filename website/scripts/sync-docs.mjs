import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const REPOSITORY = 'https://github.com/krabka-io/krabka-o11y';
const BASE = '/krabka-o11y/';
const DEFAULT_ROOT = fileURLToPath(new URL('../../', import.meta.url));

function walk(directory) {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))
    .flatMap(entry => entry.isDirectory() ? walk(path.join(directory, entry.name))
      : entry.isFile() ? [path.join(directory, entry.name)] : []);
}

// A longer fence may contain shorter fences; unfinished fences protect the rest of the file.
function outsideFences(content, transform) {
  let fence;
  let prose = '';
  let output = '';
  for (const line of content.match(/[^\n]*\n|[^\n]+$/g) ?? []) {
    if (fence) {
      output += line;
      const closing = line.match(/^[ \t]*(`+|~+)[ \t]*\r?\n?$/);
      if (closing && closing[1][0] === fence[0] && closing[1].length >= fence.length) fence = undefined;
    } else {
      const opening = line.match(/^[ \t]*(`{3,}|~{3,})/);
      if (opening) {
        output += transform(prose) + line;
        prose = '';
        fence = opening[1];
      } else prose += line;
    }
  }
  return output + transform(prose);
}

function encodedPath(source) {
  return source.split('/').map(encodeURIComponent).join('/');
}

function rewriteTarget(target, source, routes, repoRoot, image = false) {
  const angled = target.startsWith('<') && target.endsWith('>');
  const url = angled ? target.slice(1, -1) : target;
  if (/^(?:[a-z][a-z\d+.-]*:|\/|#|\?)/i.test(url)) return target;
  const suffixStart = url.search(/[?#]/);
  const filename = suffixStart < 0 ? url : url.slice(0, suffixStart);
  const suffix = suffixStart < 0 ? '' : url.slice(suffixStart);
  let decoded;
  try { decoded = decodeURIComponent(filename); } catch { return target; }
  const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(source), decoded));
  let destination = routes.get(resolved);
  if (!destination) {
    const directory = fs.existsSync(path.join(repoRoot, resolved)) && fs.statSync(path.join(repoRoot, resolved)).isDirectory();
    destination = image
      ? `https://raw.githubusercontent.com/krabka-io/krabka-o11y/main/${encodedPath(resolved)}`
      : `${REPOSITORY}/${directory ? 'tree' : 'blob'}/main/${encodedPath(resolved)}`;
  }
  return `${angled ? '<' : ''}${destination}${suffix}${angled ? '>' : ''}`;
}

// ponytail: ordinary repository links; use a Markdown parser if nested destinations are introduced.
const PROSE_TOKENS = /(?<code>(?<ticks>`+)[\s\S]*?\k<ticks>(?!`))|(?<linkStart>!?\[[^\]\n]*\]\()(?<destination><[^>\n]+>|[^\s)]+)(?<linkEnd>(?:[ \t]+(?:"[^"\n]*"|'[^'\n]*'))?\))|(?<htmlStart>\b(?:href|src)\s*=\s*)(?<quote>["'])(?<htmlTarget>.*?)\k<quote>|(?<referenceStart>^[ \t]{0,3}\[[^\]\n]+\]:[ \t]*)(?<referenceTarget><[^>\n]+>|[^\s]+)/gm;

export function rewriteDocLinks(content, { source, routes, repoRoot }) {
  return outsideFences(content, prose => prose.replace(PROSE_TOKENS, (...args) => {
    const token = args.at(-1);
    if (token.code) return args[0];
    if (token.linkStart) return token.linkStart
      + rewriteTarget(token.destination, source, routes, repoRoot, token.linkStart.startsWith('!')) + token.linkEnd;
    if (token.htmlStart) return token.htmlStart + token.quote
      + rewriteTarget(token.htmlTarget, source, routes, repoRoot, /^src/i.test(token.htmlStart)) + token.quote;
    return token.referenceStart + rewriteTarget(token.referenceTarget, source, routes, repoRoot);
  }));
}

function plainText(content) {
  return content.replace(/!?\[([^\]]+)\]\([^)]+\)/g, '$1').replace(/[`*_~]/g, '')
    .replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

export function documentMetadata(content, source) {
  let title;
  const body = outsideFences(content, prose => title ? prose : prose.replace(/^#[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$/m, (_heading, text) => {
    title = plainText(text);
    return '';
  }));
  let description;
  outsideFences(body, prose => {
    for (const paragraph of prose.split(/\n\s*\n/)) {
      if (!description && paragraph.trim() && !/^(?:#|[-*+]\s|\d+[.)]\s|>|[|<])/.test(paragraph.trim())) {
        description = plainText(paragraph);
      }
    }
    return prose;
  });
  title ??= path.posix.basename(source, '.md').replaceAll('_', ' ');
  return { title, description: description || title, body, editUrl: `${REPOSITORY}/edit/main/${encodedPath(source)}` };
}

export function syncDocs(repoRoot = DEFAULT_ROOT) {
  const sources = [];
  const add = (source, route) => {
    if (fs.existsSync(path.join(repoRoot, source))) sources.push({ source, route });
  };
  for (const filename of walk(path.join(repoRoot, 'docs'))) {
    if (filename.endsWith('.md')) {
      const source = path.relative(repoRoot, filename).split(path.sep).join('/');
      add(source, source.slice(0, -3).replace(/\/README$/, '/readme'));
    }
  }
  for (const [source, route] of Object.entries({
    'README.md': 'project', 'CONTRIBUTING.md': 'contributing', 'KNOWN_ISSUES.md': 'known_issues',
    'SECURITY.md': 'security', 'CODE_OF_CONDUCT.md': 'code_of_conduct',
    'deploy/README.md': 'deployment', 'benches/README.md': 'benchmarks', 'fuzz/README.md': 'fuzzing',
  })) add(source, `docs/${route}`);
  const crates = path.join(repoRoot, 'crates');
  for (const entry of fs.existsSync(crates) ? fs.readdirSync(crates, { withFileTypes: true }) : []) {
    if (entry.isDirectory()) for (const filename of ['README.md', 'test_coverage_report.md', 'CHANGELOG.md']) {
      add(`crates/${entry.name}/${filename}`, `docs/crates/${entry.name}/${filename.slice(0, -3).toLowerCase()}`);
    }
  }
  for (const filename of walk(path.join(repoRoot, 'website/guides'))) {
    if (filename.endsWith('.md')) {
      const source = path.relative(repoRoot, filename).split(path.sep).join('/');
      add(source, `docs/${source.slice('website/guides/'.length, -3)}`);
    }
  }
  const routes = new Map();
  const seenRoutes = new Set();
  for (const { source, route } of sources) {
    if (seenRoutes.has(route)) throw new Error(`Duplicate documentation route: ${route}`);
    seenRoutes.add(route);
    routes.set(source, `${BASE}${route}/`);
  }
  const inventories = walk(path.join(repoRoot, 'docs/api')).filter(filename => filename.endsWith('.json'));
  for (const filename of inventories) {
    routes.set(path.relative(repoRoot, filename).split(path.sep).join('/'), `${BASE}reference/${path.basename(filename)}`);
  }
  const contentDir = path.join(repoRoot, 'website/content');
  const referenceDir = path.join(repoRoot, 'website/generated-static/reference');
  fs.rmSync(contentDir, { recursive: true, force: true });
  fs.rmSync(referenceDir, { recursive: true, force: true });
  for (const { source, route } of sources.sort((a, b) => a.route.localeCompare(b.route))) {
    const original = fs.readFileSync(path.join(repoRoot, source), 'utf8').replace(/^\uFEFF/, '').replaceAll('\r\n', '\n');
    const { body, ...metadata } = documentMetadata(original, source);
    const frontmatter = Object.entries(metadata).map(([key, value]) => `${key}: ${JSON.stringify(value)}`).join('\n');
    const output = path.join(contentDir, `${route}.md`);
    fs.mkdirSync(path.dirname(output), { recursive: true });
    fs.writeFileSync(output, `---\n${frontmatter}\n---\n${rewriteDocLinks(body, { source, routes, repoRoot })}`);
  }
  fs.mkdirSync(referenceDir, { recursive: true });
  for (const filename of inventories) fs.copyFileSync(filename, path.join(referenceDir, path.basename(filename)));
  return { documents: sources.length, inventories: inventories.length };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = syncDocs();
  console.log(`Imported ${result.documents} documents and ${result.inventories} API inventories.`);
}
