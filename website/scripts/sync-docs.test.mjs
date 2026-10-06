import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { syncDocs } from './sync-docs.mjs';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'krabka-o11y-docs-'));
function write(filename, content) {
  fs.mkdirSync(path.dirname(path.join(root, filename)), { recursive: true });
  fs.writeFileSync(path.join(root, filename), content);
}
function read(route) { return fs.readFileSync(path.join(root, `website/content/${route}.md`), 'utf8'); }

try {
  const fenced = '````md\n[Unchanged](next.md#kept)\n```\n[Still unchanged](next.md)\n````';
  write('docs/getting_started.md', '# Getting **Started**\n\nA complete [guide](next.md) for evaluation.\n\n'
    + '[Next](next.md?mode=full#details)\n[Source](../crates/demo/src/lib.rs#L2)\n[Directory](../crates/demo/src)\n'
    + '[External](https://example.com/next.md#details)\n[Anchor](#details)\n'
    + '![Image](image.svg)\n<a href="next.md#details">HTML guide</a>\n<img src="image.svg">\n'
    + '[Reference]: next.md#details "Next guide"\n\n`[Inline](next.md)`\n\n' + fenced
    + '\n\n~~~md\n[Unchanged tilde](next.md)\n~~~\n\n## Keep this heading\n\nKeep all final content.\n\n# Keep later H1\n');
  write('docs/next.md', '# Next\n\nNext paragraph.\n');
  write('docs/style_guides/README.md', '# Style Guides\n\nStyle paragraph.\n');
  write('docs/releases/v1.0.0.md', '# Release\n\nRelease paragraph.\n');
  write('docs/api/routes.json', '{"routes":["/api/v1/query"]}\n');
  write('docs/api/README.md', '# API Inventory\n\n[Routes](routes.json?download=1#routes)\n');
  write('README.md', '# Project\n\nAll signals.\n\n[Start](docs/getting_started.md)\n');
  for (const filename of ['CONTRIBUTING.md', 'KNOWN_ISSUES.md', 'SECURITY.md', 'CODE_OF_CONDUCT.md',
    'deploy/README.md', 'benches/README.md', 'fuzz/README.md', 'crates/demo/README.md', 'crates/demo/test_coverage_report.md', 'crates/demo/CHANGELOG.md']) {
    write(filename, '# Source document\n\nComplete source text.\n');
  }
  write('crates/demo/src/lib.rs', 'pub fn sample() {}\n');
  write('website/guides/evaluation.md', '# Evaluation\n\nEvaluate locally.\n\n[Setup](../../docs/getting_started.md#metrics)\n');

  assert.deepEqual(syncDocs(root), { documents: 17, inventories: 1 });
  const output = read('docs/getting_started');
  assert.match(output, /^---\ntitle: "Getting Started"\ndescription: "A complete guide for evaluation\."\neditUrl: "https:\/\/github.com\/krabka-io\/krabka-o11y\/edit\/main\/docs\/getting_started.md"\n---/);
  assert.ok(!output.includes('# Getting **Started**'));
  assert.ok(output.includes('[Next](/krabka-o11y/docs/next/?mode=full#details)'));
  assert.ok(output.includes('[Source](https://github.com/krabka-io/krabka-o11y/blob/main/crates/demo/src/lib.rs#L2)'));
  assert.ok(output.includes('[Directory](https://github.com/krabka-io/krabka-o11y/tree/main/crates/demo/src)'));
  assert.ok(output.includes('[External](https://example.com/next.md#details)'));
  assert.ok(output.includes('[Anchor](#details)'));
  assert.ok(output.includes('![Image](https://raw.githubusercontent.com/krabka-io/krabka-o11y/main/docs/image.svg)'));
  assert.ok(output.includes('<a href="/krabka-o11y/docs/next/#details">'));
  assert.ok(output.includes('<img src="https://raw.githubusercontent.com/krabka-io/krabka-o11y/main/docs/image.svg">'));
  assert.ok(output.includes('[Reference]: /krabka-o11y/docs/next/#details "Next guide"'));
  assert.ok(output.includes('`[Inline](next.md)`'));
  assert.ok(output.includes(fenced));
  assert.ok(output.includes('~~~md\n[Unchanged tilde](next.md)\n~~~'));
  assert.ok(output.includes('## Keep this heading\n\nKeep all final content.\n\n# Keep later H1'));
  assert.ok(read('docs/project').includes('[Start](/krabka-o11y/docs/getting_started/)'));
  assert.ok(read('docs/evaluation').includes('[Setup](/krabka-o11y/docs/getting_started/#metrics)'));
  assert.ok(read('docs/api/readme').includes('[Routes](/krabka-o11y/reference/routes.json?download=1#routes)'));
  for (const route of ['style_guides/readme', 'releases/v1.0.0', 'deployment', 'benchmarks', 'fuzzing',
    'contributing', 'known_issues', 'security', 'code_of_conduct', 'crates/demo/readme', 'crates/demo/test_coverage_report', 'crates/demo/changelog']) {
    assert.ok(fs.existsSync(path.join(root, `website/content/docs/${route}.md`)), route);
  }
  assert.equal(fs.readFileSync(path.join(root, 'website/generated-static/reference/routes.json'), 'utf8'), '{"routes":["/api/v1/query"]}\n');
  write('website/content/docs/stale.md', '# Stale\n');
  assert.deepEqual(syncDocs(root), { documents: 17, inventories: 1 });
  assert.ok(!fs.existsSync(path.join(root, 'website/content/docs/stale.md')));
  console.log('Documentation import checks passed.');
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
