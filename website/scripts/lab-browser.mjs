import assert from 'node:assert/strict';
import { parseArgs } from 'node:util';
import { chromium } from 'playwright';

// The lab URL from `--url`, and a headless Chromium to drive it with. Shared by
// the runtime checks that run against a built lab.
export async function openLab() {
  const { values } = parseArgs({ options: {
    url: { type: 'string', default: 'http://127.0.0.1:4322/krabka-o11y/lab/' },
  } });
  const lab = new URL(values.url);
  assert.equal(lab.pathname, '/krabka-o11y/lab/', '--url must point to /krabka-o11y/lab/');
  const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || process.env.CHROMIUM_PATH;
  const browser = await chromium.launch({ executablePath, headless: true, args: ['--no-sandbox'] });
  return { lab, browser };
}
