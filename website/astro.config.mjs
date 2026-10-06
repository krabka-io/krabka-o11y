import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { cpSync } from 'node:fs';

export default defineConfig({
  site: 'https://krabka.io',
  base: '/krabka-o11y',
  trailingSlash: 'always',
  compressHTML: false,
  publicDir: './static',
  integrations: [
    starlight({
      title: 'krabka-o11y',
      description: 'Grafana-compatible metrics, logs, traces, and profiles in Rust.',
      logo: { src: './src/assets/krabka.svg' },
      favicon: '/favicon.svg',
      customCss: ['./src/styles/docs.css'],
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/krabka-io/krabka-o11y' }],
      sidebar: [
        { label: 'Start here', items: [
          { label: 'Overview', slug: 'docs/overview' },
          { label: 'Get started locally', slug: 'docs/getting_started' },
          { label: 'Grafana datasources', slug: 'docs/grafana' },
          { label: 'Observability lab', link: '/lab/' },
        ] },
        { label: 'Evaluate the stack', items: [
          { label: 'Compatibility', slug: 'docs/api_compatibility' },
          { label: 'Architecture', slug: 'docs/architecture_design' },
          { label: 'Formal verification', slug: 'docs/verification' },
          { label: 'Measured performance', slug: 'docs/operating_envelope' },
          { label: 'Known issues', slug: 'docs/known_issues' },
        ] },
        { label: 'Deploy and operate', items: [
          { label: 'Deployment', slug: 'docs/deployment' },
          { label: 'Operations', slug: 'docs/operations' },
          { label: 'Object-store contract', slug: 'docs/object_store_contract' },
          { label: 'Disaster recovery', slug: 'docs/disaster_recovery' },
          { label: 'Prometheus migration', slug: 'docs/prometheus_tsdb_migration' },
          { label: 'Observe krabka clusters', slug: 'docs/observing_krabka_clusters' },
        ] },
        { label: 'Reference', items: [
          { label: 'Documentation directory', slug: 'docs/directory' },
          { label: 'HTTP routes and oracles', slug: 'docs/api/readme' },
          { label: 'Persisted formats', slug: 'docs/persisted_formats' },
          { label: 'Upgrade compatibility', slug: 'docs/compatibility_upgrade_process' },
          { label: 'Rust API reference', link: '/api/' },
          { label: 'Crates', items: [{ autogenerate: { directory: 'docs/crates' } }], collapsed: true },
          { label: 'Releases', items: [{ autogenerate: { directory: 'docs/releases' } }], collapsed: true },
          { label: 'Style guides', items: [{ autogenerate: { directory: 'docs/style_guides' } }], collapsed: true },
        ] },
        { label: 'krabka platform', link: 'https://krabka.io/' },
      ],
    }),
    { name: 'reference-downloads', hooks: {
      'astro:build:done': ({ dir }) => cpSync('./generated-static', dir, { recursive: true }),
    } },
  ],
});
