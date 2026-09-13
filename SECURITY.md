# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use the private “Report a vulnerability” form in the repository's GitHub Security tab.

Include the affected commit, the exposed listener or input, reproduction steps, impact, and any proposed mitigation.

The maintainers will acknowledge the report, assess the impact, and coordinate a fix and disclosure through the private advisory.

## Supported code

Security fixes target the current `main` branch until the project publishes supported release lines.

## Trust boundaries

Treat telemetry bodies, compressed payloads, protocol metadata, labels, queries, broker records, and object-store data as untrusted.

Do not include credentials, tenant data, or private traces in a report unless the maintainers request them through the advisory.
