# HTTP Route Inventory

[`routes.json`](routes.json) lists every HTTP method and path registered by the four signal implementations.

The generator reads production router declarations and the local Pyroscope Connect service definitions.

Regenerate it with:

```bash
tools/route-inventory.py > docs/api/routes.json
```

CI runs `tools/route-inventory.py --check`, so a route change must update this file.
