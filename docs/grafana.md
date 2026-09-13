# Grafana Datasources

Provision one datasource per signal and send the same tenant header on each request.

The Compose deployment exposes query services on the host ports shown below.

| Signal | Grafana type | URL |
| --- | --- | --- |
| Metrics | `prometheus` | `http://host.docker.internal:9090` |
| Logs | `loki` | `http://host.docker.internal:3101` |
| Traces | `tempo` | `http://host.docker.internal:3201` |
| Profiles | `grafana-pyroscope-datasource` | `http://host.docker.internal:4042` |

Use service DNS names instead of `host.docker.internal` when Grafana runs in the same Compose or Kubernetes network.

Save this file as `provisioning/datasources/krabka.yaml` under Grafana's configuration directory:

```yaml
apiVersion: 1
datasources:
  - name: Krabka Metrics
    uid: krabka-metrics
    type: prometheus
    access: proxy
    url: http://host.docker.internal:9090
    jsonData:
      httpMethod: GET
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: quickstart
  - name: Krabka Logs
    uid: krabka-logs
    type: loki
    access: proxy
    url: http://host.docker.internal:3101
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: quickstart
  - name: Krabka Traces
    uid: krabka-traces
    type: tempo
    access: proxy
    url: http://host.docker.internal:3201
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: quickstart
  - name: Krabka Profiles
    uid: krabka-profiles
    type: grafana-pyroscope-datasource
    access: proxy
    url: http://host.docker.internal:4042
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: quickstart
```

Restart Grafana after adding the file.

The three Grafana differential suites and the profile differential suite provision these datasource types against Krabka in CI.
