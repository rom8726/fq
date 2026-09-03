# fq Helm Chart

This chart installs fq on Kubernetes with a generated `config.yml`, a ClusterIP
Service, optional token Secret, and persistent storage for WAL and dump files.

## Install

```shell
helm install fq ./charts/fq
```

The default install is a single unauthenticated fq instance for trusted development
namespaces. It uses `wal_and_dump` persistence with zstd compression, an `8Gi` PVC, and
exposes:

- `1945` for the fq client protocol;
- `2112` for `/healthz`, `/metrics`, and `/v1/info`.

## Production Values

Enable client authentication and create the token Secret from values. By default,
tokens are mounted as Secret files and the generated fq config uses `token_file`:

```shell
helm upgrade --install fq ./charts/fq \
  --set config.network.auth.enabled=true \
  --set auth.secrets.adminToken="$FQ_ADMIN_TOKEN" \
  --set auth.secrets.rwToken="$FQ_RW_TOKEN" \
  --set auth.secrets.roToken="$FQ_RO_TOKEN"
```

Or reference an existing Secret with keys named `FQ_ADMIN_TOKEN`, `FQ_RW_TOKEN`,
`FQ_RO_TOKEN`, and, when replication is enabled, `FQ_REPLICATION_TOKEN`. The chart
mounts it at `/var/run/secrets/fq` by default:

```yaml
auth:
  existingSecret: fq-auth
config:
  network:
    auth:
      enabled: true
```

Set `auth.tokenSource=env` to use `token_env` instead:

```yaml
auth:
  tokenSource: env
```

Pin the runtime image by digest when deploying outside local testing:

```yaml
image:
  repository: ghcr.io/fq-db/fq
  tag: ""
  digest: sha256:...
```

## Replication

Replication is disabled by default. To run a master:

```yaml
config:
  replication:
    enabled: true
    replicaType: master
    masterAddress: ":1946"
auth:
  secrets:
    replicationToken: "replace-with-at-least-16-characters"
```

To run a slave, install a second release and point it at the master's replication
Service:

```yaml
config:
  replication:
    enabled: true
    replicaType: slave
    replicaId: "replica-1"
    masterAddress: "fq-master:1946"
auth:
  existingSecret: fq-replication
```

The master and slave must share the same `FQ_REPLICATION_TOKEN` Secret value.

## Monitoring

Set `serviceMonitor.enabled=true` when the Prometheus Operator CRDs are installed:

```yaml
serviceMonitor:
  enabled: true
```

Without ServiceMonitor, scrape the Service's `observability` port at `/metrics`.

## TLS and Extra Mounts

TLS certificate files are deployment-specific. Mount them with `extraVolumes` and
`extraVolumeMounts`, then set `config.network.tls` or `config.replication.tls` to the
paths inside the container.
