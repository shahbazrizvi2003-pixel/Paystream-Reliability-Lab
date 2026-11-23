# PayStream Reliability Lab

A production-style Apache Kafka streaming platform running a mock retail-banking
event workload — three KRaft brokers, Schema Registry, three Java 21 / Spring
Boot services, monitoring, and a documented failure-drill programme with
runbooks.

> **Positioning note.** Everything here is *production-style*: it follows
> production practices (security, HA, observability, runbooks) on laptop-class
> hardware with synthetic data. It is not certified and has not carried real
> production traffic. See [Known limitations](#known-limitations).

## Architecture

```
                       ┌──────────────────────────────────────────────┐
                       │          Kafka cluster (KRaft, RF=3)         │
 txn-producer ────────►│  bank.transactions.v1 (6p, keyed account_id) │
 (Spring Boot,         │            │                                 │
  1–5,000 msg/s)       │            ▼                                 │
                       │  fraud-detector (Kafka Streams)              │
                       │   amount rule → bank.fraud-alerts.v1         │
                       │            │                                 │
                       │            ▼                                 │
                       │  notifier (idempotent consumer)              │
                       │   → PostgreSQL (alerts + notifications)      │
                       │   → bank.notifications.v1                    │
                       └──────────────────────────────────────────────┘
                              │                │
                       kafka-exporter    /actuator/prometheus
                              ▼                ▼
                       Prometheus → Grafana (dashboards + alert rules)
```

| Component | Choice | Pinned version |
|---|---|---|
| Event streaming | Apache Kafka, KRaft combined mode (ADR-0001) | `apache/kafka:4.0.0` |
| Schema management | Confluent Schema Registry (community licence), Avro BACKWARD (ADR-0004) | `cp-schema-registry:7.9.0` |
| Services | Java 21 LTS, Spring Boot 3.5.7, Spring Kafka, Kafka Streams | — |
| Persistence | PostgreSQL 16 + Flyway migrations V1–V4 | `postgres:16-alpine` |
| Observability | kafka-exporter → Prometheus → Grafana | `v1.10.0` / `v3.4.1` / `12.0.0` |

## Quickstart

Prerequisites: Docker + Compose v2, JDK 21, Maven 3.9+ (build only).

```bash
cp .env.example .env      # pinned versions and non-secret defaults
make up                   # images, stack, topics, ready
make smoke                # T2: verifies producer → alert → notification row
```

Then:

- **Grafana** — http://localhost:3000 (admin/admin) → *PayStream* folder →
  *Cluster Overview (MVP)*
- **Prometheus** — http://localhost:9090
- **Producer API** — `GET http://localhost:8080/api/generation/status`
  (start/stop via `POST /api/generation/start?rate=N`, 1–5,000 msg/s)

Teardown: `make down` (keeps volumes) or `make nuke` (full cold-start
rehearsal). The smoke test plus all operational scripts are idempotent and
support `--help`.

## Repository layout

One responsibility per directory (Document 04):

| Directory | Purpose |
|---|---|
| `kafka-config/` | Topics, ACLs, Avro schemas, Debezium connector as code + validation/apply/drift scripts |
| `services/` | Maven multi-module: `common-avro`, `txn-producer`, `fraud-detector`, `notifier`, `dlq-replay` |
| `infra/compose/` | Core stack + monitoring, security (WP2.1/WP2.2) and connect (WP2.4) overlays |
| `security/` | PKI and SCRAM credential scripts (`scripts/`), generated material git-ignored |
| `monitoring/` | Prometheus rules, Alertmanager, Loki/Promtail, JMX exporter config, Grafana dashboards |
| `docs/` | ADRs, runbooks, drill reports, security docs, gates readiness, deployment guide, performance baseline |
| `chaos/` | Failure-drill scripts D1–D6 + timestamped evidence (WP2.7 execution) |
| `scripts/` | Bootstrap, readiness, smoke test, certificate-expiry exporter, diagnostics collection |
| `.github/workflows/` | CI: build/tests, config lint, shell/Dockerfile lint, secret scan |

## Development

```bash
make build           # compile + full unit-test suite (no Docker needed)
make validate-config # lint topics/ACLs/schemas/connect/security/monitoring
```

- `common-avro` generates classes straight from `kafka-config/schemas/*.avsc` —
  the single source of truth shared with Schema Registry.
- Integration tests use Testcontainers and run wherever Docker is available
  (CI runners; locally they self-skip via `disabledWithoutDocker`).
- Idempotency: `notifier` inserts a `processed_events` marker and the business
  rows in one transaction, then commits the Kafka offset (FR-06). Replaying an
  alert any number of times yields exactly one notification row.
- Hardening (Stage 2): velocity window rule (TopologyTestDriver-verified),
  DLQ with the Document 03 header set on both stream and consumer sides
  (ADR-0006), and a `dlq-replay` operator tool with a dry-run mode.

## Lifecycle stage status

| Stage | Scope | Status |
|---|---|---|
| 0 — Prototype | Environment baseline (P0) recorded in `.env.example` heaps | ✅ recorded 2026-10-06 |
| 1 — MVP | Cluster, 3 services, amount rule, minimal dashboards, CI | ✅ 2026-10-06 (22 tests) |
| 2 — Final | Security, hardening, CDC, full observability, config-as-code; drills D1–D6 executed twice with evidence 2026-11-23; rolling upgrade T12 PASS x2; T10 performance baseline measured | ✅ 2026-11-23 |
| 3 — Commercial-ready | K8s, IaC, DR, SLOs (optional, time-boxed) | ⬜ planned |

## Known limitations

- Security overlay (`make up-secure`) is written per ADR-0002 but its runtime mTLS/SCRAM handshake behaviour is not yet exercised on the secured stack; one calibration pass is budgeted.
- JMX broker metrics ship as config (`monitoring/jmx/broker.yml`) but the broker javaagent wiring is documented, not enabled — the related alerts activate once wired.
- Alertmanager's lab receiver intentionally has no external integrations; the UI/API is the evidence surface for drill screenshots.
- Schema Registry HTTP endpoint is unauthenticated inside the Compose network (lab-only); Kafka listeners are fully secured in `up-secure` mode.
- D5 (certificate expiry drill) requires the secured stack and is pending the `make up-secure` calibration pass.
- Soak test (T14) not yet run — scheduled after secured stack validation.

## License

MIT — see [LICENSE](LICENSE).
