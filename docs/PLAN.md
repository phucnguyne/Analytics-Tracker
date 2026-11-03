# PLAN: Analytics Tracker & Alerting System

Tài liệu kế hoạch tổng thể. Cập nhật tài liệu này khi có quyết định mới (xem mục ADR).

## 1. Mục tiêu

**Sản phẩm:** hệ thống thu thập click/pageview/hiệu năng từ website khách, xử lý, hiển thị real-time và cảnh báo.

**Mục tiêu học tập (ưu tiên cao hơn tính năng):**

1. Xử lý bất đồng bộ đúng nghĩa: at-least-once, retry, DLQ, idempotent.
2. Làm việc với CSDL time-series/columnar: ClickHouse, percentile, materialized view.
3. Streaming phân tán: Kafka, partition, lag, replay, nhiều consumer độc lập.
4. Chứng minh bằng số liệu: load test, chaos test, benchmark.

**Ngoài phạm vi (non-goals):** session replay, heatmap, A/B test, đa tenant billing, ML phát hiện bất thường.

## 2. Mục tiêu hiệu năng (SLO, đo bằng k6)

| Chỉ số | Mục tiêu GĐ1 | Mục tiêu GĐ2-3 |
|---|---|---|
| Ingest throughput (1 instance, máy dev) | ≥ 10.000 rps | ≥ 20.000 rps |
| Ingest p99 latency | < 20 ms | < 20 ms |
| Mất dữ liệu khi kill worker | 0 event | 0 event |
| Trùng dữ liệu trong store | 0 event | 0 event |
| Độ trễ event -> dashboard live | < 3 s | < 3 s |
| Truy vấn biểu đồ 24h (p95) | < 300 ms | < 100 ms |
| Phát hiện alert -> gửi webhook | < 60 s | < 30 s |

Các con số là mục tiêu ban đầu, hiệu chỉnh sau lần đo đầu tiên và ghi lại trong `docs/BENCHMARKS.md`.

## 3. Kiến trúc

### 3.1 Giai đoạn 1-2 (Redis Streams)

```
SDK -> Go Ingest -> Redis Streams (events) -> Go Worker (consumer group)
                                              ├─ EventStore (Postgres -> ClickHouse)
                                              ├─ Aggregator -> Redis Pub/Sub -> Node SSE -> Dashboard
                                              └─ Alert evaluator -> Redis cooldown -> Webhook
Node Fastify <-> Postgres (users, sites, rules, alert_events)
Node Fastify  -> ClickHouse (truy vấn biểu đồ)
```

### 3.2 Giai đoạn 3 (Kafka)

```
SDK -> Go Ingest -> Kafka topic `events` (key = site_id)
                      ├─ group: clickhouse-writer
                      ├─ group: realtime-aggregator
                      └─ group: alert-evaluator
```

### 3.3 Phân chia trách nhiệm

- **Go ingest:** validate, rate limit, enrich, publish. Không bao giờ chờ DB.
- **Go worker:** consume, batch ghi store, aggregate, đánh giá alert, gửi webhook.
- **Node API:** auth, CRUD site/rule, truy vấn thống kê, SSE.
- Node và Go chỉ chia sẻ dữ liệu qua DB, Redis, Kafka. Không gọi HTTP lẫn nhau.

### 3.4 Interface cốt lõi (Go)

```go
type Queue interface {
    Publish(ctx context.Context, e Event) error
    Consume(ctx context.Context, group string, h func(ctx context.Context, batch []Message) error) error
}

type EventStore interface {
    InsertBatch(ctx context.Context, events []Event) error
}
```

Adapter dự kiến: `redisstream`, `kafka` (Queue); `postgres`, `clickhouse` (EventStore).

## 4. Hợp đồng dữ liệu

### 4.1 Event (JSON từ SDK)

| Trường | Kiểu | Ghi chú |
|---|---|---|
| `event_id` | UUIDv7 | SDK sinh, khoá idempotency |
| `site_key` | string | định danh site công khai |
| `type` | enum | `pageview`, `click`, `perf`, `custom` |
| `url`, `referrer` | string | |
| `session_id` | string | sinh phía client |
| `ts` | int64 ms | thời gian client, server ghi thêm `received_at` |
| `props` | object | tuỳ chọn, giới hạn kích thước |
| `perf` | object | `load_ms`, `ttfb_ms`, `lcp_ms` (cho type `perf`) |

Server làm giàu: `country`, `device`, `browser`, `os`, `received_at`. Không lưu IP thô (hash có salt xoay theo ngày nếu cần đếm unique).

### 4.2 Endpoint ingest

`POST /collect` nhận một event hoặc mảng (tối đa 100, body ≤ 64 KB). Trả `202` khi đã publish; `400` payload sai; `401/403` sai key hoặc domain; `429` vượt rate limit.

### 4.3 Redis keys (GĐ1-2)

| Key | Mục đích |
|---|---|
| `stream:events` | stream chính |
| `stream:events:dlq` | message lỗi quá ngưỡng retry |
| `site:{site_key}` | cache cấu hình site (TTL ngắn) |
| `rl:{site_id}:{window}` | rate limit |
| `live:{site_id}` | kênh Pub/Sub số liệu live |
| `alert:cooldown:{rule_id}` | cooldown, `SET NX EX` |

## 5. Schema (khung, chốt ở từng giai đoạn)

**Postgres:** `users`, `sites(site_key, allowed_domains, owner_id)`, `alert_rules(site_id, type, params, cooldown_s, webhook_url, secret)`, `alert_events(rule_id, fired_at, status, attempts, response)`. GĐ1 thêm `events` làm sink tạm với `event_id UNIQUE`.

**ClickHouse (GĐ2):**

- `events`: `MergeTree`, `PARTITION BY toYYYYMM(ts)`, `ORDER BY (site_id, toStartOfMinute(ts), event_id)`, TTL 30-90 ngày.
- `events_1m`, `events_1h`: Materialized View vào `AggregatingMergeTree` (count, uniq, `quantilesTDigestState` cho P50/P95/P99).
- Dedupe: insert dedup token theo batch, kiểm tra thêm bằng `ReplacingMergeTree` nếu cần.

## 6. Lộ trình chi tiết

### Giai đoạn 0: Khởi tạo (HOÀN THÀNH)

- [x] README, docker-compose (profiles), Makefile, `.env.example`
- [x] Cấu trúc thư mục monorepo
- [x] License MIT (`LICENSE`)
- [x] CI cơ bản (`.github/workflows/ci.yml`): validate compose, lint/test/build cho Go và Node (tự bỏ qua phần chưa có code), kiểm tra migration up/down/up trên Postgres
- [x] `git init`, push lên GitHub và xác nhận CI chạy lần đầu
- [x] Chọn công cụ migration: `goose` (ADR #9)
- [x] Target `migrate-*` trong Makefile, đã kiểm tra với Postgres 16 thật (new, up, status, down, up lại)
- [x] Khai báo Node.js/pnpm và script kiểm tra compose ở `package.json`

### Giai đoạn 1: Asynchronous Processing (Redis Streams) (HIỆN TẠI)

**Mục tiêu:** pipeline bền vững, không mất và không trùng dữ liệu.

- [ ] Migration Postgres: `events` (`event_id` unique), `sites`, `users`
- [ ] Go ingest: `/collect`, validate, cache site qua Redis, rate limit, `XADD` (có `MAXLEN ~`), `/healthz`, `/metrics`
- [ ] Interface `Queue`, adapter `redisstream`
- [ ] Go worker: `XREADGROUP`, batch (1000 event hoặc 500 ms), `INSERT ... ON CONFLICT DO NOTHING`, `XACK` sau khi ghi thành công
- [ ] Retry: theo dõi delivery count, quá ngưỡng thì chuyển `events:dlq`
- [ ] Pending: `XAUTOCLAIM` nhận lại message idle quá N giây; dashboard/metric cho kích thước PEL
- [ ] Graceful shutdown (flush batch, ack) cho cả ingest và worker
- [ ] SDK JS: sinh UUIDv7, batch, `sendBeacon`, fallback `fetch keepalive`
- [ ] Load test k6: ramp 1k -> 10k+ rps, ghi p50/p95/p99
- [ ] Chaos test: gửi 1 triệu event, kill worker ngẫu nhiên, kết quả DB đúng 1 triệu
- [ ] Prometheus metrics: rps, latency, queue depth, PEL size, batch size, write latency

**Tiêu chí hoàn thành:** đạt SLO mục 2 cột GĐ1 và chaos test xanh. Ghi kết quả vào `docs/BENCHMARKS.md`.

### Giai đoạn 2: Analytics & Alerting (ClickHouse)

**Mục tiêu:** phân tích nhanh trên khối lượng lớn, cảnh báo đáng tin cậy.

- [ ] Migration ClickHouse: `events`, materialized views, TTL
- [ ] Adapter `clickhouse` cho `EventStore`, batch lớn, dedup token
- [ ] Dual-write Postgres và ClickHouse (tạm thời) để so sánh
- [ ] Benchmark truy vấn: count theo phút, top URL, P95/P99 trên 10M -> 100M dòng (script sinh dữ liệu)
- [ ] SDK gửi chỉ số hiệu năng (`load_ms`, `ttfb_ms`, `lcp_ms`)
- [ ] Node API: auth (JWT + cookie), CRUD site, endpoint thống kê (`/stats/overview`, `/stats/timeseries`, `/stats/top-pages`, `/stats/perf`), cache Redis ngắn hạn
- [ ] Node SSE: subscribe `live:{site_id}`, kiểm tra quyền sở hữu site
- [ ] Dashboard React + ECharts: realtime, biểu đồ theo khoảng thời gian, bảng top page, biểu đồ P95/P99
- [ ] Alert rules: `traffic_spike`, `traffic_drop_to_zero`, `p95_above`, `error_rate_above`
- [ ] Evaluator chạy định kỳ 10-30 s, so sánh với baseline (cùng giờ tuần trước hoặc trung bình trượt)
- [ ] Cooldown bằng Redis `SET NX EX`
- [ ] Webhook: ký HMAC-SHA256 (`X-Signature`, `X-Timestamp`), timeout, retry exponential backoff + jitter, lưu lịch sử vào `alert_events`, đánh dấu `failed` khi hết lượt
- [ ] Kênh thông báo bổ sung (tuỳ chọn): Telegram, Slack, email

**Tiêu chí hoàn thành:** đạt SLO cột GĐ2-3, có bảng so sánh Postgres và ClickHouse với số liệu thật.

### Giai đoạn 3: Distributed Streaming (Kafka)

**Điều kiện chuyển:** chỉ làm khi ghi nhận được ít nhất một tín hiệu đo được (Redis nghẽn CPU/RAM, cần nhiều consumer độc lập, cần replay). Ghi tín hiệu đó vào ADR trước khi bắt đầu.

- [ ] Redpanda (profile `kafka`), topic `events` với N partition, replication phù hợp môi trường
- [ ] Adapter `kafka` (`franz-go`) cho `Queue`, producer batch + compression, `acks=all` hoặc cấu hình có chủ đích
- [ ] Key partition = `site_id`; phân tích rủi ro partition nóng và cách giảm (salting cho site lớn)
- [ ] Tách 3 consumer group: `clickhouse-writer`, `realtime-aggregator`, `alert-evaluator`
- [ ] Commit offset thủ công sau khi xử lý xong (at-least-once), giữ idempotent
- [ ] Rebalance: xử lý đúng cooperative rebalance, không mất batch dở
- [ ] Giám sát lag: metrics lag theo group/partition vào Prometheus, dashboard Grafana, alert khi lag vượt ngưỡng
- [ ] Replay: reset offset theo thời điểm, chạy lại `clickhouse-writer` vào bảng mới để chứng minh sửa schema/bug
- [ ] Chạy song song Redis Streams và Kafka (feature flag) để so sánh, rồi cắt chuyển
- [ ] Load test và chaos test lặp lại (kill broker/consumer)

**Tiêu chí hoàn thành:** replay thành công, lag được giám sát, so sánh Redis Streams và Kafka có số liệu.

### Giai đoạn 4: Hoàn thiện (tuỳ chọn)

- [ ] Triển khai thật (VPS/Kubernetes), TLS qua Traefik
- [ ] Tài liệu kiến trúc có sơ đồ, blog/post tổng kết, video demo
- [ ] Hardening: bot filtering, kiểm tra domain allowlist, giới hạn kích thước, GDPR (không lưu PII)

## 7. Chiến lược kiểm thử

| Loại | Công cụ | Phạm vi |
|---|---|---|
| Unit | `go test`, `vitest` | validate, enrich, evaluator, HMAC |
| Integration | `testcontainers` (Go), `fastify.inject()` | worker với Redis + DB thật, API |
| Load | k6 | ingest rps/latency |
| Chaos | script shell | kill worker/broker, kiểm tra tổng event |
| Correctness | SQL đối chiếu | đếm event gửi và event lưu, kiểm tra trùng |

## 8. Quan sát (observability)

- Mỗi service expose `/metrics` (Prometheus) và `/healthz`.
- Metric tối thiểu: `ingest_requests_total{status}`, `ingest_latency_seconds`, `queue_publish_errors_total`, `worker_batch_size`, `worker_write_seconds`, `stream_pending_messages`, `dlq_messages_total`, `alert_fired_total`, `webhook_failures_total`.
- Log có cấu trúc (JSON): `slog` (Go), `pino` (Node), kèm `request_id`.

## 9. Bảo mật

- Site key công khai + kiểm tra `Origin` theo `allowed_domains`; rate limit theo site và IP.
- Mật khẩu hash bằng `argon2`; JWT ngắn hạn, refresh qua cookie `HttpOnly`.
- Webhook có chữ ký HMAC và timestamp để chống replay.
- Không lưu IP thô; tôn trọng Do Not Track nếu bật.
- Secret chỉ qua biến môi trường, không commit `.env`.

## 10. Rủi ro và cách giảm

| Rủi ro | Cách giảm |
|---|---|
| Redis hết RAM do stream phình | `MAXLEN ~`, giám sát, cảnh báo, chuyển Kafka khi cần |
| Insert nhỏ lẻ làm chậm ClickHouse | Batch lớn, giới hạn kích thước/thời gian flush |
| Trùng event khi retry/redeliver | `event_id` + idempotent write + dedup token |
| Spam từ bot/giả mạo key | Domain allowlist, rate limit, validate chặt |
| Partition nóng trên Kafka | Phân tích phân bố key, salting |
| Scope phình to | Giữ non-goals, hoàn thành tiêu chí từng giai đoạn mới sang tiếp |
| Alert spam | Cooldown, nhóm sự kiện, trạng thái firing/resolved |

## 11. Nhật ký quyết định (ADR)

| # | Quyết định | Lý do |
|---|---|---|
| 1 | Go cho ingest/worker, Node cho API | Go chịu concurrency cao, Node nhanh phát triển nghiệp vụ |
| 2 | Fastify thuần (không NestJS) | Nhẹ, ít magic, phần nặng đã ở Go |
| 3 | Redis Streams trước Kafka | Đơn giản, đủ cho GĐ1-2, học được consumer group/PEL |
| 4 | ClickHouse thay TimescaleDB cho analytics | Mục tiêu học columnar, percentile, materialized view |
| 5 | Postgres chỉ giữ dữ liệu quản lý (từ GĐ2) | Tách OLTP và OLAP |
| 6 | Interface `Queue`/`EventStore` | Đổi công nghệ bằng adapter, không đập logic |
| 7 | Kafka chỉ khi có tín hiệu đo được | Tránh phức tạp không cần thiết |
| 8 | SSE thay WebSocket cho live | Chỉ đẩy một chiều, đơn giản hơn |
| 9 | Migration bằng `goose` (SQL thuần) | Một công cụ cho cả Postgres và ClickHouse, là Go binary, đặt tên file theo thứ tự, có up/down, chạy được trong CI |

Việc cần chốt tiếp: thư viện validate Node (Zod hay TypeBox).

Quy ước goose: file SQL đặt trong `migrations/postgres` và `migrations/clickhouse`, tên `YYYYMMDDHHMMSS_mo_ta.sql` (tạo bằng `make migrate-pg-new name=mo_ta`), mỗi file có `-- +goose Up` và `-- +goose Down`. Câu lệnh nhiều dòng (function, trigger) bọc trong `-- +goose StatementBegin/End`. Với ClickHouse, migration không có transaction nên mỗi file chỉ nên chứa một thay đổi nhỏ và có `Down` rõ ràng.

## 12. Cột mốc gợi ý

| Mốc | Nội dung | Ước lượng |
|---|---|---|
| M0 | Repo, Docker, tài liệu | 0.5 ngày |
| M1 | Ingest + SDK + Streams chạy thông | 1 tuần |
| M2 | Worker, retry/DLQ/idempotent, chaos test | 1 tuần |
| M3 | ClickHouse, benchmark, Node API | 2 tuần |
| M4 | Dashboard live + alert + webhook | 2 tuần |
| M5 | Kafka, lag, replay, so sánh | 2 tuần |
| M6 | Hoàn thiện, deploy, tổng kết | 1 tuần |

Ước lượng cho một người làm bán thời gian, điều chỉnh theo thực tế.