# Analytics Tracker

Hệ thống thu thập và phân tích lưu lượng website (click, pageview, hiệu năng trang) với dashboard real-time và cảnh báo. Dự án học tập/portfolio tập trung vào **xử lý bất đồng bộ, dữ liệu time-series và streaming phân tán**, với Go làm service chịu tải cao và Node.js lo phần nghiệp vụ.

> Trạng thái: **Giai đoạn 1 (Asynchronous Processing)**. Xem lộ trình đầy đủ ở [docs/PLAN.md](docs/PLAN.md).

## Tính năng mục tiêu

- SDK JavaScript nhẹ nhúng vào website khách, gửi event bằng `sendBeacon`.
- Ingest service (Go) nhận hàng chục nghìn request/giây, trả `202` ngay, không chờ DB.
- Xử lý bất đồng bộ: consumer group, retry, DLQ, xử lý idempotent.
- Phân tích time-series: lượng truy cập, P95/P99 latency (LCP, TTFB, load time).
- Dashboard real-time (SSE) và biểu đồ lịch sử.
- Alert rules với cooldown và webhook ký HMAC.
- Nâng cấp lên Kafka (Redpanda) khi có nhu cầu thực: partitions, lag monitoring, replay, nhiều consumer độc lập.

## Kiến trúc tóm tắt

```
SDK -> Go Ingest -> Queue -> Go Worker -> Event Store -> Node API -> Dashboard
                      │                       │
                      │                       └─ Alert evaluator -> Webhook
                      └─ Redis Streams (GĐ1-2)  /  Kafka (GĐ3)
```

| Thành phần | Công nghệ |
|---|---|
| Tracking SDK | JavaScript thuần |
| Ingest, Worker | Go (`net/http` + `chi`, `pgx`, `clickhouse-go`, `franz-go`) |
| Queue | Redis Streams, sau đó Kafka/Redpanda |
| Event store | PostgreSQL (GĐ1), ClickHouse (GĐ2+) |
| API | Node.js + Fastify + TypeScript, Drizzle, Zod |
| Dashboard | React + Vite + ECharts |
| Hạ tầng | Docker Compose, Prometheus, Grafana, k6 |

Chi tiết kiến trúc, schema và quyết định thiết kế nằm trong [docs/PLAN.md](docs/PLAN.md).

## Cấu trúc thư mục

```
.
├── docker-compose.yml
├── Makefile
├── LICENSE
├── .github/workflows/ci.yml
├── .env.example
├── docs/PLAN.md          # kế hoạch toàn bộ dự án
├── sdk/                  # JS tracking SDK
├── services/
│   ├── api/              # Node + Fastify
│   ├── ingest/           # Go: nhận /collect
│   └── worker/           # Go: consume, ghi store, alert
├── web/                  # React dashboard
├── migrations/
│   ├── postgres/
│   └── clickhouse/
├── loadtest/             # k6 scripts
└── deploy/               # Prometheus, Grafana, ...
```

## Yêu cầu

- Docker và Docker Compose v2
- Go 1.22+
- Node.js 20+ và pnpm
- [goose](https://github.com/pressly/goose) (migration): `make goose-install`
- k6 (cho load test)
- `make` (tuỳ chọn, có thể chạy lệnh `docker compose` trực tiếp)

### Package manager

Repo dùng **pnpm 12** cho toàn bộ thành phần JavaScript/TypeScript. Node.js và pnpm
được khai báo trong [package.json](package.json); dùng đúng package manager để
giữ lockfile và hành vi cài dependency nhất quán:

```bash
pnpm --version
pnpm install
pnpm run check:compose
```

Các package con (`services/api`, `web`, `sdk`) sẽ có `package.json` riêng khi
được triển khai ở các giai đoạn tiếp theo. Không dùng `npm install` hoặc
`yarn install` trong repo này.

## Chạy nhanh hạ tầng

```bash
cp .env.example .env

make up          # Giai đoạn 1: Postgres + Redis
make up-ch       # thêm ClickHouse (Giai đoạn 2)
make up-kafka    # thêm Redpanda + Console (Giai đoạn 3)
make up-obs      # thêm Prometheus + Grafana
```

Kiểm tra: `make ps`. Xoá sạch dữ liệu: `make reset`.

### Migration (goose)

```bash
make goose-install
make migrate-pg-up                      # áp dụng migration Postgres
make migrate-pg-status
make migrate-pg-new name=create_events  # tạo file migration mới
make migrate-ch-up                      # ClickHouse (cần profile clickhouse)
```

DSN mặc định khớp với `.env.example`, ghi đè bằng `PG_DSN=... make migrate-pg-up`.

### Cổng mặc định

| Dịch vụ | Cổng | Ghi chú |
|---|---|---|
| Postgres | 5432 | user/pass/db: `tracker` |
| Redis | 6379 | |
| ClickHouse HTTP / native | 8123 / 9000 | profile `clickhouse` |
| Redpanda Kafka (external) | 19092 | profile `kafka` |
| Redpanda Console | 8080 | profile `kafka` |
| Prometheus | 9090 | profile `observability` |
| Grafana | 3001 | `admin` / `admin` |
| Ingest (Go) | 8081 | khi có service |
| API (Node) | 3000 | khi có service |

> Thông tin đăng nhập trong `.env.example` chỉ dành cho môi trường dev. Không dùng ở production.

## Lệnh hữu ích

```bash
make psql         # shell Postgres
make redis-cli    # shell Redis
make ch-client    # shell ClickHouse
make logs         # xem log
```

## Quy ước phát triển

- Go service tách qua interface `Queue` và `EventStore` để đổi Redis→Kafka, Postgres→ClickHouse chỉ bằng adapter.
- Mọi event có `event_id` (UUIDv7) do SDK sinh; ghi DB phải idempotent.
- Go không bao giờ chờ DB trong request path của ingest.
- Node và Go giao tiếp qua DB, Redis, Kafka; không gọi HTTP trực tiếp cho nhau.
- Migration là SQL thuần chạy bằng `goose`, nằm trong `migrations/`, dùng chung cho cả hai ngôn ngữ.
- Commit theo Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`).

## Lộ trình

| Giai đoạn | Chủ đề | Trạng thái |
|---|---|---|
| 0 | Khởi tạo repo, Docker, tài liệu, CI | Hoàn thành |
| 1 | Asynchronous Processing (Redis Streams) | Đang làm |
| 2 | Analytics & Alerting (ClickHouse) | Chưa bắt đầu |
| 3 | Distributed Streaming (Kafka) | Chưa bắt đầu |

## Giấy phép

MIT, xem [LICENSE](LICENSE).