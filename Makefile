.PHONY: up down reset ps logs psql redis-cli ch-client up-ch up-kafka up-obs up-all \
        goose-install migrate-pg-up migrate-pg-down migrate-pg-status migrate-pg-new \
        migrate-ch-up migrate-ch-down migrate-ch-status migrate-ch-new

PG_DSN ?= postgres://tracker:tracker@localhost:5432/tracker?sslmode=disable
CH_DSN ?= clickhouse://tracker:tracker@localhost:9000/tracker

up:           ## Giai đoạn 1: Postgres + Redis
	docker compose up -d
up-ch:        ## + ClickHouse
	docker compose --profile clickhouse up -d
up-kafka:     ## + Redpanda & Console
	docker compose --profile kafka up -d
up-obs:       ## + Prometheus & Grafana
	docker compose --profile observability up -d
up-all:
	docker compose --profile clickhouse --profile kafka --profile observability up -d
down:
	docker compose --profile clickhouse --profile kafka --profile observability down
reset:        ## Xoá sạch dữ liệu (volumes)
	docker compose --profile clickhouse --profile kafka --profile observability down -v
ps:
	docker compose --profile clickhouse --profile kafka --profile observability ps
logs:
	docker compose logs -f --tail=100
psql:
	docker compose exec postgres psql -U tracker -d tracker
redis-cli:
	docker compose exec redis redis-cli
ch-client:
	docker compose exec clickhouse clickhouse-client -u tracker --password tracker -d tracker

goose-install: ## Cài goose (cần Go)
	go install github.com/pressly/goose/v3/cmd/goose@latest

migrate-pg-up:
	goose -dir migrations/postgres postgres "$(PG_DSN)" up
migrate-pg-down:
	goose -dir migrations/postgres postgres "$(PG_DSN)" down
migrate-pg-status:
	goose -dir migrations/postgres postgres "$(PG_DSN)" status
migrate-pg-new:   ## make migrate-pg-new name=create_events
	goose -dir migrations/postgres create $(name) sql

migrate-ch-up:
	goose -dir migrations/clickhouse clickhouse "$(CH_DSN)" up
migrate-ch-down:
	goose -dir migrations/clickhouse clickhouse "$(CH_DSN)" down
migrate-ch-status:
	goose -dir migrations/clickhouse clickhouse "$(CH_DSN)" status
migrate-ch-new:   ## make migrate-ch-new name=create_events
	goose -dir migrations/clickhouse create $(name) sql