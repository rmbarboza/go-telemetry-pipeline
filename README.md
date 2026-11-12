# go-telemetry-pipeline

Go service that ingests numeric metric events over HTTP and publishes them to
Kafka, answering `202 Accepted` only after the broker acknowledges the write.
An idempotent PostgreSQL persistence layer is in place; the consumer that
connects Kafka to PostgreSQL is in progress.

See [docs/architecture.md](docs/architecture.md) for the design, the HTTP
contract and the timeouts, and [docs/adr](docs/adr) for the decisions behind
them.

## Running the application

The API needs a Kafka broker on `localhost:9092` with the topic `events`. The
service does not create topics: their settings are part of the durability
guarantee (ADR 0003). Without the topic, every request returns `503`.

```bash
docker run -d --name telemetry-kafka -p 9092:9092 apache/kafka:4.1.0

docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic events --partitions 3 --replication-factor 1
```

Start the server on port 8080:

```bash
go run ./cmd/telemetry
```

Send an event; a `202` means Kafka stored it:

```bash
curl -i http://127.0.0.1:8080/metrics \
  -H "Content-Type: application/json" \
  -d "{\"id\": \"0192a5b0-6c2e-7f3a-8b4d-1e2f3a4b5c6d\", \"timestamp\": $(date +%s), \"key\": \"input_traffic\", \"value\": 0}"
```

## Running integration tests

Integration tests run against a real PostgreSQL database. Use a disposable
database: tests insert rows and do not clean them up.

Start PostgreSQL in Docker and apply the migration:

```bash
docker run -d --name telemetry-pg \
  -e POSTGRES_USER=telemetry \
  -e POSTGRES_PASSWORD=telemetry \
  -e POSTGRES_DB=telemetry \
  -p 5432:5432 \
  postgres:18

docker exec -i telemetry-pg psql -U telemetry -d telemetry < migrations/0001_create_event.sql
```

The database must use `UTF8` encoding (the default for the official image), so
that every key accepted by the API is also accepted by the database (ADR 0002).

Point the tests to the database and run them:

```bash
export TELEMETRY_DATABASE_URL='postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable'
go test -count=1 ./...
```

When `TELEMETRY_DATABASE_URL` is not set, integration tests are skipped and
only unit tests run.

To reset the database, remove the container (`docker rm -f telemetry-pg`) and
repeat the steps above.
