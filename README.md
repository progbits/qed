# Qed

[![Go](https://github.com/progbits/qed/actions/workflows/build.yaml/badge.svg?branch=main)](https://github.com/progbits/qed/actions/workflows/build.yaml)

Qed is a simple, Postgres backed task queue.

## Getting Started

### Setup

Qed leverages Postgres to persist state about running and pending tasks. The
Qed schema migration can be found in
[`database/migrations/postgres`](https://github.com/progbits/qed/tree/main/database/postgres).

Migrations can be applied directly to a running Postgres instance as follows:

```shell
psql -U postgres -h localhost -f database/postgres/schema.sql
```

### Running the Example

A simple usage example can be found [here](https://github.com/progbits/qed/tree/main/examples).

The example requires a running Postgres database with the Qed schema migration applied.

To start a new Postgres instance:

```shell
docker run -e 'POSTGRES_PASSWORD=password' -d -p 5432:5432 postgres:14.3
```

Once the Postgres instance is running, the Qed schema migration can be applied:

```shell
psql -U postgres -h localhost -f database/postgres/schema.sql
```

Once the database is set up, the example project can be built and run:

```shell
make example
```
