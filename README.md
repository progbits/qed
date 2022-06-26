# Qed

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

### Examples

A simple usage example can be found [here](https://github.com/progbits/qed/tree/main/examples).
