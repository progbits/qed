# Qed

Qed is a simple, persistent Postgres backed task queue.

## Getting Started

Qed leverages Postgres to persist its task queue. Database migrations can be found
in [`database/migrations/postgres`](https://github.com/progbits/qed/tree/main/database/postgres). These migrations can
be applied directly or copied to a projects migrations path to be applied alongside other application specific
migrations. Migrations can be applied directly to a running PostgreSQL instance as follows

```shell
psql -U postgres -h localhost -f database/postgres/schema.sql
```
