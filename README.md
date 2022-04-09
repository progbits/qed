# Qed
A super simple PostgreSQL based job queue.

## Getting Started

Qed uses PostgreSQL to persist a record of work to be done. Database migrations can be found
in `database/migrations/postgres`. These migrations can be applied directly or copied to a projects migrations path to
be applied alongside other application specific migrations. Migrations can directly to a running PostgreSQL instance as
follows

```shell
psql -U postgres -h localhost -f database/postgres/schema.sql
```

## How Does it Work
The main component of **Qed** is the `job` table.

| job_id | status                                    | payload |
| :----- | :-----------------------------------------|:--------|
| UUID   | Pending OR Running OR Succeeded OR Failed | BYTEA

On every iteration of the **Qed** event loop, a job in the *Pending* state is
transitioned to the *Running* state and removed from the queue. A handler
function, of type `func([]byte) error`, registered with the **Qed** instance is
called in a goroutine with the associated payload for the job.
