## qed-go
A super simple PostgreSQL based job queue.

## How Does it Work
The main component of **Qed** is the `job` table.

| job_id | status                                    | payload |
| :----- | :-----------------------------------------|:--------|
| UUID   | Pending OR Running OR Succeeded OR Failed | BYTEA

On every iteration of the **Qed** event loop, a job in the *Pending* state is
transitioned to the *Running* state and removed from the queue. A handler
function, of type `func([]byte) error`, registered with the **Qed** instance is
called in a goroutine with the associated payload for the job.
