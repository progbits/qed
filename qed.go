package qed

import (
	"database/sql"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var schema = `
	DROP SCHEMA IF EXISTS qed CASCADE;
	
	CREATE SCHEMA IF NOT EXISTS qed;
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA qed;

	CREATE TYPE qed.JOB_STATUS as ENUM ('Pending', 'Running', 'Succeeded', 'Failed');

	CREATE TABLE IF NOT EXISTS qed.job(
		job_id 			UUID PRIMARY KEY DEFAULT qed.uuid_generate_v4(),
		queue 			TEXT NOT NULL,
		submitted_at 	TIMESTAMP DEFAULT now(),
		status 			qed.JOB_STATUS DEFAULT 'Pending',
		payload 		BYTEA
	)
`

var atomicFetchJob = `
UPDATE qed.job
SET status = 'Running'
WHERE job_id = (
    SELECT job_id
    FROM qed.job
    WHERE status = 'Pending'
    ORDER BY submitted_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING job_id, queue, payload
`

type Qed struct {
	db       *sql.DB
	handlers map[string]func([]byte) error
	tick     time.Duration
	mutex    sync.RWMutex
}

func NewQed(db *sql.DB, tick time.Duration) *Qed {
	_, err := db.Exec(schema)
	if err != nil {
		panic(err)
	}

	return &Qed{
		db:       db,
		handlers: make(map[string]func([]byte) error),
		tick:     tick,
	}
}

// AddHandler adds a new handler for the named queue.
func (q *Qed) AddHandler(queue string, handler func([]byte) error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.handlers[queue] = handler
}

// Run starts the main queue polling loop.
func (q *Qed) Run() {
	for {
		job := q.fetchNextPendingJob()
		if job != nil {
			q.mutex.RLock()
			handler, ok := q.handlers[job.queue]
			q.mutex.RUnlock()
			if !ok {
				log.Printf("no handler registered for queue %s\n", job.queue)
				err := q.updateJobStatus(job.id, "Pending")
				if err != nil {
					panic(err)
				}
				return
			}
			go q.handleJob(job, handler)
		} else {
			log.Println("no jobs queued")
		}
		time.Sleep(q.tick)
	}
}

func (q *Qed) handleJob(job *job, handler func([]byte) error) {
	status := "Succeeded"
	err := handler(job.data)
	if err != nil {
		status = "Failed"
	}

	err = q.updateJobStatus(job.id, status)
	if err != nil {
		panic(err)
	}
}

type job struct {
	id    string
	queue string
	data  []byte
}

// fetchNextPendingJob fetches the next job in the `Pending` status from the
// queue and updates its status to `Running`.
func (q *Qed) fetchNextPendingJob() *job {
	row := q.db.QueryRow(atomicFetchJob)

	job := job{}
	err := row.Scan(&job.id, &job.queue, &job.data)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	if err == sql.ErrNoRows {
		return nil
	}
	return &job
}

func (q *Qed) updateJobStatus(jobId string, status string) error {
	_, err := q.db.Exec(`
		UPDATE qed.job
		SET status = $1
		WHERE job_id = $2`,
		status, jobId)
	return err
}

// AddJob adds a new job to the named queue with an associated payload.
func (q *Qed) AddJob(queue string, payload []byte) {
	_, err := q.db.Exec(`
		INSERT INTO qed.job(queue, payload)
		VALUES($1, $2)`,
		queue, payload)
	if err != nil {
		panic(err)
	}
}
