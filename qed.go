package qed

import (
	"database/sql"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

var schema = `
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

	DROP TABLE IF EXISTS qed;

	DROP TYPE IF EXISTS JOB_STATUS;
	CREATE TYPE JOB_STATUS as ENUM ('Pending', 'Running', 'Succeeded', 'Failed');

	CREATE TABLE IF NOT EXISTS qed(
		job_id 		UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		queue 		TEXT NOT NULL,
		status 		JOB_STATUS DEFAULT 'Pending',
		payload 	BYTEA
	)
`

type Qed struct {
	db       *sql.DB
	handlers map[string]func([]byte) error
}

func NewQed(db *sql.DB) *Qed {
	_, err := db.Exec(schema)
	if err != nil {
		panic(err)
	}

	return &Qed{
		db:       db,
		handlers: make(map[string]func([]byte) error),
	}
}

func (q *Qed) AddHandler(queue string, handler func([]byte) error) {
	q.handlers[queue] = handler
}

func (q *Qed) Run() {
	for {
		row := q.db.QueryRow(`
			UPDATE qed
			SET status = 'Running'
			WHERE job_id = (
				SELECT job_id 
				FROM qed 
				WHERE status = 'Pending' 
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING job_id, queue, payload
		`)

		var jobId string
		var queue string
		var data []byte
		err := row.Scan(&jobId, &queue, &data)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if err == sql.ErrNoRows {
			log.Println("no jobs queued")
			goto wait
		}

		go func() {
			handler, ok := q.handlers[queue]
			if !ok {
				log.Printf("no handler registered for queue %s\n", queue)
				_, err := q.db.Exec("UPDATE qed SET status = 'Pending' WHERE job_id = $1", jobId)
				if err != nil {
					panic(err)
				}
				return
			}

			err = handler(data)
			if err == nil {
				_, err := q.db.Exec("UPDATE qed SET status = 'Succeeded' WHERE job_id = $1", jobId)
				if err != nil {
					panic(err)
				}
			} else {
				_, err := q.db.Exec("UPDATE qed SET status = 'Failed' WHERE job_id = $1", jobId)
				if err != nil {
					panic(err)
				}
			}
		}()

	wait:
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
	}
}

// AddJob adds a new job to the named queue with an associated payload.
func (q *Qed) AddJob(queue string, payload []byte) {
	_, err := q.db.Exec("INSERT INTO qed(queue, payload) VALUES($1, $2)", queue, payload)
	if err != nil {
		panic(err)
	}
}
