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
		status 		JOB_STATUS DEFAULT 'Pending',
		payload 	BYTEA
	)
`

type Qed struct {
	Db      *sql.DB
	Handler func([]byte) error
}

func (q *Qed) Init() {
	_, err := q.Db.Exec(schema)
	if err != nil {
		panic(err)
	}
}

func (q *Qed) Run() {
	for {
		row := q.Db.QueryRow(`
			UPDATE qed
			SET status = 'Running'
			WHERE job_id = (
				SELECT job_id 
				FROM qed 
				WHERE status = 'Pending' 
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING job_id, payload
		`)

		var jobId string
		var data []byte
		err := row.Scan(&jobId, &data)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if err == sql.ErrNoRows {
			log.Println("no jobs queued")
			goto wait
		}

		go func() {
			err = q.Handler(data)
			if err == nil {
				_, err := q.Db.Exec("UPDATE qed SET status = 'Succeeded' WHERE job_id = $1", jobId)
				if err != nil {
					panic(err)
				}
			} else {
				_, err := q.Db.Exec("UPDATE qed SET status = 'Failed' WHERE job_id = $1", jobId)
				if err != nil {
					panic(err)
				}
			}
		}()

	wait:
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
	}
}

// AddJob adds a new job to the queue with an associated payload.
func (q *Qed) AddJob(payload []byte) {
	_, err := q.Db.Exec("INSERT INTO qed(payload) VALUES($1)", payload)
	if err != nil {
		panic(err)
	}
}
