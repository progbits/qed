package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

var counter int32

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
	db      *sql.DB
	handler func([]byte) error
}

func (q *Qed) run() {
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
			err = q.handler(data)
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

func (q *Qed) addJob(payload []byte) {
	_, err := q.db.Exec("INSERT INTO qed(payload) VALUES($1)", payload)
	if err != nil {
		panic(err)
	}
}

func handler(payload []byte) error {
	log.Printf("handling payload: %s\n", string(payload))
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
	if rand.Intn(100) < 5 {
		return errors.New("failed to handle job")
	}
	return nil
}

func producer(qed *Qed) {
	for {
		qed.addJob([]byte(fmt.Sprintf("%d", counter)))
		atomic.AddInt32(&counter, 1)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
	}
}

func connect() (*sql.DB, error) {
	host := os.Getenv("PG_HOST")
	port, err := strconv.Atoi(os.Getenv("PG_PORT"))
	if err != nil {
		port = 5432
	}
	user := os.Getenv("PG_USER")
	password := os.Getenv("PG_PASSWORD")
	dbname := os.Getenv("PG_DBNAME")

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)

	return sql.Open("postgres", connStr)
}

func main() {
	db, err := connect()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(schema)
	if err != nil {
		panic(err)
	}

	qed := &Qed{
		db:      db,
		handler: handler,
	}

	go producer(qed)
	qed.run()
}
