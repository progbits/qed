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

	"github.com/progbits/qed-go"
)

var counter int32

func handler(payload []byte) error {
	log.Printf("handling payload: %s\n", string(payload))
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
	if rand.Intn(100) < 5 {
		return errors.New("failed to handle job")
	}
	return nil
}

func producer(qed *qed.Qed) {
	for {
		qed.AddJob([]byte(fmt.Sprintf("%d", counter)))
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

	queue := qed.NewQed(db)
	queue.AddHandler(handler)

	go producer(queue)
	queue.Run()
}
