package main

import (
	"database/sql"
	"fmt"
	"github.com/progbits/qed"
	"log"
	"math/rand"
	"time"
)

const (
	queueName = "hello"
)

func main() {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		"localhost",
		"5432",
		"postgres",
		"password",
		"postgres",
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new queue that will poll every 150 milliseconds and re-queue
	// tasks that have not been acked after 60 seconds.
	options := qed.Options{Tick: 150 * time.Millisecond, Timeout: 60 * time.Second}
	taskQueue := qed.NewTaskQueue(db, options)

	// Register the 'hello' queue handler.
	taskQueue.RegisterHandler(queueName, func(data []byte) error {
		_, err := fmt.Printf("hello %s\n", data)
		return err
	})

	// Start the task queue.
	errCh := make(chan error)
	go func() {
		errCh <- taskQueue.Run()
	}()

	// Queue some tasks.
	names := []string{"John", "Sally"}
	for _, n := range names {
		delay := time.Duration(rand.Intn(30)) * time.Second
		err = taskQueue.QueueTaskWithDelay(queueName, []byte(n), delay)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = <-errCh
	if err != nil {
		fmt.Println(err)
	}
}
