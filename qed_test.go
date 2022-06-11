package qed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	_ "github.com/lib/pq"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	PostgresImage = "postgres:14.3"
)

// setupPostgres starts a new dockerized PostgreSQL instance, applies project
// schema migrations and returns a handle to the database.
func setupPostgres(t *testing.T) (*sql.DB, func(), error) {
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	out, err := cli.ImagePull(ctx, PostgresImage, types.ImagePullOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer out.Close()
	io.Copy(os.Stdout, out)

	port := "5432"
	password := "password"
	database := "postgres"
	resp, err := cli.ContainerCreate(
		ctx,
		&container.Config{
			ExposedPorts: map[nat.Port]struct{}{
				nat.Port(port): {},
			},
			Env: []string{
				fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
				fmt.Sprintf("POSTGRES_DB=%s", database),
			},
			Image: PostgresImage,
		},
		&container.HostConfig{
			PortBindings: map[nat.Port][]nat.PortBinding{
				nat.Port(port): {
					{
						HostIP:   "localhost",
						HostPort: port,
					},
				},
			},
		},
		nil,
		nil,
		"")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		err := cli.ContainerRemove(
			ctx,
			resp.ID,
			types.ContainerRemoveOptions{Force: true})
		if err != nil {
			t.Log(err)
		}
	}

	// Start the container.
	err = cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// Open a new connection to the database.
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
		"localhost",
		port,
		database,
		password,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// Wait for the database to start.
	started := false
	retryCount := 16
	for i := 0; i < retryCount; i++ {
		_, err = db.Exec("SELECT 1")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		started = true
		break
	}

	if !started {
		// Database failed to start.
		cleanup()
		return nil, nil, errors.New("failed to start database")
	}

	// Load the schema.
	migration, err := os.ReadFile("database/postgres/schema.sql")
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// Apply the schema.
	_, err = db.Exec(string(migration))
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	return db, cleanup, nil
}

func TestNoHandler(t *testing.T) {
	db, cleanup, err := setupPostgres(t)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Create a new queue that will poll every 50 milliseconds.
	taskQueue := NewTaskQueue(db, 50*time.Millisecond, 60*time.Second)

	_, err = taskQueue.QueueTask("foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = taskQueue.Run()
	if err == nil {
		t.Fatal("task run without handler")
	}

	if !strings.Contains(err.Error(), "foo") {
		t.Fatal("expected queue name in error message")
	}
}

func TestSimpleTasks(t *testing.T) {
	db, cleanup, err := setupPostgres(t)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Create a new queue that will poll every 50 milliseconds.
	taskQueue := NewTaskQueue(db, 50*time.Millisecond, 60*time.Second)

	// Task handler sets the appropriate array item to done.
	mutex := sync.Mutex{}
	items := make(map[string]string)
	handler := func(data []byte) {
		mutex.Lock()
		defer mutex.Unlock()
		items[string(data)] = string(data) + "foo"
	}
	// Register the handler.
	taskQueue.RegisterHandler("test", handler)

	// Start the task queue
	go func() {
		err = taskQueue.Run()
		if err != nil {
			t.Log(err)
		}
	}()

	// Register tasks.
	for i := 0; i < 64; i++ {
		delay := time.Duration(rand.Int63n(60)) * time.Second
		err = taskQueue.QueueTaskWithDelay("test", []byte(strconv.Itoa(i)), delay)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait until we have drained all tasks.
	for {
		remaining, err := taskQueue.size()
		if err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Check we handled each task.
	for k, v := range items {
		expected := k + "foo"
		if v != expected {
			t.Fatalf("failed to run task for item %s", k)
		}
	}
}

func TestExpiredTasks(t *testing.T) {
	db, cleanup, err := setupPostgres(t)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Create a new queue that will poll every 50 milliseconds and reclaim
	// tasks that have not been acked after 10 seconds.
	taskQueue := NewTaskQueue(db, 50*time.Millisecond, 10*time.Second)

	// Task handler sets the appropriate array item to done. The handler might
	// block for longer than the ack timeout.
	mutex := sync.Mutex{}
	items := make(map[string]string)
	handler := func(data []byte) {
		if rand.Float32() < 0.5 {
			time.Sleep(15 * time.Second)
		}
		mutex.Lock()
		defer mutex.Unlock()
		items[string(data)] = string(data) + "foo"
	}
	// Register the handler.
	taskQueue.RegisterHandler("test", handler)

	// Start the task queue
	go func() {
		err = taskQueue.Run()
		if err != nil {
			t.Log(err)
		}
	}()

	// Register tasks.
	for i := 0; i < 64; i++ {
		delay := time.Duration(rand.Int63n(60)) * time.Second
		err = taskQueue.QueueTaskWithDelay("test", []byte(strconv.Itoa(i)), delay)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait until we have drained all tasks.
	for {
		remaining, err := taskQueue.size()
		if err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Check we handled each task.
	for k, v := range items {
		expected := k + "foo"
		if v != expected {
			t.Fatalf("failed to run task for item %s", k)
		}
	}
}
