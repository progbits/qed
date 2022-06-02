package qed

import (
	"context"
	"database/sql"
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
	"sync"
	"testing"
	"time"
)

const (
	PostgresImage = "postgres:14.3"
)

func setupPostgres(t *testing.T) (*sql.DB, func(), error) {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, nil, err
	}

	out, err := cli.ImagePull(ctx, PostgresImage, types.ImagePullOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer out.Close()
	io.Copy(os.Stdout, out)

	resp, err := cli.ContainerCreate(
		ctx,
		&container.Config{
			ExposedPorts: map[nat.Port]struct{}{"5432": {}},
			Env: []string{
				"POSTGRES_PASSWORD=password",
				"POSTGRES_DB=postgres",
			},
			Image: PostgresImage,
		},
		&container.HostConfig{
			PortBindings: map[nat.Port][]nat.PortBinding{"5432": {{HostIP: "localhost", HostPort: "5432"}}},
		},
		nil,
		nil,
		"")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		err = cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{Force: true})
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
		"5432",
		"postgres",
		"password",
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// Wait for the database to start.
	retryCount := 16
	for i := 0; i < retryCount; i++ {
		_, err = db.Exec("SELECT 1")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	// Apply the schema migration.
	migration, err := os.ReadFile("database/postgres/schema.sql")
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	_, err = db.Exec(string(migration))
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	return db, cleanup, nil
}

func TestQed(t *testing.T) {
	db, cleanup, err := setupPostgres(t)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Create a new queue that will poll every 50 milliseconds.
	taskQueue := NewTaskQueue(db, 50*time.Millisecond)

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

	// Register tasks.
	for i := 0; i < 64; i++ {
		delay := time.Duration(rand.Int63n(60)) * time.Second
		err = taskQueue.QueueTaskWithDelay("test", []byte(strconv.Itoa(i)), delay)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start the task queue
	go func() {
		err = taskQueue.Run()
		if err != nil {
			t.Log(err)
		}
	}()

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
