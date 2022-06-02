package qed

import (
	"database/sql"
	"github.com/rs/xid"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type TaskQueue struct {
	mutex    sync.RWMutex
	handlers map[string]func([]byte)

	db              *sql.DB
	tick            time.Duration
	reclaimInterval time.Duration
}

// NewTaskQueue returns a new TaskQueue instance configured to use the
// specified database connection for persistent task storage. Tasks are polled
// and dispatched at an interval determined by the `tick` parameter.
func NewTaskQueue(db *sql.DB, tick time.Duration) *TaskQueue {
	return &TaskQueue{
		db:              db,
		handlers:        make(map[string]func([]byte)),
		tick:            tick,
		reclaimInterval: time.Second * 60,
	}
}

// RegisterHandler adds a new handler for the named queue. The handler is
// invoked when a task is dequeued from the named queue. If a previous
// handler was registered for the named queue, it will be replaced.
func (q *TaskQueue) RegisterHandler(queue string, handler func([]byte)) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.handlers[queue] = handler
}

// QueueTask queues a new task on the named queue.
func (q *TaskQueue) QueueTask(queue string, data []byte) (string, error) {
	taskId := xid.New().String()
	_, err := q.db.Exec(
		"SELECT qed_enqueue($1, $2, $3, now())",
		taskId, queue, data,
	)
	if err != nil {
		return "", err
	}
	return taskId, nil
}

// QueueTaskWithDelay queues a new task on the named queue to be run after a
// specified interval.
func (q *TaskQueue) QueueTaskWithDelay(queue string, data []byte, delay time.Duration) error {
	id := xid.New().String()
	_, err := q.db.Exec(
		"SELECT qed_enqueue($1, $2, $3, $4)",
		id, queue, data, delay.Seconds(),
	)
	return err
}

// Run starts the main queue polling loop.
func (q *TaskQueue) Run() error {
	// Start background task to unblock stuck tasks.
	go func() {
		for {
			err := q.reclaim()
			if err != nil {
				log.Printf("failed reclaim expired tasks: %s\n", err)
			}
			time.Sleep(q.tick)
		}
	}()

	for {
		time.Sleep(q.tick)

		// Fetch the next pending Task.
		t, err := q.fetchNext()
		if err != nil {
			log.Printf("failed to fetch next task: %s\n", err)
			continue
		}

		if t == nil {
			// No tasks pending to be run
			continue
		}

		// Fetch the task handler for the named queue.
		q.mutex.RLock()
		handler, ok := q.handlers[t.queue]
		q.mutex.RUnlock()
		if !ok {
			log.Printf("no handler registered for queue %s\n", t.queue)
			continue
		}

		// Run the handler and ack the Task.
		go func() {
			handler(t.data)
			err = q.ack(t.id)
			if err != nil {
				log.Printf("failed to ack task: %s\n", err)
			}
		}()
	}
}

// size returns the number of pending tasks in all queues.
func (q *TaskQueue) size() (int, error) {
	query := `
        SELECT count FROM qed_size
    `
	row := q.db.QueryRow(query)

	count := 0
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// task represents an instance of a task to be run.
type task struct {
	id    string
	queue string
	data  []byte
}

// fetchNext fetches the next task to be run.
func (q *TaskQueue) fetchNext() (*task, error) {
	query := `
        SELECT task_id, queue, data 
        FROM qed_dequeue();
    `
	row := q.db.QueryRow(query)
	t := task{}
	err := row.Scan(&t.id, &t.queue, &t.data)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}

	return &t, nil
}

// ack acknowledges (deletes) a task from the queue.
func (q *TaskQueue) ack(taskId string) error {
	query := `
        SELECT qed_ack($1)
    `
	_, err := q.db.Exec(query, taskId)
	return err
}

// reclaim unlocks any tasks
func (q *TaskQueue) reclaim() error {
	query := `
        SELECT qed_unlock($1)
    `
	_, err := q.db.Exec(query, q.reclaimInterval)
	return err
}
