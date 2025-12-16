package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

/*
====================================================
GLOBAL SETUP
====================================================
*/

var (
	rootCtx context.Context
	cancel  context.CancelFunc
	client  *redis.Client
)

/*
====================================================
ENV HELPERS
====================================================
*/

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			return val
		}
	}
	return defaultVal
}

func getEnvString(key string, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

/*
====================================================
CONFIG
====================================================
*/

var (
	MaxRetries        = getEnvInt("MAX_RETRIES", 3)
	VisibilityTimeout = time.Duration(getEnvInt("VISIBILITY_TIMEOUT", 10)) * time.Second
	NumWorkers        = getEnvInt("WORKER_COUNT", 5)
	RedisAddr         = getEnvString("REDIS_ADDR", "localhost:6379")
)

/*
====================================================
JOB DEFINITION
====================================================
*/

type Job struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Retries   int                    `json:"retries"`
	StartedAt int64                  `json:"started_at"`
}

/*
====================================================
SERIALIZATION
====================================================
*/

func serializeJob(job Job) (string, error) {
	b, err := json.Marshal(job)
	return string(b), err
}

func deserializeJob(data string) (Job, error) {
	var job Job
	err := json.Unmarshal([]byte(data), &job)
	return job, err
}

/*
====================================================
IDEMPOTENCY
====================================================
*/

func isProcessed(jobID string) (bool, error) {
	exists, err := client.Exists(rootCtx, "processed:"+jobID).Result()
	return exists > 0, err
}

func markProcessed(jobID string) error {
	return client.Set(rootCtx, "processed:"+jobID, "1", 0).Err()
}

/*
====================================================
RECOVERY
====================================================
*/

func recoverStaleJobs() {
	fmt.Println("🧹 [Recovery] Requeuing stale jobs...")

	for {
		_, err := client.LMove(
			rootCtx,
			"processing",
			"jobs",
			"RIGHT",
			"LEFT",
		).Result()

		if err != nil {
			break
		}
	}

	fmt.Println("🧹 [Recovery] Done.")
}

/*
====================================================
MONITOR
====================================================
*/

func startMonitor() {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-rootCtx.Done():
			return
		case <-ticker.C:
			j, _ := client.LLen(rootCtx, "jobs").Result()
			p, _ := client.LLen(rootCtx, "processing").Result()
			d, _ := client.LLen(rootCtx, "dlq").Result()

			fmt.Printf(
				"\n📊 Pending: %d | In-Flight: %d | DLQ: %d\n\n",
				j, p, d,
			)
		}
	}
}

/*
====================================================
JOB PROCESSOR
====================================================
*/

func processJob(job Job) error {
	if job.Retries < 2 {
		return fmt.Errorf("simulated failure")
	}

	time.Sleep(2 * time.Second)
	return nil
}

/*
====================================================
WORKER
====================================================
*/

func startWorker(id int) {
	fmt.Printf("[Worker %d] Ready\n", id)

	for {
		select {
		case <-rootCtx.Done():
			fmt.Printf("[Worker %d] 🛑 Shutting down\n", id)
			return
		default:
		}

		jobStr, err := client.BLMove(
			rootCtx,
			"jobs",
			"processing",
			"LEFT",
			"RIGHT",
			5*time.Second,
		).Result()

		if err != nil {
			continue
		}

		job, err := deserializeJob(jobStr)
		if err != nil {
			client.LRem(rootCtx, "processing", 1, jobStr)
			client.LPush(rootCtx, "dlq", jobStr)
			continue
		}

		done, err := isProcessed(job.ID)
		if err != nil || done {
			client.LRem(rootCtx, "processing", 1, jobStr)
			continue
		}

		job.StartedAt = time.Now().Unix()
		updatedJobStr, _ := serializeJob(job)

		client.LRem(rootCtx, "processing", 1, jobStr)
		client.RPush(rootCtx, "processing", updatedJobStr)

		fmt.Printf(
			"[Worker %d] ▶ Processing job %s (retry %d)\n",
			id, job.ID, job.Retries,
		)

		if err := processJob(job); err == nil {
			_ = markProcessed(job.ID)
			client.LRem(rootCtx, "processing", 1, updatedJobStr)
			fmt.Printf("[Worker %d] ✅ Job %s completed\n", id, job.ID)
			continue
		}

		client.LRem(rootCtx, "processing", 1, updatedJobStr)
		job.Retries++
		job.StartedAt = 0
		failedJobStr, _ := serializeJob(job)

		if job.Retries <= MaxRetries {
			client.LPush(rootCtx, "jobs", failedJobStr)
		} else {
			client.LPush(rootCtx, "dlq", failedJobStr)
		}
	}
}

/*
====================================================
VISIBILITY TIMEOUT
====================================================
*/

func startVisibilitySweeper() {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-rootCtx.Done():
			return
		case <-ticker.C:
			jobs, _ := client.LRange(rootCtx, "processing", 0, -1).Result()
			now := time.Now().Unix()

			for _, jobStr := range jobs {
				job, err := deserializeJob(jobStr)
				if err != nil || job.StartedAt == 0 {
					continue
				}

				if now-job.StartedAt > int64(VisibilityTimeout.Seconds()) {
					client.LRem(rootCtx, "processing", 1, jobStr)
					job.Retries++
					job.StartedAt = 0
					updated, _ := serializeJob(job)

					if job.Retries <= MaxRetries {
						client.LPush(rootCtx, "jobs", updated)
					} else {
						client.LPush(rootCtx, "dlq", updated)
					}
				}
			}
		}
	}
}

/*
====================================================
MAIN
====================================================
*/

func main() {
	rootCtx, cancel = context.WithCancel(context.Background())
	defer cancel()

	client = redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	recoverStaleJobs()

	go startMonitor()
	go startVisibilitySweeper()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\n🛑 Shutdown signal received")
		cancel()
	}()

	var wg sync.WaitGroup
	fmt.Printf("🚀 Titan started with %d workers\n", NumWorkers)

	for i := 1; i <= NumWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			startWorker(id)
		}(i)
	}

	wg.Wait()
	fmt.Println("✅ Shutdown complete")
}
