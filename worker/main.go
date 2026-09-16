package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	kafka "github.com/segmentio/kafka-go"
)

// How many times to try the Redis write before giving up on a message
const maxRedisRetries = 3

type InferenceRequest struct {
	RequestID string `json:"request_id"`
	Prompt    string `json:"prompt"`
	Model     string `json:"model"`
}

type InferenceResult struct {
	Status string `json:"status"`
	Prompt string `json:"prompt"`
	Result string `json:"result"`
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// statusHealthServer runs a tiny HTTP server in the background so something
// outside the worker (a container orchestrator, a load balancer, or just curl)
// can check whether this worker is alive. The Kafka loop is not an HTTP server,
// so without this there is no way to knock on the worker's door
func statusHealthServer(port string) {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"worker"}`))
	})
	addr := ":" + port
	//This blocks(it serves forever), which is why the caller runs it in a
	//goroutine so the Kafka loop can keep running alongside it.
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Health server error: %v", err)
	}
}

// writeResultWithRetry writes the result to Redis, retrying with backoff if the
// write fails. Most write failures are transient, for example, like a brief Redis hiccup,
// so a few retries usually recover without throwing away the work the worker just did.
// Returns nil as soon as one attempt succeeds, or the last error if all fail.
func writeResultWithRetry(ctx context.Context, rdb *redis.Client, key, value string) error {
	var lastErr error
	for attempt := 1; attempt <= maxRedisRetries; attempt++ {
		lastErr = rdb.SetEX(ctx, key, value, 5*time.Minute).Err()
		if lastErr == nil {
			return nil //write succeeded
		}
		log.Printf("Redis write attempt %d/%d failed: %v", attempt, maxRedisRetries, lastErr)
		if attempt < maxRedisRetries {
			//Wait before the next try, and wait longer each time (200ms, 400ms).
			//This gives Redis room to recover instead of hammering it.
			time.Sleep(time.Duration(attempt*200) * time.Millisecond)
		}
	}
	return lastErr
}

func main() {
	kafkaBroker := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")
	healthPort := getEnv("WORKER_HEALTH_PORT", "8080")

	//start the healthserver in the background. The main loop below keeps
	//pulling from Kafka this serves health checks consurrently.
	go statusHealthServer(healthPort)
	log.Printf("Health server listening on: %s/healthz", healthPort)

	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", redisHost, redisPort),
	})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "inference-requests",
		GroupID:  "inference-workers",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Worker started, waiting for messages...")

	for {
		// FetchMessage reads the next message WITHOUT committing its offset
		// (ReadMessage commits as it reads)
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error fetching message: %v", err)
			continue
		}

		var request InferenceRequest
		if err := json.Unmarshal(msg.Value, &request); err != nil {
			log.Printf("Error parsing message: %v", err)
			//This message will never parse.
			//Committed so it does not re-read the same broker message on every restart
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Error committing offset for unparseable message: %v", err)
			}
			continue
		}

		//Validate requred input fields. A message can be valid JSON but still be
		//ususable (empty id or empty prompt). That's a permanent failure, not a
		//transient one, so skip and commit rather than retry.
		if request.RequestID == "" || request.Prompt == "" {
			log.Printf("Invalid request (empty request_id or prompt), skipping: %s", string(msg.Value))
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Error committing offset for invalid message: %v", err)
			}
			continue
		}

		log.Printf("Processing request: %s | prompt: %s", request.RequestID, request.Prompt)

		// Simulate inference work
		time.Sleep(1 * time.Second)
		result := fmt.Sprintf("Processed: %s [model=%s]", request.Prompt, request.Model)

		inferenceResult := InferenceResult{
			Status: "completed",
			Prompt: request.Prompt,
			Result: result,
		}

		resultJSON, _ := json.Marshal(inferenceResult)

		key := fmt.Sprintf("request:%s", request.RequestID)
		//Write the result first, with bounded retry. Only if it succeeds do we
		//commit the offset
		if err := writeResultWithRetry(context.Background(), rdb, key, string(resultJSON)); err != nil {
			//Every retry failed. Do NOT commit, so the message stays uncommitted
			//and is reprocessed after a restart (preserving at-least-once).
			log.Printf("All Redis write attempts failed for %s (offset left uncommitted): %v", request.RequestID, err)
			continue
		}

		//Result is safely in Redis, at this point mark the message as done.
		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Printf("Error committing offset for %s: %v", request.RequestID, err)
			continue
		}

		log.Printf("Completed request: %s", request.RequestID)
	}
}
