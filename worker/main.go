package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	kafka "github.com/segmentio/kafka-go"
)

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

func main() {
	kafkaBroker := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")

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
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("Error committing offset for unparseable message: %v", err)
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
		ctx := context.Background()
		//Write the result first. Only if succeeds do we commit offset.
		if err := rdb.SetEX(ctx, fmt.Sprintf("request:%s", request.RequestID), string(resultJSON), 5*time.Minute).Err(); err != nil {
			//Redis write failed. No committing, wo the message stays uncommitted and is redelivered
			// after a restart.
			log.Printf("Error writing to Redis for %s: %v (offset left uncommitted)", request.RequestID, err)
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
