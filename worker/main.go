package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v8"
	"github.com/segmentio/kafka-go"
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

func main() {
	kafkaBroker := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", redisHost, redisPort),
	})

	// Kafka reader
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
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var request InferenceRequest
		if err := json.Unmarshal(msg.Value, &request); err != nil {
			log.Printf("Error parsing message: %v", err)
			continue
		}

		log.Printf("Processing request: %s | prompt: %s", request.RequestID, request.Prompt)

		// Simulate inference work
		time.Sleep(1 * time.Second)
		result := fmt.Sprintf("Processed: %s [model=%s]", request.Prompt, request.Model)

		// Update Redis with result
		inferenceResult := InferenceResult{
			Status: "completed",
			Prompt: request.Prompt,
			Result: result,
		}

		resultJSON, _ := json.Marshal(inferenceResult)
		ctx := context.Background()
		rdb.SetEx(ctx, fmt.Sprintf("request:%s", request.RequestID), string(resultJSON), 5*time.Minute)

		log.Printf("Completed request: %s", request.RequestID)
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
