# Distributed AI Inference Platform

A backend for serving ML inference that doesn't fall over when traffic spikes or a worker dies. Requests come in through a FastAPI gateway, get queued in Kafka, and a pool of Go workers picks them up and runs them. Results land in Redis and you poll for them by ID.

## Overview
The whole idea is to not call the model synchronously on every request. If that happens and one burst of traffic either melts the model or drops requests on the floor, and if a worker crashes halfway through a job, that job is gone. 

Hence, there is a queue in the middle instead. The gateway takes your request, hands back an ID right away, and moves on. Workers pull from the queue whenever they're free, so a spike just makes the queue longer instead of taking anything down, and a worker that dies mid-job never acked its message, so Kafka gives it to another worker. 

It doesn't care what model sits behind it. Right now the worker just echoes the prompt back, because I wanted the infrastructure solid before dropping a real model in. That swap is on the roadmap below.

## Architecture

[Coming soon - architecture diagram]

1. You POST a prompt to /infer.
2. Gateway makes a request_id, drops the request on Kafka, writes a   pending entry to Redis, and returns the ID.
3. A worker reads the request off Kafka, runs it, and writes the result back to Redis.
4. You poll /result/{request_id} until it's done.

## Why this design

- **Kafka in the middle** keeps the gateway from blocking on inference, lets bursts pile up in the queue instead of taking things down, and gives durable delivery so a worker crash doesn't lose the request.
- **Go workers** are cheap to spin up and easy to run a bunch of, so scaling out is just starting more of them.
- **Redis for results** gives fast reads for polling, and a TTL means old results clean themselves up instead of piling up forever.
- **Shared consumer group** over a partitioned topic lets Kafka split partitions across workers, so they process in parallel and throughput scales with worker count.

## Tech stack
 
Gateway: Python, FastAPI. Queue: Kafka. Workers: Go. Results: Redis. Orchestration: Docker Compose. On the roadmap: Kubernetes, Terraform, Grafana.
 
## Status
 
**Working**
- Full path from gateway through Kafka and the worker to Redis and back
- Gateway endpoints: `/infer`, `/result/{id}`, `/health`
- Go worker consuming from Kafka and writing results to Redis
- Kafka, Zookeeper, and Redis running locally through Docker Compose
**Building next**
- Manual Kafka offset commits, so a message only gets acked after Redis actually has the result
- Retry logic in the worker with a cap, so a bad message doesn't loop forever
- A health endpoint on the worker
- More than one partition plus the shared consumer group, so workers run in parallel for real
**Later**
- A real model in place of the placeholder (local Llama or Mistral through vLLM or Ollama, or just proxy an API)
- An MCP server so the platform shows up as a tool in MCP clients
- Kubernetes manifests and Terraform to deploy it
- Grafana dashboards for queue depth, worker load, and latency
## Running locally
 
You'll need Docker, Python 3.12+, and Go 1.22+.
 
1. Start Kafka, Zookeeper, and Redis:
```bash
   docker compose up -d
```
2. Start the gateway:
```bash
   cd gateway
   python3 -m venv venv && source venv/bin/activate
   pip install -r requirements.txt
   python3 main.py
```
3. Start a worker in a second terminal:
```bash
   cd worker
   go run main.go
```
4. Send a request:
```bash
   curl -X POST http://localhost:8000/infer \
     -H "Content-Type: application/json" \
     -d '{"prompt": "What is the capital of France?"}'
```
5. Grab the result with the ID it hands back:
```bash
   curl http://localhost:8000/result/<request_id>
```
 
## Project structure
 
```
distributed-ai-inference-platform/
├── gateway/            FastAPI gateway (Python)
├── worker/             inference worker (Go)
├── infra/
│   ├── k8s/            Kubernetes manifests (later)
│   └── terraform/      Terraform (later)
├── frontend/           UI (later)
├── docker-compose.yml  Kafka, Zookeeper, Redis
└── README.md
```
