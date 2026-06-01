from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import redis
import json
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="AI Inference Gateway")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Redis client
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

class InferenceRequest(BaseModel):
    prompt: str
    model: str = "default"

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "gateway",
        "kafka": "connected",
        "redis": "connected"
    }

@app.post("/infer")
def infer(request: InferenceRequest):
    request_id = str(uuid.uuid4())
    
    message = {
        "request_id": request_id,
        "prompt": request.prompt,
        "model": request.model
    }
    
    # Send to Kafka
    producer.send("inference-requests", message)
    producer.flush()
    
    # Store pending status in Redis
    redis_client.setex(
        f"request:{request_id}",
        300,  # expire in 5 minutes
        json.dumps({"status": "pending", "prompt": request.prompt})
    )
    
    return {
        "request_id": request_id,
        "status": "queued",
        "message": "Request sent to inference workers"
    }

@app.get("/result/{request_id}")
def get_result(request_id: str):
    result = redis_client.get(f"request:{request_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Request not found")
    return json.loads(result)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)