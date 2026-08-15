from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "StreamForge Dashboard API"}


@app.get("/metrics")
def metrics():
    return {
        "throughput": 12450,
        "active_workers": 3,
        "failed_events": 1
    }


@app.get("/workers")
def workers():
    return [
        {
            "id": "worker1",
            "status": "active",
            "partition": "partition0"
        },
        {
            "id": "worker2",
            "status": "active",
            "partition": "partition1"
        },
        {
            "id": "worker3",
            "status": "active",
            "partition": "partition2"
        }
    ]