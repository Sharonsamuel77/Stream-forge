import asyncio
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="StreamForge Topology Monitor")

app.add_middleware(
    CORSMiddleware,
   


INITIAL_TOPOLOGY = {
    "nodes": [
        {
            "id": "kafka-ingest",
            "type": "input",
            "data": {"label": "Kafka Topic: truck-telemetry\n(Partitions: 20)"},
            "position": {"x": 50, "y": 150},
            "style": {"background": "#2563eb", "color": "#fff", "borderRadius": "8px", "padding": "10px"}
        },
        {
            "id": "worker-1",
            "data": {"label": "Worker Node #1\nStatus: HEALTHY\nThroughput: 5,200 msg/s"},
            "position": {"x": 350, "y": 50},
            "style": {"background": "#16a34a", "color": "#fff", "borderRadius": "8px", "padding": "10px"}
        },
        {
            "id": "worker-2",
            "data": {"label": "Worker Node #2\nStatus: HEALTHY\nThroughput: 4,950 msg/s"},
            "position": {"x": 350, "y": 250},
            "style": {"background": "#16a34a", "color": "#fff", "borderRadius": "8px", "padding": "10px"}
        },
        {
            "id": "rocksdb-store",
            "data": {"label": "RocksDB State Store\nWindow: 5-Min Rolling Avg"},
            "position": {"x": 650, "y": 150},
            "style": {"background": "#9333ea", "color": "#fff", "borderRadius": "8px", "padding": "10px"}
        },
        {
            "id": "changelog-topic",
            "type": "output",
            "data": {"label": "Kafka Changelog Topic\ntruck-telemetry-changelog"},
            "position": {"x": 950, "y": 150},
            "style": {"background": "#dc2626", "color": "#fff", "borderRadius": "8px", "padding": "10px"}
        }
    ],
    "edges": [
        {"id": "e1-w1", "source": "kafka-ingest", "target": "worker-1", "animated": True},
        {"id": "e1-w2", "source": "kafka-ingest", "target": "worker-2", "animated": True},
        {"id": "w1-db", "source": "worker-1", "target": "rocksdb-store", "animated": True},
        {"id": "w2-db", "source": "worker-2", "target": "rocksdb-store", "animated": True},
        {"id": "db-cl", "source": "rocksdb-store", "target": "changelog-topic", "animated": True}
    ]
}

@app.get("/api/topology")
async def get_topology():
    return INITIAL_TOPOLOGY

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Simulate real-time Prometheus / Faust telemetry metrics
            metrics = {
                "worker-1": {
                    "throughput": random.randint(4800, 5300),
                    "lag": random.randint(0, 50),
                    "status": "HEALTHY"
                },
                "worker-2": {
                    "throughput": random.randint(4700, 5100),
                    "lag": random.randint(0, 120),
                    "status": "HEALTHY" if random.random() > 0.1 else "DEGRADED"
                },
                "total_events_sec": random.randint(95000, 105000)
            }
            await websocket.send_json(metrics)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

