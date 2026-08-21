import asyncio
import json
import subprocess
import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware


# ============================================================
# Configuration
# ============================================================

KAFKA_CONTAINER = "streamforge-kafka"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "truck_telemetry"
KAFKA_GROUP = "streamforge-state-workers"

ROCKSDB_BASE = Path("data/rocksdb")
STATE_SNAPSHOT = Path("data/state_snapshot.json")

PARTITION_COUNT = 20


# ============================================================
# Throughput tracking
# ============================================================

_previous_total_offset = None
_previous_offset_time = None

_ws_previous_total_offset = None
_ws_previous_offset_time = None


# ============================================================
# FastAPI
# ============================================================

app = FastAPI(
    title="StreamForge Topology Monitor",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Get ALL telemetry worker processes
#
# Important:
#
# Each logical StreamForge worker may appear as:
#
# PowerShell
#    └── venv Python worker
#           └── Python311 Kafka consumer
#
# Kafka client ID may contain the CHILD PID.
# We therefore keep the complete process tree.
# ============================================================

def get_all_worker_processes():
    """
    Return all processes whose command line contains
    worker.telemetry_worker.

    Unlike get_worker_status(), this function intentionally
    keeps both parent and child processes.
    """

    processes = []

    try:

        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            (
                "Get-CimInstance Win32_Process | "
                "Where-Object { "
                "$_.CommandLine -and "
                "$_.CommandLine -match 'worker\\.telemetry_worker' "
                "} | "
                "Select-Object ProcessId,ParentProcessId,CommandLine | "
                "ConvertTo-Json -Compress"
            ),
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:
            print(
                f"Worker process detection failed: "
                f"{result.stderr}"
            )
            return []

        output = result.stdout.strip()

        if not output:
            return []

        data = json.loads(output)

        if isinstance(data, dict):
            data = [data]

        for process in data:

            pid = process.get("ProcessId")
            parent_pid = process.get("ParentProcessId")
            command_line = (
                process.get("CommandLine")
                or ""
            )

            if pid is None:
                continue

            try:

                processes.append(
                    {
                        "pid": int(pid),
                        "parent_pid": (
                            int(parent_pid)
                            if parent_pid is not None
                            else None
                        ),
                        "command": command_line,
                    }
                )

            except (
                ValueError,
                TypeError,
            ):

                continue

    except subprocess.TimeoutExpired:

        print(
            "Worker process detection timed out"
        )

    except json.JSONDecodeError as error:

        print(
            f"Worker process JSON error: {error}"
        )

    except Exception as error:

        print(
            f"Worker process detection error: {error}"
        )

    return processes


# ============================================================
# Resolve a process to the logical StreamForge worker
# ============================================================

def resolve_worker_root_pid(
    pid,
    process_map,
):
    """
    Resolve a Kafka consumer PID to the top-level
    StreamForge worker PID.

    Example:

        Kafka client PID = 19136

        19136
           ↓ parent
         840
           ↓ parent
        19688

    The logical StreamForge worker is 840 because
    it is the venv Python process running:

        python.exe -m worker.telemetry_worker
    """

    try:
        current_pid = int(pid)
    except (
        ValueError,
        TypeError,
    ):
        return None

    visited = set()

    while current_pid not in visited:

        visited.add(current_pid)

        process = process_map.get(
            current_pid
        )

        if process is None:
            return None

        command = (
            process.get("command")
            or ""
        )

        # The actual StreamForge worker is the process
        # running from the project's venv.
        #
        # Example:
        #
        # E:\stream-forge\venv\Scripts\python.exe
        #
        if (
            "worker.telemetry_worker"
            in command
            and "\\venv\\Scripts\\python.exe"
            in command.lower()
        ):

            return current_pid

        parent_pid = process.get(
            "parent_pid"
        )

        if parent_pid is None:
            return None

        try:
            current_pid = int(
                parent_pid
            )
        except (
            ValueError,
            TypeError,
        ):
            return None

    return None


# ============================================================
# Worker status
# ============================================================

# ============================================================
# Worker status
# ============================================================

def get_worker_status():
    """
    Detect the actual telemetry worker Python processes.

    StreamForge currently launches workers in a two-level process
    structure:

        venv Python
            |
            +-- Python 3.11 worker.telemetry_worker

    The child Python process is the actual Kafka consumer and its
    PID appears in the Kafka client ID:

        worker-9880

    Therefore we keep the LEAF telemetry_worker processes rather
    than removing all children.
    """

    workers = []

    try:

        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            (
                "Get-CimInstance Win32_Process "
                "-Filter \"Name='python.exe'\" | "
                "Select-Object ProcessId,ParentProcessId,CommandLine | "
                "ConvertTo-Json -Compress"
            ),
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:

            print(
                f"Worker detection failed: {result.stderr}"
            )

            return []

        output = result.stdout.strip()

        if not output:
            return []

        data = json.loads(output)

        if isinstance(data, dict):
            data = [data]

        # ----------------------------------------------------
        # Collect every telemetry_worker process
        # ----------------------------------------------------

        candidates = []

        for process in data:

            pid = process.get("ProcessId")

            parent_pid = process.get(
                "ParentProcessId"
            )

            command_line = (
                process.get("CommandLine")
                or ""
            )

            if (
                "worker.telemetry_worker"
                not in command_line
            ):
                continue

            if pid is None:
                continue

            try:

                pid = int(pid)

                parent_pid = (
                    int(parent_pid)
                    if parent_pid is not None
                    else None
                )

            except (
                ValueError,
                TypeError,
            ):

                continue

            candidates.append(
                {
                    "pid": pid,
                    "parent_pid": parent_pid,
                    "command": command_line,
                }
            )

        if not candidates:
            return []

        # ----------------------------------------------------
        # Build PID set
        # ----------------------------------------------------

        candidate_pids = {
            worker["pid"]
            for worker in candidates
        }

        # ----------------------------------------------------
        # Detect leaf workers
        #
        # A process is a leaf worker if no other
        # telemetry_worker process has it as parent.
        #
        # Example:
        #
        # 840
        #   └── 19136
        #
        # 840 is parent.
        # 19136 is leaf.
        #
        # We want 19136.
        # ----------------------------------------------------

        parent_pids = {
            worker["parent_pid"]
            for worker in candidates
            if worker["parent_pid"] is not None
        }

        leaf_workers = [
            worker
            for worker in candidates
            if worker["pid"] not in parent_pids
        ]

        # ----------------------------------------------------
        # If no leaf workers are detected, fall back to all
        # candidates instead of returning an empty list.
        # ----------------------------------------------------

        if not leaf_workers:
            leaf_workers = candidates

        # ----------------------------------------------------
        # Return actual Kafka worker processes
        # ----------------------------------------------------

        for worker in leaf_workers:

            workers.append(
                {
                    "pid": worker["pid"],
                    "command": worker["command"],
                }
            )

    except subprocess.TimeoutExpired:

        print(
            "Worker detection timed out"
        )

    except json.JSONDecodeError as error:

        print(
            f"Worker detection JSON error: {error}"
        )

    except Exception as error:

        print(
            f"Worker detection error: {error}"
        )

    # --------------------------------------------------------
    # Remove duplicate PIDs
    # --------------------------------------------------------

    unique_workers = {}

    for worker in workers:

        pid = worker["pid"]

        unique_workers[pid] = worker

    workers = list(
        unique_workers.values()
    )

    workers.sort(
        key=lambda worker: worker["pid"]
    )

    return workers


# ============================================================
# Kafka information
# ============================================================

def get_kafka_partitions():

    try:

        command = [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/opt/kafka/bin/"
            "kafka-consumer-groups.sh",
            "--bootstrap-server",
            KAFKA_BOOTSTRAP,
            "--describe",
            "--group",
            KAFKA_GROUP,
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:
            return []

        partitions = []

        for line in result.stdout.splitlines():

            line = line.strip()

            if not line:
                continue

            if line.startswith(
                "GROUP"
            ):
                continue

            if line.startswith(
                "Warning"
            ):
                continue

            parts = line.split()

            if len(parts) < 8:
                continue

            try:

                partition = int(
                    parts[2]
                )

                current_offset = int(
                    parts[3]
                )

                log_end_offset = int(
                    parts[4]
                )

                lag = int(
                    parts[5]
                )

                consumer_id = parts[6]
                host = parts[7]

                client_id = (
                    parts[8]
                    if len(parts) > 8
                    else "-"
                )

                partitions.append(
                    {
                        "partition": partition,
                        "current_offset": current_offset,
                        "log_end_offset": log_end_offset,
                        "lag": lag,
                        "consumer_id": consumer_id,
                        "host": host,
                        "client_id": client_id,
                    }
                )

            except (
                ValueError,
                IndexError,
            ):

                continue

        partitions.sort(
            key=lambda x: x["partition"]
        )

        return partitions

    except subprocess.TimeoutExpired:

        print(
            "Kafka partition query timed out"
        )

        return []

    except Exception as error:

        print(
            f"Kafka partition error: {error}"
        )

        return []


# ============================================================
# RocksDB information
# ============================================================

def get_rocksdb_info():

    result = []

    existing_partitions = set()

    if ROCKSDB_BASE.exists():

        for path in ROCKSDB_BASE.glob(
            "partition_*"
        ):

            try:

                partition_number = int(
                    path.name.split("_")[-1]
                )

                existing_partitions.add(
                    partition_number
                )

            except ValueError:

                continue

    max_partition = max(
        PARTITION_COUNT - 1,
        max(
            existing_partitions,
            default=-1,
        ),
    )

    for partition in range(
        max_partition + 1
    ):

        path = (
            ROCKSDB_BASE
            / f"partition_{partition}"
        )

        records = 0

        if path.exists():

            try:

                records = len(
                    list(
                        path.glob("*.sst")
                    )
                )

            except Exception:

                records = 0

        result.append(
            {
                "partition": partition,
                "path": str(path),
                "exists": path.exists(),
                "sst_files": records,
            }
        )

    return result


# ============================================================
# Extract PID from Kafka client ID
# ============================================================

def get_worker_pid_from_client_id(
    client_id
):
    """
    Kafka client IDs look like:

        worker-9880

    The PID may belong to the child Kafka consumer.
    We resolve it to the parent logical worker later.
    """

    if not client_id:
        return None

    if client_id == "-":
        return None

    if not client_id.startswith(
        "worker-"
    ):
        return None

    try:

        pid_text = client_id.replace(
            "worker-",
            "",
            1,
        )

        pid_text = pid_text.split(
            "-",
            1,
        )[0]

        return int(pid_text)

    except (
        ValueError,
        TypeError,
    ):

        return None


# ============================================================
# Build ACTUAL Kafka → Worker mapping
# ============================================================

def build_worker_mapping(
    partitions,
    workers,
):
    """
    Build:

        Kafka partition
             ↓
        Kafka client PID
             ↓
        logical StreamForge worker PID

    This handles the two-process worker architecture.
    """

    if not partitions or not workers:
        return {}

    # --------------------------------------------------------
    # Get the complete process tree again.
    # --------------------------------------------------------

    all_processes = (
        get_all_worker_processes()
    )

    process_map = {
        process["pid"]: process
        for process in all_processes
    }

    # --------------------------------------------------------
    # Known logical worker PIDs
    # --------------------------------------------------------

    worker_pids = set()

    for worker in workers:

        try:

            worker_pids.add(
                int(worker["pid"])
            )

        except (
            ValueError,
            TypeError,
            KeyError,
        ):

            continue

    mapping = {}

    # --------------------------------------------------------
    # Match each Kafka assignment
    # --------------------------------------------------------

    for partition in partitions:

        consumer_id = (
            partition.get(
                "consumer_id",
                "-",
            )
        )

        client_id = (
            partition.get(
                "client_id",
                "-",
            )
        )

        child_pid = (
            get_worker_pid_from_client_id(
                client_id
            )
        )

        if child_pid is None:
            continue

        # ----------------------------------------------------
        # Case 1:
        # Kafka PID itself is a logical worker.
        # ----------------------------------------------------

        if child_pid in worker_pids:

            logical_pid = child_pid

        else:

            # ------------------------------------------------
            # Case 2:
            # Kafka PID belongs to the child process.
            #
            # Resolve:
            #
            # child PID → parent → worker PID
            # ------------------------------------------------

            logical_pid = (
                resolve_worker_root_pid(
                    child_pid,
                    process_map,
                )
            )

        if logical_pid is None:
            continue

        if logical_pid not in worker_pids:
            continue

        mapping[consumer_id] = {
            "pid": logical_pid,
            "client_pid": child_pid,
            "worker": next(
                (
                    worker
                    for worker in workers
                    if int(
                        worker["pid"]
                    ) == logical_pid
                ),
                None,
            ),
        }

    return mapping


# ============================================================
# Get worker assigned to partition
# ============================================================

def get_partition_worker(
    partition,
    worker_mapping,
):

    consumer_id = (
        partition.get(
            "consumer_id",
            "-",
        )
    )

    return worker_mapping.get(
        consumer_id
    )


# ============================================================
# Topology
# ============================================================

@app.get("/api/topology")
async def topology():

    partitions = (
        get_kafka_partitions()
    )

    rocksdb = (
        get_rocksdb_info()
    )

    workers = (
        get_worker_status()
    )

    worker_mapping = (
        build_worker_mapping(
            partitions,
            workers,
        )
    )

    nodes = []
    edges = []

    # ========================================================
    # Kafka
    # ========================================================

    nodes.append(
        {
            "id": "kafka",

            "position": {
                "x": 500,
                "y": 0,
            },

            "data": {
                "label": (
                    "Kafka\n"
                    "truck_telemetry\n"
                    f"{PARTITION_COUNT} Partitions"
                )
            },
        }
    )

    # ========================================================
    # Partitions
    # ========================================================

    for partition in partitions:

        partition_number = (
            partition["partition"]
        )

        lag = partition["lag"]

        consumer_id = (
            partition.get(
                "consumer_id",
                "-",
            )
        )

        client_id = (
            partition.get(
                "client_id",
                "-",
            )
        )

        worker_info = (
            get_partition_worker(
                partition,
                worker_mapping,
            )
        )

        if worker_info:

            worker_pid = (
                worker_info["pid"]
            )

            assignment = (
                f"Worker PID {worker_pid}"
            )

        elif consumer_id == "-":

            assignment = "Not Assigned"

        else:

            assignment = (
                f"Consumer {client_id}"
            )

        nodes.append(
            {
                "id": (
                    f"p{partition_number}"
                ),

                "position": {
                    "x": (
                        80
                        + (
                            partition_number
                            % 10
                        ) * 180
                    ),

                    "y": (
                        150
                        + (
                            partition_number
                            // 10
                        ) * 120
                    ),
                },

                "data": {
                    "label": (
                        f"Partition {partition_number}\n"
                        f"Lag: {lag}\n"
                        f"{assignment}"
                    )
                },
            }
        )

        edges.append(
            {
                "id": (
                    f"kafka-p"
                    f"{partition_number}"
                ),

                "source": "kafka",

                "target": (
                    f"p{partition_number}"
                ),

                "animated": True,
            }
        )

    # ========================================================
    # Workers
    # ========================================================

    assigned_pids = []

    for info in worker_mapping.values():

        pid = info["pid"]

        if pid not in assigned_pids:
            assigned_pids.append(pid)

    # Add workers with no current Kafka assignment.
    for worker in workers:

        pid = int(
            worker["pid"]
        )

        if pid not in assigned_pids:
            assigned_pids.append(pid)

    assigned_pids.sort()

    for index, pid in enumerate(
        assigned_pids
    ):

        worker_id = (
            f"worker-pid-{pid}"
        )

        assigned_partitions = [

            p["partition"]

            for p in partitions

            if (
                get_partition_worker(
                    p,
                    worker_mapping,
                )
                and
                get_partition_worker(
                    p,
                    worker_mapping,
                )["pid"]
                == pid
            )
        ]

        partition_text = (
            ", ".join(
                str(p)
                for p in sorted(
                    assigned_partitions
                )
            )
            if assigned_partitions
            else "None"
        )

        nodes.append(
            {
                "id": worker_id,

                "position": {
                    "x": (
                        100
                        + index * 180
                    ),

                    "y": 430,
                },

                "data": {
                    "label": (
                        f"Worker #{index + 1}\n"
                        f"PID: {pid}\n"
                        f"Partitions: "
                        f"{partition_text}\n"
                        f"HEALTHY"
                    )
                },
            }
        )

    # ========================================================
    # Partition → Actual Worker
    # ========================================================

    for partition in partitions:

        partition_number = (
            partition["partition"]
        )

        worker_info = (
            get_partition_worker(
                partition,
                worker_mapping,
            )
        )

        if not worker_info:
            continue

        worker_pid = (
            worker_info["pid"]
        )

        worker_id = (
            f"worker-pid-{worker_pid}"
        )

        edges.append(
            {
                "id": (
                    f"p{partition_number}"
                    f"-{worker_id}"
                ),

                "source": (
                    f"p{partition_number}"
                ),

                "target": worker_id,

                "animated": True,
            }
        )

    # ========================================================
    # RocksDB
    # ========================================================

    nodes.append(
        {
            "id": "rocksdb",

            "position": {
                "x": 500,
                "y": 650,
            },

            "data": {
                "label": (
                    "RocksDB State Store\n"
                    "5-Minute Temperature Windows"
                )
            },
        }
    )

    # ========================================================
    # Worker → RocksDB
    # ========================================================

    for pid in assigned_pids:

        worker_id = (
            f"worker-pid-{pid}"
        )

        edges.append(
            {
                "id": (
                    f"{worker_id}-db"
                ),

                "source": worker_id,

                "target": "rocksdb",

                "animated": True,
            }
        )

    return {
        "nodes": nodes,
        "edges": edges,
        "partitions": partitions,
        "rocksdb": rocksdb,
        "workers": workers,
        "worker_mapping": {
            consumer_id: {
                "pid": info["pid"],
                "client_pid": info[
                    "client_pid"
                ],
            }

            for consumer_id, info
            in worker_mapping.items()
        },
    }


# ============================================================
# API Metrics
# ============================================================

@app.get("/api/metrics")
async def metrics():

    partitions = (
        get_kafka_partitions()
    )

    workers = (
        get_worker_status()
    )

    total_lag = sum(
        p["lag"]
        for p in partitions
    )

    return {

        "workers": len(
            workers
        ),

        "partitions": len(
            partitions
        ),

        "expected_partition_count":
            PARTITION_COUNT,

        "total_lag":
            total_lag,

        "healthy":
            total_lag == 0,
    }


# ============================================================
# Dashboard Throughput Metrics
# ============================================================

@app.get("/metrics")
async def dashboard_metrics():

    global _previous_total_offset
    global _previous_offset_time

    partitions = (
        get_kafka_partitions()
    )

    current_total_offset = sum(
        partition[
            "log_end_offset"
        ]
        for partition in partitions
    )

    current_time = time.time()

    throughput = 0.0

    if (
        _previous_total_offset is not None
        and _previous_offset_time is not None
    ):

        elapsed = (
            current_time
            - _previous_offset_time
        )

        offset_delta = (
            current_total_offset
            - _previous_total_offset
        )

        if (
            elapsed > 0
            and offset_delta >= 0
        ):

            throughput = (
                offset_delta
                / elapsed
            )

    _previous_total_offset = (
        current_total_offset
    )

    _previous_offset_time = (
        current_time
    )

    total_lag = sum(
        partition["lag"]
        for partition in partitions
    )

    return {

        "throughput": round(
            throughput,
            2,
        ),

        "total_events_sec": round(
            throughput,
            2,
        ),

        "total_lag":
            total_lag,

        "partitions":
            len(partitions),

        "expected_partition_count":
            PARTITION_COUNT,

        "timestamp":
            current_time,
    }


# ============================================================
# WebSocket Metrics
# ============================================================

@app.websocket("/ws/metrics")
async def websocket_metrics(
    websocket: WebSocket
):

    global _ws_previous_total_offset
    global _ws_previous_offset_time

    await websocket.accept()

    print(
        "Metrics WebSocket connected"
    )

    try:

        while True:

            partitions = (
                get_kafka_partitions()
            )

            workers = (
                get_worker_status()
            )

            total_lag = sum(
                p["lag"]
                for p in partitions
            )

            # =================================================
            # Throughput
            # =================================================

            current_total_offset = sum(
                partition[
                    "log_end_offset"
                ]
                for partition in partitions
            )

            current_time = time.time()

            throughput = 0.0

            if (
                _ws_previous_total_offset
                is not None
                and _ws_previous_offset_time
                is not None
            ):

                elapsed = (
                    current_time
                    - _ws_previous_offset_time
                )

                offset_delta = (
                    current_total_offset
                    - _ws_previous_total_offset
                )

                if (
                    elapsed > 0
                    and offset_delta >= 0
                ):

                    throughput = (
                        offset_delta
                        / elapsed
                    )

            _ws_previous_total_offset = (
                current_total_offset
            )

            _ws_previous_offset_time = (
                current_time
            )

            # =================================================
            # Worker mapping
            # =================================================

            worker_mapping = (
                build_worker_mapping(
                    partitions,
                    workers,
                )
            )

            # =================================================
            # Main payload
            # =================================================

            payload = {

                "worker_count":
                    len(workers),

                "partition_count":
                    len(partitions),

                "expected_partition_count":
                    PARTITION_COUNT,

                "total_lag":
                    total_lag,

                "total_events_sec":
                    round(
                        throughput,
                        2,
                    ),

                "throughput":
                    round(
                        throughput,
                        2,
                    ),

                "status":
                    (
                        "HEALTHY"
                        if total_lag == 0
                        else "CATCHING UP"
                    ),
            }

            # =================================================
            # Individual worker metrics
            # =================================================

            for worker in workers:

                worker_pid = int(
                    worker["pid"]
                )

                worker_id = (
                    f"worker-pid-{worker_pid}"
                )

                assigned_partitions = []

                for partition in partitions:

                    worker_info = (
                        get_partition_worker(
                            partition,
                            worker_mapping,
                        )
                    )

                    if not worker_info:
                        continue

                    if (
                        worker_info["pid"]
                        == worker_pid
                    ):

                        assigned_partitions.append(
                            partition[
                                "partition"
                            ]
                        )

                worker_lag = sum(

                    partition["lag"]

                    for partition
                    in partitions

                    if (
                        partition[
                            "partition"
                        ]
                        in assigned_partitions
                    )
                )

                worker_partition_count = (
                    len(
                        assigned_partitions
                    )
                )

                worker_throughput = 0.0

                if len(partitions) > 0:

                    worker_throughput = (
                        throughput
                        * worker_partition_count
                        / len(partitions)
                    )

                payload[worker_id] = {

                    "status":
                        (
                            "HEALTHY"
                            if worker_lag == 0
                            else "CATCHING UP"
                        ),

                    "throughput":
                        round(
                            worker_throughput,
                            2,
                        ),

                    "lag":
                        worker_lag,

                    "pid":
                        worker_pid,

                    "partition_count":
                        worker_partition_count,

                    "partitions":
                        sorted(
                            assigned_partitions
                        ),
                }

            await websocket.send_json(
                payload
            )

            await asyncio.sleep(2)

    except WebSocketDisconnect:

        print(
            "Metrics WebSocket disconnected"
        )

    except Exception as error:

        print(
            f"Metrics WebSocket error: {error}"
        )


# ============================================================
# Dashboard Summary API
# ============================================================

@app.get("/summary")
async def summary():

    partitions = (
        get_kafka_partitions()
    )

    workers = (
        get_worker_status()
    )

    total_lag = sum(
        p["lag"]
        for p in partitions
    )

    # ========================================================
    # Read state snapshot
    # ========================================================

    total_trucks = 0
    total_readings = 0
    temperature_sum = 0.0

    try:

        if STATE_SNAPSHOT.exists():

            with open(
                STATE_SNAPSHOT,
                "r",
                encoding="utf-8",
            ) as f:

                state = json.load(f)

            total_trucks = len(
                state
            )

            for truck in state.values():

                readings = int(
                    truck.get(
                        "readings",
                        0,
                    )
                )

                avg_temperature = float(
                    truck.get(
                        "avg_temperature",
                        0,
                    )
                )

                total_readings += (
                    readings
                )

                temperature_sum += (
                    avg_temperature
                    * readings
                )

    except Exception as error:

        print(
            f"Summary state error: {error}"
        )

    # ========================================================
    # Weighted average temperature
    # ========================================================

    avg_temperature = 0.0

    if total_readings > 0:

        avg_temperature = round(

            temperature_sum
            / total_readings,

            2,
        )

    # ========================================================
    # Alerts
    # ========================================================

    active_alerts = sum(

        1

        for partition in partitions

        if partition["lag"] > 0
    )

    # ========================================================
    # System status
    # ========================================================

    status = (
        "HEALTHY"
        if total_lag == 0
        else "CATCHING UP"
    )

    return {

        "total_trucks":
            total_trucks,

        "active_workers":
            len(workers),

        "total_readings":
            total_readings,

        "avg_temperature":
            avg_temperature,

        "active_alerts":
            active_alerts,

        "total_lag":
            total_lag,

        "status":
            status,

        "topic":
            KAFKA_TOPIC,

        "consumer_group":
            KAFKA_GROUP,

        "partition_count":
            len(partitions),

        "expected_partition_count":
            PARTITION_COUNT,
    }


# ============================================================
# Workers API
# ============================================================

@app.get("/workers")
async def workers():

    worker_list = (
        get_worker_status()
    )

    partitions = (
        get_kafka_partitions()
    )

    worker_mapping = (
        build_worker_mapping(
            partitions,
            worker_list,
        )
    )

    result = []

    for index, worker in enumerate(
        worker_list
    ):

        pid = int(
            worker["pid"]
        )

        assigned_partitions = []

        for partition in partitions:

            worker_info = (
                get_partition_worker(
                    partition,
                    worker_mapping,
                )
            )

            if not worker_info:
                continue

            if worker_info["pid"] == pid:

                assigned_partitions.append(
                    partition["partition"]
                )

        worker_lag = sum(

            partition["lag"]

            for partition in partitions

            if (
                partition["partition"]
                in assigned_partitions
            )
        )

        worker_partition_count = (
            len(
                assigned_partitions
            )
        )

        worker_status = (
            "HEALTHY"
            if worker_lag == 0
            else "CATCHING UP"
        )

        result.append(
            {
                "id": index + 1,

                "pid": pid,

                "status":
                    worker_status,

                "partitions":
                    sorted(
                        assigned_partitions
                    ),

                "partition_count":
                    worker_partition_count,

                "lag":
                    worker_lag,

                "command":
                    worker["command"],
            }
        )

    return {

        "workers":
            result,

        "total_workers":
            len(result),

        "total_partitions":
            len(partitions),

        "expected_partition_count":
            PARTITION_COUNT,
    }


# ============================================================
# State API
# ============================================================

@app.get("/state")
async def state():

    if not STATE_SNAPSHOT.exists():

        return {

            "trucks": {},

            "total_trucks": 0,

            "total_readings": 0,

            "avg_temperature": 0,
        }

    try:

        with open(
            STATE_SNAPSHOT,
            "r",
            encoding="utf-8",
        ) as f:

            snapshot = json.load(f)

        total_trucks = len(
            snapshot
        )

        total_readings = sum(

            int(
                truck.get(
                    "readings",
                    0,
                )
            )

            for truck
            in snapshot.values()
        )

        if total_readings > 0:

            avg_temperature = round(

                sum(

                    float(
                        truck.get(
                            "avg_temperature",
                            0,
                        )
                    )
                    *
                    int(
                        truck.get(
                            "readings",
                            0,
                        )
                    )

                    for truck
                    in snapshot.values()

                )
                / total_readings,

                2,
            )

        else:

            avg_temperature = 0

        return {

            "trucks":
                snapshot,

            "total_trucks":
                total_trucks,

            "total_readings":
                total_readings,

            "avg_temperature":
                avg_temperature,
        }

    except Exception as error:

        return {

            "error":
                str(error),

            "trucks": {},

            "total_trucks": 0,

            "total_readings": 0,

            "avg_temperature": 0,
        }