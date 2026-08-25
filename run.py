import os
import sys
import time
import signal
import subprocess
import webbrowser
from pathlib import Path


# ============================================================
# STREAM FORGE - COMPLETE RUNNER
# ============================================================

ROOT = Path(__file__).resolve().parent

PYTHON = ROOT / "venv" / "Scripts" / "python.exe"

KAFKA_CONTAINER = "streamforge-kafka"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "truck_telemetry"
KAFKA_GROUP = "streamforge-state-workers"

API_HOST = "127.0.0.1"
API_PORT = "8001"

FRONTEND_DIR = ROOT / "dashboard" / "frontend"
FRONTEND_URL = "http://127.0.0.1:5173"

MAX_WORKERS = 20

processes = []
shutdown_started = False


# ============================================================
# HEADER
# ============================================================

def print_header():
    print()
    print("=" * 70)
    print("                    STREAM FORGE")
    print("             Distributed Event Processor")
    print("=" * 70)
    print()


# ============================================================
# COMMAND HELPER
# ============================================================

def run_command(command, cwd=None):
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )


# ============================================================
# START LONG RUNNING PROCESS
# ============================================================

def start_process(command, cwd=None, name="process"):

    print(f"[START] {name}")

    creationflags = 0

    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    process = subprocess.Popen(
        command,
        cwd=cwd,
        creationflags=creationflags,
    )

    processes.append((name, process))

    return process


# ============================================================
# DOCKER START
# ============================================================

def start_docker():

    print()
    print("[1/6] Starting Docker services...")
    print()

    result = run_command(
        [
            "docker",
            "compose",
            "up",
            "-d",
        ],
        cwd=ROOT,
    )

    if result.returncode != 0:
        print("ERROR: Docker Compose failed.")
        print(result.stderr)
        sys.exit(1)

    print(result.stdout)

    print("Waiting for Kafka to start...")
    time.sleep(5)

    result = run_command(
        [
            "docker",
            "inspect",
            "-f",
            "{{.State.Running}}",
            KAFKA_CONTAINER,
        ]
    )

    if result.returncode != 0:
        print("ERROR: Could not inspect Kafka container.")
        print(result.stderr)
        sys.exit(1)

    if result.stdout.strip().lower() != "true":
        print("ERROR: Kafka container is not running.")
        sys.exit(1)

    print("[OK] Kafka is running.")


# ============================================================
# CHECK EXISTING TOPIC
# ============================================================

def check_topic():

    print()
    print("[2/6] Checking existing Kafka topic...")
    print()

    print(f"Topic: {KAFKA_TOPIC}")

    result = run_command(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            KAFKA_BOOTSTRAP,
            "--describe",
            "--topic",
            KAFKA_TOPIC,
        ]
    )

    if result.returncode != 0:

        print()
        print("=" * 70)
        print("ERROR: EXISTING KAFKA TOPIC NOT FOUND")
        print("=" * 70)
        print()

        print(
            f"run.py will NOT create or recreate '{KAFKA_TOPIC}'."
        )

        print()
        print(result.stderr)

        sys.exit(1)

    print(result.stdout)

    partition_count = None

    for line in result.stdout.splitlines():

        if "PartitionCount:" in line:

            try:
                partition_count = int(
                    line.split("PartitionCount:")[1]
                    .split()[0]
                )
            except Exception:
                pass

    if partition_count is not None:

        print()
        print(
            f"[OK] Existing topic found with "
            f"{partition_count} partitions."
        )

        if partition_count != 20:

            print()
            print(
                f"[WARNING] Expected 20 partitions, "
                f"but found {partition_count}."
            )

            print(
                "[WARNING] run.py will NOT modify the topic."
            )

    else:

        print(
            f"[OK] Existing topic '{KAFKA_TOPIC}' found."
        )


# ============================================================
# ASK WORKER COUNT
# ============================================================

def ask_worker_count():

    print()
    print("[3/6] Worker configuration")
    print()

    print(f"Maximum workers: {MAX_WORKERS}")
    print()

    while True:

        value = input(
            "Enter number of workers "
            f"(1-{MAX_WORKERS}) [20]: "
        ).strip()

        if value == "":
            return MAX_WORKERS

        try:

            count = int(value)

            if 1 <= count <= MAX_WORKERS:
                return count

        except ValueError:
            pass

        print()
        print(
            f"Please enter a number between "
            f"1 and {MAX_WORKERS}."
        )


# ============================================================
# START PRODUCER
# ============================================================

def start_producer():

    print()
    print("[4/6] Starting telemetry producer...")
    print()

    return start_process(
        [
            str(PYTHON),
            "-m",
            "producer.telemetry_producer",
        ],
        cwd=ROOT,
        name="Telemetry Producer",
    )


# ============================================================
# START WORKERS
# ============================================================

def start_workers(worker_count):

    print()
    print(
        f"[5/6] Starting {worker_count} worker(s)..."
    )
    print()

    for i in range(worker_count):

        start_process(
            [
                str(PYTHON),
                "-m",
                "worker.telemetry_worker",
            ],
            cwd=ROOT,
            name=f"Worker {i + 1}/{worker_count}",
        )

        time.sleep(0.2)

    print()
    print(
        f"[OK] Started {worker_count} worker process(es)."
    )


# ============================================================
# START FASTAPI
# ============================================================

def start_fastapi():

    print()
    print("[6/6] Starting FastAPI...")
    print()

    return start_process(
        [
            str(PYTHON),
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            API_HOST,
            "--port",
            str(API_PORT),
        ],
        cwd=ROOT,
        name="FastAPI",
    )


# ============================================================
# START FRONTEND
# ============================================================

def start_frontend():

    print()
    print("[FRONTEND] Starting Vite...")
    print()

    if not FRONTEND_DIR.exists():

        print(
            "ERROR: Frontend directory does not exist:"
        )

        print(FRONTEND_DIR)

        return None

    npm_command = (
        "npm.cmd"
        if os.name == "nt"
        else "npm"
    )

    return start_process(
        [
            npm_command,
            "run",
            "dev",
            "--",
            "--host",
            "127.0.0.1",
        ],
        cwd=FRONTEND_DIR,
        name="Vite Frontend",
    )


# ============================================================
# OPEN BROWSER
# ============================================================

def open_browser():

    print()
    print("Waiting for frontend...")
    time.sleep(5)

    print()
    print("=" * 70)
    print("                 STREAM FORGE READY")
    print("=" * 70)
    print()

    print(f"Frontend : {FRONTEND_URL}")
    print(
        f"FastAPI  : http://{API_HOST}:{API_PORT}"
    )
    print(
        f"API Docs : http://{API_HOST}:{API_PORT}/docs"
    )

    print()
    print(
        "Press CTRL+C in this terminal to stop EVERYTHING."
    )
    print()

    try:

        webbrowser.open(FRONTEND_URL)

        print("[OK] Browser opened.")

    except Exception as exc:

        print(
            f"[WARNING] Browser could not be opened: {exc}"
        )

        print(
            f"Open manually: {FRONTEND_URL}"
        )


# ============================================================
# STOP ONE PROCESS
# ============================================================

def stop_process(name, process):

    if process is None:
        return

    if process.poll() is not None:
        return

    print(f"[STOP] {name}")

    try:

        if os.name == "nt":

            try:

                process.send_signal(
                    signal.CTRL_BREAK_EVENT
                )

            except Exception:

                pass

            try:

                process.wait(timeout=5)

            except subprocess.TimeoutExpired:

                print(
                    f"[FORCE STOP] {name}"
                )

                process.kill()

                try:
                    process.wait(timeout=3)
                except Exception:
                    pass

        else:

            process.terminate()

            try:

                process.wait(timeout=5)

            except subprocess.TimeoutExpired:

                process.kill()

    except Exception as exc:

        print(
            f"[WARNING] Error stopping {name}: {exc}"
        )

        try:
            process.kill()
        except Exception:
            pass


# ============================================================
# STOP DOCKER COMPOSE
# ============================================================

def stop_docker():

    print()
    print("[DOCKER] Stopping Docker Compose services...")
    print()

    try:

        result = run_command(
            [
                "docker",
                "compose",
                "down",
            ],
            cwd=ROOT,
        )

        if result.returncode == 0:

            print(result.stdout)

            print(
                "[OK] Docker Compose services stopped."
            )

        else:

            print(
                "[WARNING] Docker Compose shutdown "
                "returned an error."
            )

            print(result.stderr)

    except Exception as exc:

        print(
            f"[WARNING] Docker shutdown failed: {exc}"
        )


# ============================================================
# FULL SHUTDOWN
# ============================================================

def shutdown():

    global shutdown_started

    if shutdown_started:
        return

    shutdown_started = True

    print()
    print()
    print("=" * 70)
    print("              STOPPING STREAM FORGE")
    print("=" * 70)
    print()

    # --------------------------------------------------------
    # Stop Python / Node processes first.
    # --------------------------------------------------------

    for name, process in reversed(processes):

        stop_process(
            name,
            process
        )

    processes.clear()

    # --------------------------------------------------------
    # Then stop Docker Compose.
    #
    # This stops:
    #   Kafka
    #   Prometheus
    #
    # It does NOT run:
    #   kafka-topics --delete
    #   kafka-topics --create
    #   kafka-storage format
    #
    # Therefore run.py does not intentionally delete/recreate
    # the truck_telemetry topic.
    # --------------------------------------------------------

    stop_docker()

    print()
    print("=" * 70)
    print("             STREAM FORGE STOPPED")
    print("=" * 70)
    print()


# ============================================================
# SIGNAL HANDLERS
# ============================================================

def handle_sigint(signum, frame):

    shutdown()

    raise SystemExit(0)


def handle_sigterm(signum, frame):

    shutdown()

    raise SystemExit(0)


# ============================================================
# MAIN
# ============================================================

def main():

    if not PYTHON.exists():

        print()
        print(
            "ERROR: Virtual environment Python not found:"
        )

        print(PYTHON)

        print()
        sys.exit(1)

    signal.signal(
        signal.SIGINT,
        handle_sigint
    )

    if hasattr(signal, "SIGTERM"):

        signal.signal(
            signal.SIGTERM,
            handle_sigterm
        )

    print_header()

    try:

        # ----------------------------------------------------
        # 1. Docker
        # ----------------------------------------------------

        start_docker()

        # ----------------------------------------------------
        # 2. Existing topic
        # ----------------------------------------------------

        check_topic()

        # ----------------------------------------------------
        # 3. Worker count
        # ----------------------------------------------------

        worker_count = ask_worker_count()

        # ----------------------------------------------------
        # 4. Producer
        # ----------------------------------------------------

        start_producer()

        time.sleep(2)

        # ----------------------------------------------------
        # 5. Workers
        # ----------------------------------------------------

        start_workers(worker_count)

        time.sleep(3)

        # ----------------------------------------------------
        # 6. FastAPI
        # ----------------------------------------------------

        start_fastapi()

        time.sleep(3)

        # ----------------------------------------------------
        # 7. Frontend
        # ----------------------------------------------------

        start_frontend()

        # ----------------------------------------------------
        # 8. Browser
        # ----------------------------------------------------

        open_browser()

        # ----------------------------------------------------
        # Keep runner alive.
        # ----------------------------------------------------

        while True:

            time.sleep(1)

            # Check whether one of our processes died.
            for name, process in list(processes):

                return_code = process.poll()

                if return_code is not None:

                    print()
                    print(
                        f"[WARNING] {name} stopped."
                    )

                    print(
                        f"Exit code: {return_code}"
                    )

                    # Remove dead process.
                    processes.remove(
                        (name, process)
                    )

            # If all application processes died,
            # initiate full cleanup.
            if not processes:

                print()
                print(
                    "[WARNING] All application processes "
                    "have stopped."
                )

                shutdown()

                break

    except KeyboardInterrupt:

        shutdown()

    except Exception as exc:

        print()
        print("=" * 70)
        print("ERROR")
        print("=" * 70)
        print()
        print(exc)
        print()

        shutdown()

        sys.exit(1)

    finally:

        shutdown()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()