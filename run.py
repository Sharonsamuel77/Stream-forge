import os
import sys
import time
import signal
import subprocess
import webbrowser
from pathlib import Path


# ============================================================
# STREAM FORGE - WINDOWS COMPLETE RUNNER
# ============================================================
#
# Starts:
#   1. Docker Compose
#      - Kafka
#      - Prometheus
#   2. Existing Kafka topic check
#   3. Telemetry producer
#   4. Up to 20 workers
#   5. FastAPI
#   6. Vite frontend
#   7. Browser
#
# CTRL+C:
#   - Stops producer process tree
#   - Stops ALL worker process trees
#   - Stops FastAPI process tree
#   - Stops Vite / Node process tree
#   - Runs docker compose down
#
# IMPORTANT:
#   This script DOES NOT create, delete, or recreate Kafka topics.
# ============================================================


# ============================================================
# CONFIGURATION
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


# ============================================================
# GLOBAL PROCESS LIST
# ============================================================

processes = []

shutdown_started = False


# ============================================================
# HEADER
# ============================================================

def print_header():

    print()
    print("=" * 72)
    print("                         STREAM FORGE")
    print("                  Distributed Event Processor")
    print("=" * 72)
    print()

    print(f"Project Root : {ROOT}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Worker Group : {KAFKA_GROUP}")
    print(f"Max Workers  : {MAX_WORKERS}")

    print()


# ============================================================
# COMMAND HELPER
# ============================================================

def run_command(command, cwd=None):

    try:

        return subprocess.run(
            command,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )

    except FileNotFoundError as exc:

        print()
        print("[ERROR] Command not found:")
        print(command[0])
        print()

        raise exc


# ============================================================
# START PROCESS
# ============================================================

def start_process(command, cwd=None, name="process"):

    print(f"[START] {name}")

    creationflags = 0

    if os.name == "nt":

        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
        )

    process = subprocess.Popen(
        command,
        cwd=cwd,
        creationflags=creationflags,
    )

    processes.append(
        {
            "name": name,
            "process": process,
        }
    )

    return process


# ============================================================
# START DOCKER
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

        print()
        print("[ERROR] Docker Compose failed.")
        print()

        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print(result.stderr)

        sys.exit(1)

    if result.stdout:
        print(result.stdout)

    print()
    print("[WAIT] Waiting for Kafka to start...")
    print()

    # Give Kafka some time to initialize.
    time.sleep(5)

    # --------------------------------------------------------
    # Check Kafka container.
    # --------------------------------------------------------

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

        print()
        print("[ERROR] Could not inspect Kafka container.")
        print(result.stderr)

        sys.exit(1)

    if result.stdout.strip().lower() != "true":

        print()
        print("[ERROR] Kafka container is not running.")
        print()

        sys.exit(1)

    print("[OK] Kafka is running.")


# ============================================================
# CHECK EXISTING KAFKA TOPIC
# ============================================================
#
# IMPORTANT:
# This function ONLY DESCRIBES the topic.
#
# It NEVER:
#   kafka-topics --create
#   kafka-topics --delete
#   kafka-topics --alter
#
# Therefore the existing 20-partition topic remains intact.
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
        print("=" * 72)
        print("ERROR: EXISTING KAFKA TOPIC NOT FOUND")
        print("=" * 72)
        print()

        print(
            f"run.py will NOT create or recreate "
            f"'{KAFKA_TOPIC}'."
        )

        print()
        print("Kafka response:")

        if result.stderr:
            print(result.stderr)

        print()

        sys.exit(1)

    if result.stdout:
        print(result.stdout)

    # --------------------------------------------------------
    # Detect partition count.
    # --------------------------------------------------------

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
                "[WARNING] Expected 20 partitions, "
                f"but found {partition_count}."
            )

            print(
                "[WARNING] run.py will NOT modify "
                "the topic."
            )

        else:

            print(
                "[OK] Topic has the expected "
                "20 partitions."
            )

    else:

        print()
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

    print(
        f"Maximum workers allowed: {MAX_WORKERS}"
    )

    print()

    while True:

        try:

            value = input(
                "Enter number of workers "
                f"(1-{MAX_WORKERS}) [20]: "
            ).strip()

        except (EOFError, KeyboardInterrupt):

            print()
            print("[INFO] Worker configuration cancelled.")

            shutdown()

            sys.exit(0)

        if value == "":
            return MAX_WORKERS

        try:

            count = int(value)

            if 1 <= count <= MAX_WORKERS:

                print()
                print(
                    f"[OK] Worker count selected: {count}"
                )

                return count

        except ValueError:
            pass

        print()
        print(
            f"[ERROR] Enter a number between "
            f"1 and {MAX_WORKERS}."
        )

        print()


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

    for index in range(worker_count):

        start_process(
            [
                str(PYTHON),
                "-m",
                "worker.telemetry_worker",
            ],
            cwd=ROOT,
            name=f"Worker {index + 1}/{worker_count}",
        )

        # Small delay prevents all workers from starting
        # simultaneously and makes Kafka group rebalancing
        # more stable during startup.

        time.sleep(0.25)

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
            "[ERROR] Frontend directory does not exist:"
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
# WAIT FOR FRONTEND
# ============================================================

def wait_for_frontend(timeout=30):

    print()
    print("[WAIT] Waiting for Vite frontend...")

    # We don't need requests here.
    # Simply give Vite time to initialize.

    for _ in range(timeout):

        time.sleep(1)

        # Check whether Vite process is still alive.

        for item in processes:

            if item["name"] == "Vite Frontend":

                process = item["process"]

                if process.poll() is not None:

                    print()
                    print(
                        "[ERROR] Vite stopped during startup."
                    )

                    return False

                break

    return True


# ============================================================
# OPEN BROWSER
# ============================================================

def open_browser():

    print()
    print("=" * 72)
    print("                       STREAM FORGE READY")
    print("=" * 72)
    print()

    print(
        f"Frontend : {FRONTEND_URL}"
    )

    print(
        f"FastAPI  : "
        f"http://{API_HOST}:{API_PORT}"
    )

    print(
        f"API Docs : "
        f"http://{API_HOST}:{API_PORT}/docs"
    )

    print()
    print(
        "Press CTRL+C in THIS terminal to stop EVERYTHING."
    )

    print()

    try:

        opened = webbrowser.open(
            FRONTEND_URL,
            new=2
        )

        if opened:

            print("[OK] Browser opened.")

        else:

            print(
                "[WARNING] Browser could not be opened."
            )

            print(
                f"Open manually: {FRONTEND_URL}"
            )

    except Exception as exc:

        print(
            f"[WARNING] Browser opening failed: {exc}"
        )

        print(
            f"Open manually: {FRONTEND_URL}"
        )

    print()


# ============================================================
# WINDOWS PROCESS TREE KILL
# ============================================================
#
# This is the important part.
#
# taskkill /PID <PID> /T /F
#
# /T = terminate the entire process tree
# /F = force termination
#
# This handles worker -> child Python relationships.
# ============================================================

def kill_process_tree(process, name):

    if process is None:
        return

    pid = process.pid

    if pid is None:
        return

    print(
        f"[STOP] {name} "
        f"(PID {pid})"
    )

    # --------------------------------------------------------
    # First check whether the process is already dead.
    # --------------------------------------------------------

    if process.poll() is not None:

        print(
            f"[INFO] {name} already stopped."
        )

        return

    # --------------------------------------------------------
    # Windows
    # --------------------------------------------------------

    if os.name == "nt":

        try:

            result = subprocess.run(
                [
                    "taskkill",
                    "/PID",
                    str(pid),
                    "/T",
                    "/F",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=False,
            )

            if result.returncode == 0:

                print(
                    f"[OK] {name} process tree stopped."
                )

            else:

                # Sometimes taskkill reports that the
                # process has already disappeared.

                if result.stdout:
                    print(result.stdout.strip())

                if result.stderr:
                    print(result.stderr.strip())

        except Exception as exc:

            print(
                f"[WARNING] taskkill failed for "
                f"{name}: {exc}"
            )

            # Fallback.

            try:
                process.kill()
            except Exception:
                pass

    # --------------------------------------------------------
    # Linux / macOS fallback
    # --------------------------------------------------------

    else:

        try:

            process.terminate()

            process.wait(
                timeout=5
            )

        except subprocess.TimeoutExpired:

            try:
                process.kill()
            except Exception:
                pass

        except Exception as exc:

            print(
                f"[WARNING] Could not stop "
                f"{name}: {exc}"
            )


# ============================================================
# STOP ALL APPLICATION PROCESSES
# ============================================================

def stop_all_processes():

    print()
    print("=" * 72)
    print("                  STOPPING APPLICATIONS")
    print("=" * 72)
    print()

    # --------------------------------------------------------
    # Copy list because we will modify the original list.
    # --------------------------------------------------------

    current_processes = list(processes)

    # --------------------------------------------------------
    # Stop in reverse startup order:
    #
    # Vite
    # FastAPI
    # Workers
    # Producer
    # --------------------------------------------------------

    for item in reversed(current_processes):

        name = item["name"]
        process = item["process"]

        kill_process_tree(
            process,
            name
        )

    processes.clear()

    print()
    print("[OK] Application processes cleaned up.")


# ============================================================
# STOP DOCKER
# ============================================================

def stop_docker():

    print()
    print("=" * 72)
    print("                    STOPPING DOCKER")
    print("=" * 72)
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

            if result.stdout:
                print(result.stdout)

            print(
                "[OK] Docker Compose services stopped."
            )

        else:

            print(
                "[WARNING] Docker Compose shutdown "
                "returned an error."
            )

            if result.stderr:
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
    print("=" * 72)
    print("                  STREAM FORGE SHUTDOWN")
    print("=" * 72)
    print()

    # --------------------------------------------------------
    # 1. Stop all Windows process trees.
    # --------------------------------------------------------

    stop_all_processes()

    # --------------------------------------------------------
    # 2. Stop Docker Compose.
    #
    # IMPORTANT:
    #
    # docker compose down does NOT delete the Kafka topic.
    #
    # We are NOT using:
    #
    # kafka-topics --delete
    # kafka-topics --create
    # kafka-topics --alter
    #
    # Therefore truck_telemetry remains intact in the
    # Kafka storage.
    # --------------------------------------------------------

    stop_docker()

    print()
    print("=" * 72)
    print("                   STREAM FORGE STOPPED")
    print("=" * 72)
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
# REGISTER SIGNALS
# ============================================================

def register_signals():

    signal.signal(
        signal.SIGINT,
        handle_sigint
    )

    if hasattr(signal, "SIGTERM"):

        signal.signal(
            signal.SIGTERM,
            handle_sigterm
        )


# ============================================================
# VALIDATE ENVIRONMENT
# ============================================================

def validate_environment():

    print("[CHECK] Checking environment...")

    # --------------------------------------------------------
    # Python
    # --------------------------------------------------------

    if not PYTHON.exists():

        print()
        print(
            "[ERROR] Virtual environment Python not found:"
        )

        print(PYTHON)

        print()

        sys.exit(1)

    print(
        f"[OK] Python: {PYTHON}"
    )

    # --------------------------------------------------------
    # Frontend
    # --------------------------------------------------------

    if not FRONTEND_DIR.exists():

        print()
        print(
            "[ERROR] Frontend directory not found:"
        )

        print(FRONTEND_DIR)

        print()

        sys.exit(1)

    print(
        f"[OK] Frontend: {FRONTEND_DIR}"
    )

    print()


# ============================================================
# MAIN
# ============================================================

def main():

    register_signals()

    print_header()

    validate_environment()

    try:

        # ====================================================
        # 1. DOCKER
        # ====================================================

        start_docker()

        # ====================================================
        # 2. EXISTING KAFKA TOPIC
        # ====================================================

        check_topic()

        # ====================================================
        # 3. WORKER COUNT
        # ====================================================

        worker_count = ask_worker_count()

        # ====================================================
        # 4. PRODUCER
        # ====================================================

        producer = start_producer()

        # Give producer a moment to initialize.

        time.sleep(2)

        # ====================================================
        # 5. WORKERS
        # ====================================================

        start_workers(
            worker_count
        )

        # Give Kafka group time to rebalance.

        time.sleep(3)

        # ====================================================
        # 6. FASTAPI
        # ====================================================

        start_fastapi()

        # Give Uvicorn time to initialize.

        time.sleep(3)

        # ====================================================
        # 7. FRONTEND
        # ====================================================

        frontend = start_frontend()

        if frontend is None:

            print()
            print(
                "[ERROR] Frontend could not start."
            )

            shutdown()

            sys.exit(1)

        # ====================================================
        # 8. WAIT FOR FRONTEND
        # ====================================================

        if not wait_for_frontend():

            shutdown()

            sys.exit(1)

        # ====================================================
        # 9. OPEN BROWSER
        # ====================================================

        open_browser()

        # ====================================================
        # KEEP RUNNER ALIVE
        # ====================================================

        while True:

            time.sleep(1)

            # ------------------------------------------------
            # Check whether a managed process has stopped.
            # ------------------------------------------------

            for item in list(processes):

                name = item["name"]
                process = item["process"]

                return_code = process.poll()

                if return_code is not None:

                    print()
                    print(
                        f"[WARNING] {name} stopped."
                    )

                    print(
                        f"Exit code: {return_code}"
                    )

                    # Remove from managed list.

                    try:

                        processes.remove(
                            item
                        )

                    except ValueError:

                        pass

            # ------------------------------------------------
            # If everything unexpectedly died, clean up.
            # ------------------------------------------------

            if not processes:

                print()
                print(
                    "[WARNING] All application processes "
                    "have stopped."
                )

                shutdown()

                break

    except KeyboardInterrupt:

        # ----------------------------------------------------
        # This is the normal CTRL+C path.
        # ----------------------------------------------------

        print()
        print(
            "[CTRL+C] Shutdown requested."
        )

        shutdown()

    except Exception as exc:

        print()
        print("=" * 72)
        print("                         ERROR")
        print("=" * 72)
        print()

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()

        shutdown()

        sys.exit(1)

    finally:

        # ----------------------------------------------------
        # Ensures cleanup even if an unexpected exception
        # occurs.
        # ----------------------------------------------------

        shutdown()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()