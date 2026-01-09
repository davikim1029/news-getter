#main.py
"""
News Aggregator Server Manager

Manages the FastAPI news aggregator service with start/stop/monitor capabilities.
"""

import sys
import os
import subprocess
import signal
from pathlib import Path
import time
import psutil
from datetime import datetime
import argparse
from collections import deque
from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# -----------------------------
# Paths & Constants
# -----------------------------
PORT = 9000
PID_FILE = Path("news_server.pid")
APP_NAME = "news_server:app" 
APP_NAME_FRAGMENT = "news_server"
LOG_FILE = Path("news_server.log")

# uvicorn command to start FastAPI server
UVICORN_CMD = [
    sys.executable, "-m", "uvicorn",
    APP_NAME,
    "--host", "0.0.0.0",
    "--port", str(PORT),
    "--log-level", "info",
]

RESTART_DELAY = 2  # seconds before auto-restart if crashed
HEARTBEAT = 600  # 10 minutes between health checks

server_start_time = None

# -----------------------------
# Helper Functions
# -----------------------------
def is_server_running():
    """Check if server process is running"""
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text())
        os.kill(pid, 0)  # check if process exists
        return True
    except (ValueError, ProcessLookupError):
        PID_FILE.unlink(missing_ok=True)
        return False


def get_process_using_port(port: int):
    """Find process using specified port"""
    try:
        conns = psutil.net_connections(kind="inet")
    except psutil.AccessDenied:
        logger.logMessage(
            "[Shutdown] Access denied while scanning network connections"
        )
        return None

    for conn in conns:
        if (
            conn.laddr
            and conn.laddr.port == port
            and conn.status == psutil.CONN_LISTEN
            and conn.pid
        ):
            try:
                return psutil.Process(conn.pid)
            except psutil.AccessDenied:
                return None
    return None


def kill_process_using_port(port: int):
    """Kill process using specified port"""
    proc = get_process_using_port(port)
    if not proc:
        return False

    print(f"[CLEANUP] Killing process on port {port}: PID={proc.pid}, NAME={proc.name()}")
    try:
        proc.terminate()
        proc.wait(timeout=3)
    except psutil.TimeoutExpired:
        proc.kill()
    return True


def kill_processes_by_name(name_fragment: str):
    """Kill processes where cmdline contains name_fragment"""
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            text = " ".join(proc.info['cmdline']) if proc.info['cmdline'] else proc.info['name']
            if text and name_fragment.lower() in text.lower():
                print(f"[CLEANUP] Killing process: PID={proc.pid}, NAME={proc.info['name']}")
                proc.terminate()
                killed += 1
        except Exception:
            continue
    
    return killed


def cleanup_previous_instance():
    """Clean up any previous server instances"""
    # 1: Kill anything using port
    kill_process_using_port(PORT)

    # 2: Kill anything with our app's name
    kill_processes_by_name(APP_NAME_FRAGMENT)

    # 3: Remove orphaned PID file
    if PID_FILE.exists():
        try:
            with PID_FILE.open() as f:
                old_pid = int(f.read().strip())
            print(f"[CLEANUP] PID file detected, killing old PID={old_pid}")
            os.kill(old_pid, signal.SIGKILL)
        except Exception:
            pass
        PID_FILE.unlink(missing_ok=True)


def start_server():
    """Start the news aggregator server"""
    global server_start_time
    
    if is_server_running():
        pid = int(PID_FILE.read_text())
        print(f"Server is already running with PID {pid}")
        if server_start_time:
            print(f"Started at: {server_start_time}")
        return

    # Clean up any previous instances
    cleanup_previous_instance()

    if PID_FILE.exists():
        PID_FILE.unlink(missing_ok=True)

    # Start the server
    print(f"Starting news aggregator server on port {PORT}...")
    with LOG_FILE.open("a") as log_file:
        process = subprocess.Popen(
            UVICORN_CMD,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True
        )
    
    PID_FILE.write_text(str(process.pid))
    server_start_time = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Server started with PID {process.pid}")
    print(f"Start time: {server_start_time}")
    print(f"Logging to: {LOG_FILE}")
    print(f"API available at: http://localhost:{PORT}")
    print(f"API docs at: http://localhost:{PORT}/docs")


def stop_server():
    """Stop the news aggregator server"""
    if not PID_FILE.exists():
        print("No PID file found. Server may not be running.")
        cleanup_previous_instance()
        return

    pid = int(PID_FILE.read_text())

    try:
        # Kill entire session (process group)
        os.killpg(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to process group for PID {pid}")
        
        # Wait for graceful shutdown
        time.sleep(2)
        
        # Force kill if still running
        if is_server_running():
            os.killpg(pid, signal.SIGKILL)
            print(f"Sent SIGKILL to process group for PID {pid}")

    except ProcessLookupError:
        print(f"No process with PID {pid} found.")
    except PermissionError:
        print("Permission error - trying alternative cleanup")
        cleanup_previous_instance()

    PID_FILE.unlink(missing_ok=True)
    print("Server stopped.")


def check_server():
    """Check server status"""
    if is_server_running():
        pid = int(PID_FILE.read_text())
        proc = psutil.Process(pid)
        
        print(f"✓ Server is running")
        print(f"  PID: {pid}")
        print(f"  CPU: {proc.cpu_percent()}%")
        print(f"  Memory: {proc.memory_info().rss / 1024 / 1024:.1f} MB")
        print(f"  Started: {datetime.fromtimestamp(proc.create_time()).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  API: http://localhost:{PORT}")
        print(f"  Docs: http://localhost:{PORT}/docs")
    else:
        print("✗ Server is not running.")


def monitor_loop():
    """Continuously monitor the server and restart if it crashes"""
    global server_start_time

    print(f"Starting monitoring loop (checking every {HEARTBEAT}s)...")
    print("Press Ctrl+C to stop monitoring")

    while True:
        try:
            if is_server_running():
                time.sleep(HEARTBEAT)
                continue

            # Server is down, restart it
            server_start_time = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
            logger.logMessage(f"Server down, restarting at {server_start_time}")
            print(f"\n[{server_start_time}] Server crashed, restarting...")
            
            if PID_FILE.exists():
                PID_FILE.unlink(missing_ok=True)

            with LOG_FILE.open("a") as log_file:
                process = subprocess.Popen(
                    UVICORN_CMD,
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True
                )
                PID_FILE.write_text(str(process.pid))
                print(f"Restarted with PID {process.pid}")
                process.wait()  # Wait until server exits

            logger.logMessage(f"Server exited, restarting in {RESTART_DELAY}s...")
            time.sleep(RESTART_DELAY)
            
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped by user.")
            break


def tail_log(file_path: Path, n: int = 20):
    """Show last N lines of log file"""
    if not file_path.exists():
        print(f"Log file not found: {file_path}")
        return

    with file_path.open("r") as f:
        last_lines = deque(f, maxlen=n)

    print(f"\n--- Last {n} lines of {file_path} ---")
    for line in last_lines:
        print(line, end='')
    print("--- End of log ---\n")


def stats():
    """Show server statistics"""
    if not is_server_running():
        print("Server is not running.")
        return
    
    try:
        import requests
        response = requests.get(f"http://localhost:{PORT}/stats", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("\n=== News Aggregator Statistics ===")
            print(f"Symbols tracked: {data.get('total_symbols_tracked', 0)}")
            print(f"Option symbols: {data.get('total_option_symbols', 0)}")
            print(f"Average sentiment: {data.get('average_sentiment', 0):.3f}")
            print(f"Total articles: {data.get('total_articles', 0)}")
            print("\nRecent updates:")
            for update in data.get('recent_updates', [])[:5]:
                print(f"  {update['symbol']}: {update['sentiment']:.3f} "
                      f"({update['article_count']} articles) - {update['last_updated']}")
        else:
            print(f"Error fetching stats: {response.status_code}")
    except Exception as e:
        print(f"Error connecting to server: {e}")


def get_mode_from_prompt():
    """Interactive mode selection"""
    modes = [
        ("start-server", "Start the news aggregator server"),
        ("monitor", "Monitor the server and auto-restart if crashed"),
        ("stop", "Stop the server"),
        ("check", "Check server status"),
        ("stats", "Show server statistics"),
        ("logs", "Show last 20 log lines"),
        ("quit", "Exit program")
    ]

    while True:
        print("\n=== News Aggregator Server Manager ===")
        print("Available commands:")
        for i, (key, desc) in enumerate(modes, start=1):
            print(f"  {i}. {desc} [{key}]")
        
        choice = input("\nEnter command number or name (default 1): ").strip().lower()
        
        if choice in ("q", "quit"):
            return "quit"
        
        if not choice:
            choice = "1"
        
        # Try to match by number
        try:
            index = int(choice) - 1
            if 0 <= index < len(modes):
                return modes[index][0]
        except ValueError:
            pass
        
        # Try to match by name
        for key, _ in modes:
            if key == choice:
                return key
        
        print("Invalid choice, try again.")

import requests, time

def wait_for_server(port=PORT, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"http://localhost:{port}", timeout=1)
            if r.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    return False


# -----------------------------
# Main Entry Point
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="News Aggregator Server Manager")
    parser.add_argument("--mode", help="Mode to run: start-server, monitor, stop, check, stats, logs")
    parser.add_argument("--lines", type=int, default=20, help="Number of log lines to show")
    args = parser.parse_args()
    
    while True:
        mode = args.mode.lower() if args.mode else get_mode_from_prompt()
        
        if mode == "start-server":
            start_server()
            if wait_for_server():
                print("Server ready to receive API calls.")
            else:
                print("Server did not start properly, check logs.")

            
        elif mode == "monitor":
            monitor_loop()
            
        elif mode == "stop":
            stop_server()
            
        elif mode == "check":
            check_server()
            
        elif mode == "stats":
            stats()
            
        elif mode == "logs":
            tail_log(LOG_FILE, args.lines)
            
        elif mode == "quit":
            print("Goodbye!")
            break
            
        else:
            print(f"Unknown mode: {mode}")
        
        # If mode was provided via CLI, exit after running once
        if args.mode:
            break


if __name__ == "__main__":
    main()