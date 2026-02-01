#!/usr/bin/env python3
"""
Simple FastAPI Health Check Server
Lightweight service to keep Render hosting alive
"""

import os
import time
import threading
import subprocess
import sys
import requests
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Load environment variables first
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded in health.py")
except ImportError:
    print("python-dotenv not installed")
except Exception as e:
    print(f"Could not load .env file in health.py: {e}")

app = FastAPI(title="Health Check Service", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

start_time = time.time()
detector_process = None
detector_starting = False
restart_count = 0
max_restarts = 5
last_restart_time = 0

# Health ping configuration
HEALTH_PING_URL = "https://fyers-volume-spike-detector.onrender.com/health"
HEALTH_PING_INTERVAL = 420  # 7 minutes in seconds
last_health_ping_time = 0
health_ping_success_count = 0
health_ping_failure_count = 0


@app.get("/")
async def root():
    return {
        "message": "Health service running", 
        "status": "online"
    }

@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    global detector_process, restart_count, health_ping_success_count, health_ping_failure_count
    detector_status = "unknown"
    
    if detector_process:
        if detector_process.poll() is None:
            detector_status = "running"
        else:
            detector_status = "stopped"
    else:
        detector_status = "not_started"
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - start_time,
            "detector_status": detector_status,
            "restart_count": restart_count,
            "health_ping_success": health_ping_success_count,
            "health_ping_failure": health_ping_failure_count
        }
    )

@app.post("/restart-detector")
async def restart_detector():
    """Manually restart the detector - single attempt only"""
    global detector_process
    
    try:
        print("Manual restart command received")
        
        # Stop current process if running
        if detector_process and detector_process.poll() is None:
            print("Stopping current detector process...")
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                print("Force killing detector process...")
                detector_process.kill()
            detector_process = None
        
        # Single restart attempt - no retries
        print("Starting detector (single attempt)...")
        start_detector()
        
        # Give it a moment to start
        time.sleep(2)
        
        # Check if it started successfully
        if detector_process and detector_process.poll() is None:
            return JSONResponse(
                status_code=200,
                content={"message": "Detector restart successful", "status": "running"}
            )
        else:
            return JSONResponse(
                status_code=200,
                content={"message": "Detector restart attempted but process may have failed", "status": "unknown"}
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to restart detector: {str(e)}"}
        )

@app.post("/stop-detector")
async def stop_detector():
    """Stop the detector"""
    global detector_process
    
    try:
        if detector_process and detector_process.poll() is None:
            print("Stopping detector process...")
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                print("Force killing detector process...")
                detector_process.kill()
            detector_process = None
            return JSONResponse(
                status_code=200,
                content={"message": "Detector stopped successfully"}
            )
        else:
            return JSONResponse(
                status_code=200,
                content={"message": "Detector was not running"}
            )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to stop detector: {str(e)}"}
        )

@app.get("/detector-info")
async def detector_info():
    """Get detailed detector information"""
    global detector_process, health_ping_success_count, health_ping_failure_count
    
    status = "unknown"
    pid = None
    
    if detector_process:
        if detector_process.poll() is None:
            status = "running"
            pid = detector_process.pid
        else:
            status = "stopped"
            pid = f"exited with code {detector_process.returncode}"
    else:
        status = "not_started"
    
    return JSONResponse(
        status_code=200,
        content={
            "detector_status": status,
            "detector_pid": pid,
            "restart_behavior": "single_attempt_only",
            "websocket_retries": "limited_to_1",
            "telegram_restart_commands": "disabled",
            "health_ping_url": HEALTH_PING_URL,
            "health_ping_interval": f"{HEALTH_PING_INTERVAL} seconds (7 minutes)",
            "health_ping_success": health_ping_success_count,
            "health_ping_failure": health_ping_failure_count,
            "service": "fyers-volume-spike-detector"
        }
    )

@app.get("/ping-stats")
async def ping_stats():
    """Get health ping statistics"""
    global health_ping_success_count, health_ping_failure_count, last_health_ping_time
    
    return JSONResponse(
        status_code=200,
        content={
            "health_ping_url": HEALTH_PING_URL,
            "ping_interval_seconds": HEALTH_PING_INTERVAL,
            "ping_interval_minutes": HEALTH_PING_INTERVAL / 60,
            "total_success": health_ping_success_count,
            "total_failure": health_ping_failure_count,
            "last_ping_time": datetime.fromtimestamp(last_health_ping_time).isoformat() if last_health_ping_time > 0 else "never",
            "next_ping_in_seconds": max(0, HEALTH_PING_INTERVAL - (time.time() - last_health_ping_time)) if last_health_ping_time > 0 else 0
        }
    )

def send_health_ping():
    """Send health ping to keep service alive"""
    global last_health_ping_time, health_ping_success_count, health_ping_failure_count
    
    try:
        print(f"Sending health ping to {HEALTH_PING_URL}")
        response = requests.get(HEALTH_PING_URL, timeout=10)
        
        if response.status_code == 200:
            health_ping_success_count += 1
            print(f"Health ping successful (#{health_ping_success_count})")
        else:
            health_ping_failure_count += 1
            print(f"Health ping failed with status {response.status_code} (#{health_ping_failure_count})")
        
        last_health_ping_time = time.time()
        
    except Exception as e:
        health_ping_failure_count += 1
        print(f"Health ping error (#{health_ping_failure_count}): {e}")
        last_health_ping_time = time.time()

def health_ping_worker():
    """Background worker to send periodic health pings"""
    global last_health_ping_time
    
    print(f"Health ping worker started - will ping every {HEALTH_PING_INTERVAL} seconds (7 minutes)")
    
    # Wait a bit before starting
    time.sleep(30)
    
    while True:
        try:
            current_time = time.time()
            
            # Check if it's time to send a ping
            if current_time - last_health_ping_time >= HEALTH_PING_INTERVAL:
                send_health_ping()
            
            # Sleep for 1 minute before checking again
            time.sleep(60)
            
        except Exception as e:
            print(f"Health ping worker error: {e}")
            time.sleep(60)

def start_detector():
    """Start the Fyers detector process"""
    global detector_process, detector_starting
    
    if detector_starting:
        return
        
    detector_starting = True
    
    try:
        print("Starting Fyers detector...")
        
        # Find fyers.py in possible locations
        possible_locations = [
            os.path.dirname(os.path.abspath(__file__)),
            os.getcwd(),
            "/opt/render/project/src",
            ".",
        ]
        
        detector_path = None
        health_dir = None
        
        for location in possible_locations:
            test_path = os.path.join(location, "fyers.py")
            if os.path.exists(test_path):
                detector_path = test_path
                health_dir = location
                break
        
        if not detector_path:
            print("ERROR: fyers.py not found in any location:")
            for location in possible_locations:
                test_path = os.path.join(location, "fyers.py")
                print(f"  - {test_path} (exists: {os.path.exists(test_path)})")
            return
        
        print(f"Starting detector from directory: {health_dir}")
        print(f"Detector script path: {detector_path}")
        
        # Use full path to python and capture output
        detector_process = subprocess.Popen(
            [sys.executable, detector_path],
            cwd=health_dir,  # Ensure subprocess runs in correct directory
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"Detector started with PID: {detector_process.pid}")
        
        # Start a thread to read output
        def read_output():
            try:
                for line in iter(detector_process.stdout.readline, ''):
                    if line:
                        print(f"DETECTOR: {line.strip()}")
            except Exception as e:
                print(f"Output reader error: {e}")
        
        threading.Thread(target=read_output, daemon=True).start()
        
    except Exception as e:
        print(f"Failed to start detector: {e}")
    finally:
        detector_starting = False

def monitor_detector():
    """Keep detector running with restart limits"""
    global detector_process, restart_count, last_restart_time
    
    # Wait before starting monitoring
    time.sleep(10)
    
    while True:
        try:
            if detector_process is None or detector_process.poll() is not None:
                current_time = time.time()
                
                # Check if we've exceeded max restarts within a time window
                if restart_count >= max_restarts:
                    if current_time - last_restart_time < 300:  # 5 minutes
                        print(f"Max restarts ({max_restarts}) reached within 5 minutes. Stopping auto-restart.")
                        print("Manual intervention required. Check logs and restart manually.")
                        time.sleep(300)  # Wait 5 minutes before resetting counter
                        restart_count = 0
                        continue
                    else:
                        # Reset counter if enough time has passed
                        restart_count = 0
                
                if detector_process and detector_process.poll() is not None:
                    print(f"Detector exited with code: {detector_process.returncode}")
                
                print(f"Restarting detector... (attempt {restart_count + 1}/{max_restarts})")
                restart_count += 1
                last_restart_time = current_time
                
                # Wait longer between restarts to avoid rapid cycling
                time.sleep(10)
                start_detector()
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"Monitor error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("Starting health server...")
    print(f"Python version: {sys.version}")
    print(f"Platform: {os.name}")
    print(f"Environment variables: RENDER={os.environ.get('RENDER', 'Not set')}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Health.py absolute path: {os.path.abspath(__file__)}")
    
    # List all files in current directory for debugging
    try:
        print("Files in current directory:")
        for f in sorted(os.listdir('.')):
            print(f"  - {f}")
    except Exception as e:
        print(f"Error listing current directory: {e}")
    
    # Find the correct directory and change to it
    possible_locations = [
        os.path.dirname(os.path.abspath(__file__)),
        os.getcwd(),
        "/opt/render/project/src",
        ".",
    ]
    
    working_dir = None
    for location in possible_locations:
        if os.path.exists(os.path.join(location, "fyers.py")):
            working_dir = location
            break
    
    if working_dir:
        if os.getcwd() != working_dir:
            print(f"Changing working directory to: {working_dir}")
            os.chdir(working_dir)
        print(f"Current working directory: {os.getcwd()}")
        print(f"fyers.py exists: {os.path.exists('fyers.py')}")
    else:
        print("WARNING: Could not find fyers.py in any expected location")
        print(f"Current working directory: {os.getcwd()}")
        print("Available files:", [f for f in os.listdir('.') if f.endswith('.py')])
    
    # Start detector after a short delay
    def delayed_start():
        time.sleep(2)
        start_detector()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    threading.Thread(target=monitor_detector, daemon=True).start()
    
    # Start health ping worker
    threading.Thread(target=health_ping_worker, daemon=True).start()
    print("Health ping worker scheduled to start in 30 seconds")

    # Start health server
    port = int(os.getenv("PORT", 8000))
    print(f"Health server starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
