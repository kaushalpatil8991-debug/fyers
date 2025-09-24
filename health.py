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
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

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
manual_restart_count = 0
manual_restart_in_progress = False

@app.get("/")
async def root():
    return {
        "message": "Health service running", 
        "status": "online"
    }

@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    global detector_process, restart_count, manual_restart_count
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
            "manual_restart_count": manual_restart_count
        }
    )

@app.post("/restart-detector")
async def restart_detector():
    """Manually restart the detector"""
    global detector_process, restart_count
    
    try:
        # Stop current process if running
        if detector_process and detector_process.poll() is None:
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                detector_process.kill()
        
        # Reset restart counter for manual restart
        restart_count = 0
        
        # Start new process
        start_detector()
        
        return JSONResponse(
            status_code=200,
            content={"message": "Detector restart initiated", "restart_count": restart_count}
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
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                detector_process.kill()
            detector_process = None
            return JSONResponse(
                status_code=200,
                content={"message": "Detector stopped"}
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

def start_detector():
    """Start the Fyers detector process"""
    global detector_process, detector_starting
    
    if detector_starting:
        return
        
    detector_starting = True
    
    try:
        print("Starting Fyers detector...")
        
        # Use full path to python and capture output
        detector_process = subprocess.Popen(
            [sys.executable, "fyers_detector.py"],
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
    
    # Start detector after a short delay
    def delayed_start():
        time.sleep(2)
        start_detector()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    threading.Thread(target=monitor_detector, daemon=True).start()
    
    # Start health server
    port = int(os.getenv("PORT", 8000))
    print(f"Health server starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
