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

@app.get("/")
async def root():
    return {
        "message": "Health service running", 
        "status": "online"
    }

@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    global detector_process
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
            "detector_status": detector_status
        }
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
    """Keep detector running"""
    global detector_process
    
    # Wait before starting monitoring
    time.sleep(10)
    
    while True:
        try:
            if detector_process is None or detector_process.poll() is not None:
                if detector_process and detector_process.poll() is not None:
                    print(f"Detector exited with code: {detector_process.returncode}")
                
                print("Restarting detector...")
                time.sleep(5)  # Wait before restart
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