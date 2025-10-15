#!/usr/bin/env python3
"""
Fyers Volume Spike Detector - Google Sheets Integration with Sector Classification
Detects large individual trades and updates Google Sheets in real-time with sector information
"""

import json
import os
import sys
import time
import threading
import requests
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import pyotp
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded from .env file")
except ImportError:
    print("python-dotenv not installed. Install with: pip install python-dotenv")
except Exception as e:
    print(f"Could not load .env file: {e}")

# =============================================================================
# CONFIGURATION - Update these with your actual credentials
# =============================================================================

# Fyers API Credentials
FYERS_CLIENT_ID = "EH8TE9J6PZ-100"
FYERS_SECRET_KEY = "V8EC76L8UP"
FYERS_REDIRECT_URI = "https://fyersauth.vercel.app/"
FYERS_TOTP_SECRET = "7JKB7FFBMZNQRYYV7PQ46L7XRUQLR6FV"
FYERS_PIN = "8905"

# Trading Configuration
INDIVIDUAL_TRADE_THRESHOLD = 30000000  # Rs 3 crore for individual trades
MIN_VOLUME_SPIKE = 1000  # Minimum volume spike to consider

# Google Sheets Configuration
GOOGLE_SHEETS_ID = "1l_6Sx_u1czhY-5JdT22tpmCV8Os3XuZmZ3U2ShKDLHw"

# =============================================================================
# RUN CONTROLLER GLOBALS
# =============================================================================

_running_flag = False
_stop_event = threading.Event()

# =============================================================================
# RUN CONTROLLER FUNCTIONS
# =============================================================================

def _start_stream_once():
    """Start your Fyers WebSocket loop exactly once."""
    global _running_flag, _stop_event
    if _running_flag:
        return False
    _stop_event.clear()
    threading.Thread(target=_stream_worker, args=(_stop_event,), daemon=True).start()
    _running_flag = True
    print("Stream STARTED")
    return True

def _stop_stream_once():
    """Signal your stream loop to stop and wait briefly."""
    global _running_flag, _stop_event
    if not _running_flag:
        return False
    _stop_event.set()
    time.sleep(2)
    _running_flag = False
    print("Stream STOPPED")
    return False

def _stream_worker(stop_event: threading.Event):
    """Simplified worker that runs the detector with proper error handling"""
    try:
        print("Starting detector stream worker (single attempt)", flush=True)
        detector = VolumeSpikeDetector()
        detector.stop_event = stop_event
        
        print("Initializing detector...", flush=True)
        if detector.initialize():
            print("Detector initialized successfully", flush=True)
            print("Starting monitoring...", flush=True)
            detector.start_monitoring()
            print("Monitoring ended normally", flush=True)
        else:
            print("Detector initialization failed - exiting", flush=True)
            return
            
    except Exception as e:
        print(f"Stream worker error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        print("Stream worker exiting due to error", flush=True)
        return
    
    print("Stream worker stopped", flush=True)

def _inside_window_ist() -> bool:
    """Check if current IST time is within market hours."""
    # Check if force start flag is set
    import builtins
    if hasattr(builtins, 'FORCE_START') and builtins.FORCE_START:
        return True  # Always return True when forced
    
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    hhmm = now.strftime("%H:%M")
    return MARKET_START_TIME <= hhmm < MARKET_END_TIME

def supervisor_loop():
    """Simplified supervisor that manages the detector lifecycle"""
    print("Supervisor loop started", flush=True)
    detector = None
    last_auth_check = time.time()
    last_command_check = time.time()
    AUTH_CHECK_INTERVAL = 3600
    COMMAND_CHECK_INTERVAL = 3  # Check for commands every 10 seconds
    
    # Create telegram handler for checking commands
    telegram = TelegramHandler()
    
    while True:
        try:
            current_time = time.time()
            
            # Check for force/restart commands periodically
            if current_time - last_command_check > COMMAND_CHECK_INTERVAL:
                print("[SUPERVISOR] Checking for commands...", flush=True)
                
                # Check for force command
                if telegram.check_for_force_command():
                    print("[SUPERVISOR] Force command detected! Setting FORCE_START flag", flush=True)
                    telegram.send_message("🚀 <b>Force Start Initiated</b>\n\n⏳ Bypassing market hours...\n📊 Starting detector immediately...")
                    import builtins
                    builtins.FORCE_START = True
                
                # Check for restart command
                if telegram.check_for_restart_command():
                    print("[SUPERVISOR] Restart command detected! Restarting detector only...", flush=True)
                    
                    # Send status message
                    telegram.send_message("🔄 <b>Restarting Detector...</b>\n\n⏳ Stopping current detector process...")
                    
                    # Stop only the detector, not the summary scheduler
                    _stop_stream_once()
                    detector = None
                    
                    telegram.send_message("✅ Detector stopped\n⏳ Starting fresh detector instance...")
                    
                    time.sleep(2)
                    
                    # Create new detector instance
                    try:
                        detector = VolumeSpikeDetector()
                        telegram.send_message("🔧 Detector instance created\n⏳ Initializing...")
                        
                        _start_stream_once()
                        
                        telegram.send_message("✅ <b>Detector Restarted Successfully!</b>\n\n📊 Status: Monitoring active\n⏰ Time: " + datetime.now().strftime('%H:%M:%S'))
                        print("[SUPERVISOR] Detector restarted successfully", flush=True)
                        
                    except Exception as restart_error:
                        error_msg = f"❌ <b>Restart Failed</b>\n\nError: {str(restart_error)}\n\nPlease try again or check logs."
                        telegram.send_message(error_msg)
                        print(f"[SUPERVISOR] Restart error: {restart_error}", flush=True)
                    
                    # Continue to next iteration
                    last_command_check = current_time
                    continue
                
                last_command_check = current_time
            
            # Check if we're in market hours
            in_window = _inside_window_ist()
            
            if SCHEDULING_ENABLED and not in_window:
                if detector:
                    print("Outside market hours, stopping detector...", flush=True)
                    _stop_stream_once()
                    detector = None
                # Don't spam logs, only print occasionally
                if int(current_time) % 300 == 0:  # Every 5 minutes
                    print("Waiting for market hours (or send 'force' command)...", flush=True)
                time.sleep(60)
                continue
            
            # We should be running - start detector if not running
            if not detector or not _running_flag:
                print("Starting detector...", flush=True)
                _stop_stream_once()
                time.sleep(2)
                
                try:
                    detector = VolumeSpikeDetector()
                    print("Detector instance created, starting stream...", flush=True)
                    _start_stream_once()
                    print("Stream started successfully", flush=True)
                    
                    # Send connection status to user after successful start
                    try:
                        # Wait a moment for initialization to complete
                        time.sleep(3)
                        
                        # Check if detector is actually running and authenticated
                        if detector and hasattr(detector, 'authenticator') and detector.authenticator.is_authenticated:
                            # Get user info if available
                            user_name = "Unknown"
                            try:
                                if detector.authenticator.fyers_model:
                                    profile = detector.authenticator.fyers_model.get_profile()
                                    if profile.get('s') == 'ok':
                                        user_name = profile['data']['name']
                            except:
                                pass
                            
                            connection_msg = f"""✅ <b>Detector Connected Successfully!</b>

👤 <b>User:</b> {user_name}
📊 <b>Status:</b> Monitoring Active
🎯 <b>Symbols:</b> {len(STOCK_SYMBOLS)} stocks
💰 <b>Threshold:</b> Rs {INDIVIDUAL_TRADE_THRESHOLD/10000000:.1f} Cr
⏰ <b>Started:</b> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

🔔 You will receive alerts for large volume spikes!"""
                            
                            telegram.send_message(connection_msg)
                            print("Connection status sent to user", flush=True)
                            
                    except Exception as msg_error:
                        print(f"Could not send connection message: {msg_error}", flush=True)
                        
                except Exception as start_error:
                    print(f"Error starting detector: {start_error}", flush=True)
                    telegram.send_message(f"❌ <b>Detector Start Failed</b>\n\nError: {str(start_error)}\n\nWill retry in 30 seconds...")
                    time.sleep(30)
                
            # Periodic auth check (every hour)
            if current_time - last_auth_check > AUTH_CHECK_INTERVAL:
                print("Performing periodic auth check...", flush=True)
                if detector and hasattr(detector, 'authenticator'):
                    if not detector.authenticator.is_authenticated:
                        print("Auth expired, will re-authenticate on next cycle", flush=True)
                        _stop_stream_once()
                        detector = None
                last_auth_check = current_time
            
            # Sleep before next check
            time.sleep(5)  # Check more frequently
            
        except Exception as e:
            print(f"Supervisor error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            time.sleep(10)

# Load Google Credentials from Environment Variables
try:
    google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if google_creds_json:
        GOOGLE_CREDENTIALS = json.loads(google_creds_json)
        if 'private_key' in GOOGLE_CREDENTIALS:
            GOOGLE_CREDENTIALS['private_key'] = GOOGLE_CREDENTIALS['private_key'].replace('\\n', '\n')
        print("Google Sheets credentials loaded from environment variable")
    else:
        private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        if private_key:
            private_key = private_key.replace('\\n', '\n')
        
        GOOGLE_CREDENTIALS = {
            "type": "service_account",
            "project_id": os.getenv('GOOGLE_PROJECT_ID'),
            "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
            "private_key": private_key,
            "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
            "client_id": os.getenv('GOOGLE_CLIENT_ID'),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
            "universe_domain": "googleapis.com"
        }
        
        required_fields = ['project_id', 'private_key_id', 'private_key', 'client_email', 'client_id']
        missing_fields = [field for field in required_fields if not GOOGLE_CREDENTIALS.get(field)]
        
        if missing_fields:
            print(f"Missing required Google credentials environment variables: {', '.join(missing_fields)}")
            GOOGLE_CREDENTIALS = None
        else:
            print("Google Sheets credentials loaded from individual environment variables")
            
except json.JSONDecodeError as e:
    print(f"Error parsing Google credentials JSON from environment: {e}")
    GOOGLE_CREDENTIALS = None
except Exception as e:
    print(f"Error loading Google credentials from environment: {e}")
    GOOGLE_CREDENTIALS = None

# Load Fyers Access Token from JSON file or environment variables
try:
    with open('fyers_access_token.json', 'r') as f:
        token_data = json.load(f)
        FYERS_ACCESS_TOKEN = token_data.get('access_token', '')
        FYERS_TOKEN_TIMESTAMP = float(token_data.get('timestamp', 0))
        FYERS_TOKEN_CREATED_AT = token_data.get('created_at', '')
        print("Fyers access token loaded from JSON file")
except FileNotFoundError:
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print("Fyers access token JSON file not found, using environment variables")
except Exception as e:
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print(f"Error loading Fyers token from JSON: {e}, using environment variables")

def validate_fyers_token_from_json():
    """Validate if the Fyers token from JSON file is still valid"""
    try:
        if not FYERS_ACCESS_TOKEN or FYERS_ACCESS_TOKEN.strip() == "":
            return False, "No token available"
        
        current_time = time.time()
        token_time = FYERS_TOKEN_TIMESTAMP
        
        if current_time - token_time < 28800:
            print("Fyers token from JSON file is valid")
            return True, "Token is valid"
        else:
            print("Fyers token from JSON file expired, need fresh authentication")
            return False, "Token expired"
            
    except Exception as e:
        print(f"Error validating Fyers token: {e}")
        return False, f"Validation error: {str(e)}"

def save_fyers_token_to_json(access_token, timestamp=None, created_at=None):
    """Save Fyers access token to JSON file"""
    try:
        if timestamp is None:
            timestamp = time.time()
        if created_at is None:
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        token_data = {
            "access_token": access_token,
            "timestamp": timestamp,
            "created_at": created_at
        }
        
        with open('fyers_access_token.json', 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print("Fyers access token saved to JSON file")
        return True
        
    except Exception as e:
        print(f"Error saving Fyers token to JSON: {e}")
        return False

# Telegram Configuration - For Detector
TELEGRAM_BOT_TOKEN = "8360146544:AAEObU8_9LoGTZk66PVSwcayD5Hw5fnHTgY"
TELEGRAM_CHAT_ID = "5715256800"
TELEGRAM_POLLING_INTERVAL = 5
TELEGRAM_AUTH_TIMEOUT = 300

# Summary Telegram Bot Configuration - SEPARATE CREDENTIALS
SUMMARY_TELEGRAM_BOT_TOKEN = "8490433133:AAEvchBW14co_Obtr8FPl_Nqg3LzqahEuNM"
SUMMARY_TELEGRAM_CHAT_ID = "8388919023"
SUMMARY_SEND_TIME = "16:30"  # 4:30 PM IST
SUMMARY_SEND_INTERVAL = 7200  # 2 hours in seconds

# Scheduling Configuration
MARKET_START_TIME = "09:13"
MARKET_END_TIME = "16:00"
SCHEDULING_ENABLED = True

# =============================================================================
# COMPREHENSIVE SECTOR MAPPING FOR NSE STOCKS
# =============================================================================

SECTOR_MAPPING = {
    # Technology Sector
    "NSE:TCS-EQ": "Information Technology",
    "NSE:INFY-EQ": "Information Technology",
    "NSE:WIPRO-EQ": "Information Technology",
    "NSE:HCLTECH-EQ": "Information Technology",
    "NSE:TECHM-EQ": "Information Technology",
    "NSE:LTIM-EQ": "Information Technology",
    "NSE:LTTS-EQ": "Information Technology",
    "NSE:MINDTREE-EQ": "Information Technology",
    "NSE:COFORGE-EQ": "Information Technology",
    "NSE:PERSISTENT-EQ": "Information Technology",
    "NSE:CYIENT-EQ": "Information Technology",
    "NSE:MPHASIS-EQ": "Information Technology",
    "NSE:INTELLECT-EQ": "Information Technology",
    "NSE:TATAELXSI-EQ": "Information Technology",
    "NSE:KPITTECH-EQ": "Information Technology",
    "NSE:MASTEK-EQ": "Information Technology",
    "NSE:NEWGEN-EQ": "Information Technology",
    "NSE:CYIENTDLM-EQ": "Information Technology",
    "NSE:OFSS-EQ": "Information Technology",
    "NSE:ZENSAR-EQ": "Information Technology",
    
    # Banking & Financial Services
    "NSE:HDFCBANK-EQ": "Banking",
    "NSE:ICICIBANK-EQ": "Banking",
    "NSE:AXISBANK-EQ": "Banking",
    "NSE:SBIN-EQ": "Banking",
    "NSE:KOTAKBANK-EQ": "Banking",
    "NSE:INDUSINDBK-EQ": "Banking",
    "NSE:BANDHANBNK-EQ": "Banking",
    "NSE:IDFCFIRSTB-EQ": "Banking",
    "NSE:FEDERALBNK-EQ": "Banking",
    "NSE:RBLBANK-EQ": "Banking",
    "NSE:YESBANK-EQ": "Banking",
    "NSE:AUBANK-EQ": "Banking",
    "NSE:BANKBARODA-EQ": "Banking",
    "NSE:PNB-EQ": "Banking",
    "NSE:CANBK-EQ": "Banking",
    "NSE:UNIONBANK-EQ": "Banking",
    "NSE:BANKINDIA-EQ": "Banking",
    "NSE:CENTRALBK-EQ": "Banking",
    "NSE:IOB-EQ": "Banking",
    "NSE:PSB-EQ": "Banking",
    "NSE:IDBI-EQ": "Banking",
    "NSE:UCOBANK-EQ": "Banking",
    "NSE:INDIANB-EQ": "Banking",
    "NSE:CSBBANK-EQ": "Banking",
    "NSE:DCBBANK-EQ": "Banking",
    "NSE:SOUTHBANK-EQ": "Banking",
    "NSE:TMB-EQ": "Banking",
    "NSE:KTKBANK-EQ": "Banking",
    "NSE:J&KBANK-EQ": "Banking",
    "NSE:DHANBANK-EQ": "Banking",
    "NSE:MAHABANK-EQ": "Banking",
    "NSE:KARURVYSYA-EQ": "Banking",
    "NSE:CUB-EQ": "Banking",
    "NSE:UTKARSHBNK-EQ": "Banking",
    "NSE:ESAFSFB-EQ": "Banking",
    "NSE:UJJIVANSFB-EQ": "Banking",
    "NSE:EQUITASBNK-EQ": "Banking",
    "NSE:CAPITALSFB-EQ": "Banking",
    "NSE:SURYODAY-EQ": "Banking",
    "NSE:FINPIPE-EQ": "Banking",
    
    # Financial Services (Non-Banking)
    "NSE:BAJFINANCE-EQ": "Financial Services",
    "NSE:BAJAJFINSV-EQ": "Financial Services",
    "NSE:HDFCLIFE-EQ": "Financial Services",
    "NSE:SBILIFE-EQ": "Financial Services",
    "NSE:ICICIGI-EQ": "Financial Services",
    "NSE:ICICIPRULI-EQ": "Financial Services",
    "NSE:LICI-EQ": "Financial Services",
    "NSE:NIACL-EQ": "Financial Services",
    "NSE:GODIGIT-EQ": "Financial Services",
    "NSE:STARHEALTH-EQ": "Financial Services",
    "NSE:NIVABUPA-EQ": "Financial Services",
    "NSE:HDFCAMC-EQ": "Financial Services",
    "NSE:UTIAMC-EQ": "Financial Services",
    "NSE:CRISIL-EQ": "Financial Services",
    "NSE:CREDITACC-EQ": "Financial Services",
    "NSE:BFSL-EQ": "Financial Services",
    "NSE:CHOLAFIN-EQ": "Financial Services",
    "NSE:MUTHOOTFIN-EQ": "Financial Services",
    "NSE:MANAPPURAM-EQ": "Financial Services",
    "NSE:PFC-EQ": "Financial Services",
    "NSE:RECLTD-EQ": "Financial Services",
    "NSE:IRFC-EQ": "Financial Services",
    "NSE:EDELWEISS-EQ": "Financial Services",
    "NSE:IIFL-EQ": "Financial Services",
    "NSE:M&MFIN-EQ": "Financial Services",
    "NSE:SHRIRAMFIN-EQ": "Financial Services",
    "NSE:BAJAJHFL-EQ": "Financial Services",
    "NSE:CANFINHOME-EQ": "Financial Services",
    "NSE:LICHSGFIN-EQ": "Financial Services",
    "NSE:PNBHOUSING-EQ": "Financial Services",
    "NSE:REPCO-EQ": "Financial Services",
    "NSE:HOMEFIRST-EQ": "Financial Services",
    "NSE:INDOSTAR-EQ": "Financial Services",
    "NSE:SPANDANA-EQ": "Financial Services",
    "NSE:PAISALO-EQ": "Financial Services",
    "NSE:JSFB-EQ": "Financial Services",
    "NSE:SBFC-EQ": "Financial Services",
    "NSE:ASIANFIN-EQ": "Financial Services",
    "NSE:RELIGARE-EQ": "Financial Services",
    "NSE:MOTILALOFS-EQ": "Financial Services",
    "NSE:ANGELONE-EQ": "Financial Services",
    "NSE:ANANDRATHI-EQ": "Financial Services",
    "NSE:ARIHANTCAP-EQ": "Financial Services",
    "NSE:GEOJITFSL-EQ": "Financial Services",
    "NSE:NUVAMA-EQ": "Financial Services",
    "NSE:KFINTECH-EQ": "Financial Services",
    "NSE:CDSL-EQ": "Financial Services",
    "NSE:BSE-EQ": "Financial Services",
    "NSE:MCX-EQ": "Financial Services",
    "NSE:IEX-EQ": "Financial Services",
    "NSE:CAMS-EQ": "Financial Services",
    "NSE:JIOFIN-EQ": "Financial Services",
    
    # Oil & Gas / Energy
    "NSE:RELIANCE-EQ": "Oil & Gas",
    "NSE:ONGC-EQ": "Oil & Gas",
    "NSE:IOC-EQ": "Oil & Gas",
    "NSE:BPCL-EQ": "Oil & Gas",
    "NSE:HINDPETRO-EQ": "Oil & Gas",
    "NSE:GAIL-EQ": "Oil & Gas",
    "NSE:OIL-EQ": "Oil & Gas",
    "NSE:MGL-EQ": "Oil & Gas",
    "NSE:IGL-EQ": "Oil & Gas",
    "NSE:GUJGASLTD-EQ": "Oil & Gas",
    "NSE:ATGL-EQ": "Oil & Gas",
    "NSE:CASTROLIND-EQ": "Oil & Gas",
    "NSE:GULF-EQ": "Oil & Gas",
    "NSE:GULFOILLUB-EQ": "Oil & Gas",
    "NSE:GULFPETRO-EQ": "Oil & Gas",
    "NSE:HINDOILEXP-EQ": "Oil & Gas",
    "NSE:SELAN-EQ": "Oil & Gas",
    "NSE:MRPL-EQ": "Oil & Gas",
    "NSE:TNPETRO-EQ": "Oil & Gas",
    "NSE:CHENNPETRO-EQ": "Oil & Gas",
    "NSE:HINDNATGLS-EQ": "Oil & Gas",
    "NSE:GSPL-EQ": "Oil & Gas",
    "NSE:ADANIGAS-EQ": "Oil & Gas",
    "NSE:GSFC-EQ": "Oil & Gas",
    
    # Power / Utilities
    "NSE:NTPC-EQ": "Power",
    "NSE:POWERGRID-EQ": "Power",
    "NSE:COALINDIA-EQ": "Power",
    "NSE:TATAPOWER-EQ": "Power",
    "NSE:ADANIPOWER-EQ": "Power",
    "NSE:ADANIGREEN-EQ": "Power",
    "NSE:JSW-ENERGY-EQ": "Power",
    "NSE:NHPC-EQ": "Power",
    "NSE:SJVN-EQ": "Power",
    "NSE:IREDA-EQ": "Power",
    "NSE:NTPCGREEN-EQ": "Power",
    "NSE:ADANIENSOL-EQ": "Power",
    "NSE:SUZLON-EQ": "Power",
    "NSE:INOXWIND-EQ": "Power",
    "NSE:ORIENTGEN-EQ": "Power",
    "NSE:JPPOWER-EQ": "Power",
    "NSE:JPINFRATEC-EQ": "Power",
    "NSE:RPOWER-EQ": "Power",
    "NSE:TORNTPOWER-EQ": "Power",
    "NSE:CESC-EQ": "Power",
    "NSE:TRENT-EQ": "Consumer Goods",
    "NSE:THERMAX-EQ": "Power",
    "NSE:KEC-EQ": "Power",
    "NSE:RTNPOWER-EQ": "Power",
    "NSE:JSWENERGY-EQ": "Power",
    "NSE:NTPC-EQ": "Power",
    "NSE:INOXGREEN-EQ": "Power",
    "NSE:WAAREEENER-EQ": "Power",
    "NSE:SWSOLAR-EQ": "Power",
    "NSE:SOLARINDS-EQ": "Power",
    "NSE:INOXWI-RE-EQ": "Power",
    "NSE:WEBSOL-EQ": "Power",
    "NSE:WEBELSOLAR-EQ": "Power",
    "NSE:GREENPOWER-EQ": "Power",
    "NSE:BOROSIL-EQ": "Power",
    
    # Automobiles
    "NSE:MARUTI-EQ": "Automobiles",
    "NSE:TATAMOTORS-EQ": "Automobiles",
    "NSE:M&M-EQ": "Automobiles",
    "NSE:EICHERMOT-EQ": "Automobiles",
    "NSE:BAJAJ-AUTO-EQ": "Automobiles",
    "NSE:HEROMOTOCO-EQ": "Automobiles",
    "NSE:TVSMOTOR-EQ": "Automobiles",
    "NSE:ASHOKLEY-EQ": "Automobiles",
    "NSE:ESCORTS-EQ": "Automobiles",
    "NSE:BALKRISIND-EQ": "Automobiles",
    "NSE:MRF-EQ": "Automobiles",
    "NSE:APOLLOTYRE-EQ": "Automobiles",
    "NSE:CEAT-EQ": "Automobiles",
    "NSE:JK-TYRE-EQ": "Automobiles",
    "NSE:MOTHERSON-EQ": "Automobiles",
    "NSE:BOSCHLTD-EQ": "Automobiles",
    "NSE:EXIDEIND-EQ": "Automobiles",
    "NSE:AMARON-EQ": "Automobiles",
    "NSE:SUNDARAM-EQ": "Automobiles",
    "NSE:TIINDIA-EQ": "Automobiles",
    "NSE:LUMAX-EQ": "Automobiles",
    "NSE:MINDA-EQ": "Automobiles",
    "NSE:ENDURANCE-EQ": "Automobiles",
    "NSE:SUPRAJIT-EQ": "Automobiles",
    "NSE:SUBROS-EQ": "Automobiles",
    "NSE:TEAMLEASE-EQ": "Automobiles",
    "NSE:FORCEMOT-EQ": "Automobiles",
    "NSE:SJS-EQ": "Automobiles",
    "NSE:SANSERA-EQ": "Automobiles",
    "NSE:SANDHAR-EQ": "Automobiles",
    "NSE:SCHAEFFLER-EQ": "Automobiles",
    "NSE:TALBROS-EQ": "Automobiles",
    "NSE:RALLIS-EQ": "Automobiles",
    "NSE:AAUTOIND-EQ": "Automobiles",
    "NSE:JAMNAAUTO-EQ": "Automobiles",
    "NSE:WHEELS-EQ": "Automobiles",
    "NSE:AUTOAXLES-EQ": "Automobiles",
    "NSE:PPAP-EQ": "Automobiles",
    "NSE:FIEM-EQ": "Automobiles",
    "NSE:GABRIEL-EQ": "Automobiles",
    "NSE:JTEKT-EQ": "Automobiles",
    "NSE:VARROC-EQ": "Automobiles",
    "NSE:MSUMI-EQ": "Automobiles",
    "NSE:UNOMINDA-EQ": "Automobiles",
    "NSE:UNIPARTS-EQ": "Automobiles",
    "NSE:RICOAUTO-EQ": "Automobiles",
    "NSE:RAMKRISHNA-EQ": "Automobiles",
    "NSE:ANANDRISHIJI-EQ": "Automobiles",
    "NSE:BAJAJHLD-EQ": "Automobiles",
    "NSE:VINATIORGA-EQ": "Automobiles",
    "NSE:BAJAJCON-EQ": "Automobiles",
    "NSE:HINDMOTORS-EQ": "Automobiles",
    "NSE:OMAXAUTO-EQ": "Automobiles",
    "NSE:BHEL-EQ": "Automobiles",
    "NSE:HINDCOPPER-EQ": "Automobiles",
    "NSE:ATULAUTO-EQ": "Automobiles",
    "NSE:SHIVAMILLS-EQ": "Automobiles",
    "NSE:CUMMINSIND-EQ": "Automobiles",
    "NSE:HONDAPOWER-EQ": "Automobiles",
    "NSE:KIRLOSKP-EQ": "Automobiles",
    "NSE:SETCO-EQ": "Automobiles",
    "NSE:MAGMA-EQ": "Automobiles",
    "NSE:OLECTRA-EQ": "Automobiles",
    "NSE:OLAELEC-EQ": "Automobiles",
    "NSE:HYUNDAI-EQ": "Automobiles",
    "NSE:MAHINDCIE-EQ": "Automobiles",
    "NSE:TATAELXSI-EQ": "Automobiles",
    
    # Metals & Mining
    "NSE:TATASTEEL-EQ": "Metals & Mining",
    "NSE:HINDALCO-EQ": "Metals & Mining",
    "NSE:JSWSTEEL-EQ": "Metals & Mining",
    "NSE:SAIL-EQ": "Metals & Mining",
    "NSE:VEDL-EQ": "Metals & Mining",
    "NSE:HINDZINC-EQ": "Metals & Mining",
    "NSE:JINDALSTEL-EQ": "Metals & Mining",
    "NSE:NMDC-EQ": "Metals & Mining",
    "NSE:MOIL-EQ": "Metals & Mining",
    "NSE:NATIONALUM-EQ": "Metals & Mining",
    "NSE:BALRAMCHIN-EQ": "Metals & Mining",
    "NSE:APL-EQ": "Metals & Mining",
    "NSE:RATNAMANI-EQ": "Metals & Mining",
    "NSE:WELSPUNIND-EQ": "Metals & Mining",
    "NSE:JINDALPOLY-EQ": "Metals & Mining",
    "NSE:ORIENTCEM-EQ": "Metals & Mining",
    "NSE:STEELXIND-EQ": "Metals & Mining",
    "NSE:LLOYDSME-EQ": "Metals & Mining",
    "NSE:VISAKAIND-EQ": "Metals & Mining",
    "NSE:ARSS-EQ": "Metals & Mining",
    "NSE:KALYANI-EQ": "Metals & Mining",
    "NSE:KALYANIFRG-EQ": "Metals & Mining",
    "NSE:GRAPHITE-EQ": "Metals & Mining",
    "NSE:UGARSUGAR-EQ": "Metals & Mining",
    "NSE:RSWM-EQ": "Metals & Mining",
    "NSE:RAIN-EQ": "Metals & Mining",
    "NSE:GRAVITA-EQ": "Metals & Mining",
    "NSE:GVKPIL-EQ": "Metals & Mining",
    "NSE:MANORG-EQ": "Metals &Mining",
    "NSE:JKLAKSHMI-EQ": "Metals & Mining",
    "NSE:SREESTEEL-EQ": "Metals & Mining",
    "NSE:SUNFLAG-EQ": "Metals & Mining",
    "NSE:FACOR-EQ": "Metals & Mining",
    "NSE:BHUSHAN-EQ": "Metals & Mining",
    "NSE:ROHLTD-EQ": "Metals & Mining",
    "NSE:ZENITHSTL-EQ": "Metals & Mining",
    "NSE:VISHNU-EQ": "Metals & Mining",
    "NSE:UTTAMSTL-EQ": "Metals & Mining",
    "NSE:INDIACEM-EQ": "Metals & Mining",
    "NSE:RAMCOCEM-EQ": "Metals & Mining",
    "NSE:DALMIA-EQ": "Metals & Mining",
    "NSE:CENTURYPLY-EQ": "Metals & Mining",
    "NSE:CENTEXT-EQ": "Metals & Mining",
    "NSE:MAGNESITA-EQ": "Metals & Mining",
    "NSE:ORIENTREFR-EQ": "Metals & Mining",
    "NSE:MADRASFERT-EQ": "Metals & Mining",
    "NSE:MANDHANA-EQ": "Metals & Mining",
    "NSE:RAMASTEEL-EQ": "Metals & Mining",
    "NSE:PALLADINESTEEL-EQ": "Metals & Mining",
    "NSE:PALREDTEC-EQ": "Metals & Mining",
    "NSE:SALSTEEL-EQ": "Metals & Mining",
    "NSE:VSTL-EQ": "Metals & Mining",
    "NSE:STEELCAS-EQ": "Metals & Mining",
    "NSE:STEELCITY-EQ": "Metals & Mining",
    "NSE:STEL-EQ": "Metals & Mining",
    "NSE:SUNSTEEL-EQ": "Metals & Mining",
    "NSE:MAHASTEEL-EQ": "Metals & Mining",
    "NSE:HISARMETAL-EQ": "Metals & Mining",
    "NSE:ISGEC-EQ": "Metals & Mining",
    "NSE:KDDL-EQ": "Metals & Mining",
    "NSE:KIOCL-EQ": "Metals & Mining",
    "NSE:MEP-EQ": "Metals & Mining",
    "NSE:METALFORGE-EQ": "Metals & Mining",
    "NSE:MITTAL-EQ": "Metals & Mining",
    "NSE:MUKANDLTD-EQ": "Metals & Mining",
    "NSE:NCML-EQ": "Metals & Mining",
    "NSE:ORISSAMINE-EQ": "Metals & Mining",
    "NSE:POKARNA-EQ": "Metals & Mining",
    "NSE:RAMCOIND-EQ": "Metals & Mining",
    "NSE:SAMTEL-EQ": "Metals & Mining",
    "NSE:SILGO-EQ": "Metals & Mining",
    "NSE:UTTAM-EQ": "Metals & Mining",
    "NSE:WALCHANNAG-EQ": "Metals & Mining",
    "NSE:WELSPUN-EQ": "Metals & Mining",
    "NSE:ADANIENT-EQ": "Metals & Mining",
    "NSE:BEML-EQ": "Metals & Mining",
    
    # Pharmaceutical & Healthcare
    "NSE:SUNPHARMA-EQ": "Pharmaceuticals",
    "NSE:DRREDDY-EQ": "Pharmaceuticals",
    "NSE:CIPLA-EQ": "Pharmaceuticals",
    "NSE:DIVISLAB-EQ": "Pharmaceuticals",
    "NSE:LUPIN-EQ": "Pharmaceuticals",
    "NSE:BIOCON-EQ": "Pharmaceuticals",
    "NSE:AUROPHARMA-EQ": "Pharmaceuticals",
    "NSE:TORNTPHARM-EQ": "Pharmaceuticals",
    "NSE:GLENMARK-EQ": "Pharmaceuticals",
    "NSE:CADILAHC-EQ": "Pharmaceuticals",
    "NSE:ALKEM-EQ": "Pharmaceuticals",
    "NSE:LALPATHLAB-EQ": "Pharmaceuticals",
    "NSE:METROPOLIS-EQ": "Pharmaceuticals",
    "NSE:FORTIS-EQ": "Pharmaceuticals",
    "NSE:APOLLOHOSP-EQ": "Pharmaceuticals",
    "NSE:HCG-EQ": "Pharmaceuticals",
    "NSE:MAXHEALTH-EQ": "Pharmaceuticals",
    "NSE:NARAYANHRU-EQ": "Pharmaceuticals",
    "NSE:RAINBOWHSPL-EQ": "Pharmaceuticals",
    "NSE:KRSNAA-EQ": "Pharmaceuticals",
    "NSE:MEDANTA-EQ": "Pharmaceuticals",
    "NSE:KIMS-EQ": "Pharmaceuticals",
    "NSE:SHALBY-EQ": "Pharmaceuticals",
    "NSE:THYROCARE-EQ": "Pharmaceuticals",
    "NSE:SEQUENT-EQ": "Pharmaceuticals",
    "NSE:GRANULES-EQ": "Pharmaceuticals",
    "NSE:LAURUSLABS-EQ": "Pharmaceuticals",
    "NSE:JUBLPHARMA-EQ": "Pharmaceuticals",
    "NSE:CAPLIN-EQ": "Pharmaceuticals",
    "NSE:AJANTPHARM-EQ": "Pharmaceuticals",
    "NSE:ERIS-EQ": "Pharmaceuticals",
    "NSE:SUVEN-EQ": "Pharmaceuticals",
    "NSE:NATCOPHARM-EQ": "Pharmaceuticals",
    "NSE:STRIDES-EQ": "Pharmaceuticals",
    "NSE:GUFICBIO-EQ": "Pharmaceuticals",
    "NSE:MARKSANS-EQ": "Pharmaceuticals",
    "NSE:SOLARA-EQ": "Pharmaceuticals",
    "NSE:ORCHPHARMA-EQ": "Pharmaceuticals",
    "NSE:IPCA-EQ": "Pharmaceuticals",
    "NSE:IPCALAB-EQ": "Pharmaceuticals",
    "NSE:SYNGENE-EQ": "Pharmaceuticals",
    "NSE:BLISSGVS-EQ": "Pharmaceuticals",
    "NSE:NEULANDLAB-EQ": "Pharmaceuticals",
    "NSE:MANKIND-EQ": "Pharmaceuticals",
    "NSE:EMCURE-EQ": "Pharmaceuticals",
    "NSE:PFIZER-EQ": "Pharmaceuticals",
    "NSE:GLAXO-EQ": "Pharmaceuticals",
    "NSE:ABBOTINDIA-EQ": "Pharmaceuticals",
    "NSE:SANOFI-EQ": "Pharmaceuticals",
    "NSE:NOVARTIS-EQ": "Pharmaceuticals",
    "NSE:MSD-EQ": "Pharmaceuticals",
    "NSE:BAYER-EQ": "Pharmaceuticals",
    "NSE:WOCKPHARMA-EQ": "Pharmaceuticals",
    "NSE:INDOCO-EQ": "Pharmaceuticals",
    "NSE:FDC-EQ": "Pharmaceuticals",
    "NSE:CENTRALDRUG-EQ": "Pharmaceuticals",
    "NSE:JAGSONPAL-EQ": "Pharmaceuticals",
    "NSE:ARISTO-EQ": "Pharmaceuticals",
    "NSE:ALEMBICLTD-EQ": "Pharmaceuticals",
    "NSE:UNICHEMLAB-EQ": "Pharmaceuticals",
    "NSE:MOREPEN-EQ": "Pharmaceuticals",
    "NSE:UNICHEM-EQ": "Pharmaceuticals",
    "NSE:ADVENZYMES-EQ": "Pharmaceuticals",
    "NSE:TATACHEM-EQ": "Pharmaceuticals",
    "NSE:DEEPAKNTR-EQ": "Pharmaceuticals",
    "NSE:PIDILITIND-EQ": "Pharmaceuticals",
    "NSE:AKZOINDIA-EQ": "Pharmaceuticals",
    
    # FMCG & Consumer Goods
    "NSE:HINDUNILVR-EQ": "FMCG",
    "NSE:ITC-EQ": "FMCG",
    "NSE:BRITANNIA-EQ": "FMCG",
    "NSE:NESTLEIND-EQ": "FMCG",
    "NSE:DABUR-EQ": "FMCG",
    "NSE:GODREJCP-EQ": "FMCG",
    "NSE:MARICO-EQ": "FMCG",
    "NSE:COLPAL-EQ": "FMCG",
    "NSE:EMAMILTD-EQ": "FMCG",
    "NSE:JYOTHYLAB-EQ": "FMCG",
    "NSE:GILLETTE-EQ": "FMCG",
    "NSE:PGHH-EQ": "FMCG",
    "NSE:TATACONSUM-EQ": "FMCG",
    "NSE:UBL-EQ": "FMCG",
    "NSE:PATANJALI-EQ": "FMCG",
    "NSE:RADICO-EQ": "FMCG",
    "NSE:MCDOWELL-EQ": "FMCG",
    "NSE:VSTIND-EQ": "FMCG",
    "NSE:KPRMILL-EQ": "FMCG",
    "NSE:WELSPUNLIV-EQ": "FMCG",
    "NSE:VMART-EQ": "FMCG",
    "NSE:SHOPERSTOP-EQ": "FMCG",
    "NSE:ADITYA-EQ": "FMCG",
    "NSE:VENKEYS-EQ": "FMCG",
    "NSE:HATSUN-EQ": "FMCG",
    "NSE:SULA-EQ": "FMCG",
    "NSE:TASTYBITE-EQ": "FMCG",
    "NSE:BIKAJI-EQ": "FMCG",
    "NSE:JUBLFOOD-EQ": "FMCG",
    "NSE:HERITGFOOD-EQ": "FMCG",
    "NSE:GOCOLORS-EQ": "FMCG",
    "NSE:NYKAA-EQ": "FMCG",
    "NSE:HONASA-EQ": "FMCG",
    "NSE:MANYAVAR-EQ": "FMCG",
    "NSE:AHLUWALIA-EQ": "FMCG",
    "NSE:RELAXO-EQ": "FMCG",
    "NSE:BATA-EQ": "FMCG",
    "NSE:LIBERTSHOE-EQ": "FMCG",
    "NSE:KHADIM-EQ": "FMCG",
    "NSE:MIRZA-EQ": "FMCG",
    "NSE:VIP-EQ": "FMCG",
    "NSE:SKUMAR-EQ": "FMCG",
    "NSE:SYMPHONY-EQ": "FMCG",
    "NSE:VOLTAS-EQ": "FMCG",
    "NSE:BLUESTARCO-EQ": "FMCG",
    "NSE:HAVELLS-EQ": "FMCG",
    "NSE:CROMPTON-EQ": "FMCG",
    "NSE:ORIENT-EQ": "FMCG",
    "NSE:WHIRLPOOL-EQ": "FMCG",
    "NSE:AMBER-EQ": "FMCG",
    "NSE:BAJAJHCARE-EQ": "FMCG",
    "NSE:VGUARD-EQ": "FMCG",
    "NSE:POLYCAB-EQ": "FMCG",
    "NSE:FINOLEX-EQ": "FMCG",
    "NSE:KEI-EQ": "FMCG",
    "NSE:DIXON-EQ": "FMCG",
    "NSE:TITAN-EQ": "FMCG",
    "NSE:KALYAN-EQ": "FMCG",
    "NSE:THANGAMAY-EQ": "FMCG",
    "NSE:SENCO-EQ": "FMCG",
    "NSE:TBZ-EQ": "FMCG",
    "NSE:PCJEWELLER-EQ": "FMCG",
    "NSE:GITANJALI-EQ": "FMCG",
    
    # Cement & Construction
    "NSE:ULTRACEMCO-EQ": "Cement",
    "NSE:AMBUJACEM-EQ": "Cement",
    "NSE:ACC-EQ": "Cement",
    "NSE:SHREECEM-EQ": "Cement",
    "NSE:JKCEMENT-EQ": "Cement",
    "NSE:HEIDELBERG-EQ": "Cement",
    "NSE:KAKATCEM-EQ": "Cement",
    "NSE:KESORAMIND-EQ": "Cement",
    "NSE:NUVOCO-EQ": "Cement",
    "NSE:STARCEMENT-EQ": "Cement",
    "NSE:PRISMCEM-EQ": "Cement",
    "NSE:UDAICEMENT-EQ": "Cement",
    "NSE:MAGADH-EQ": "Cement",
    "NSE:SAURASHCEM-EQ": "Cement",
    "NSE:MANGLMCEM-EQ": "Cement",
    "NSE:DECCAN-EQ": "Cement",
    
    # Construction & Infrastructure
    "NSE:LT-EQ": "Construction",
    "NSE:DLF-EQ": "Real Estate",
    "NSE:GODREJPROP-EQ": "Real Estate",
    "NSE:OBEROIRLTY-EQ": "Real Estate",
    "NSE:BRIGADE-EQ": "Real Estate",
    "NSE:PHOENIXMILLS-EQ": "Real Estate",
    "NSE:PRESTIGE-EQ": "Real Estate",
    "NSE:SOBHA-EQ": "Real Estate",
    "NSE:SUNTECK-EQ": "Real Estate",
    "NSE:KOLTEPATIL-EQ": "Real Estate",
    "NSE:MAHLIFE-EQ": "Real Estate",
    "NSE:LODHA-EQ": "Real Estate",
    "NSE:SIGNATURE-EQ": "Real Estate",
    "NSE:RUSTOMJEE-EQ": "Real Estate",
    "NSE:MIDHANI-EQ": "Construction",
    "NSE:IRCON-EQ": "Construction",
    "NSE:RITES-EQ": "Construction",
    "NSE:RVNL-EQ": "Construction",
    "NSE:RAILTEL-EQ": "Construction",
    "NSE:CONCOR-EQ": "Construction",
    "NSE:NCC-EQ": "Construction",
    "NSE:HCC-EQ": "Construction",
    "NSE:IRB-EQ": "Construction",
    "NSE:SADBHAV-EQ": "Construction",
    "NSE:ASHOKA-EQ": "Construction",
    "NSE:KNR-EQ": "Construction",
    "NSE:PNC-EQ": "Construction",
    "NSE:PATEL-EQ": "Construction",
    "NSE:NBCC-EQ": "Construction",
    "NSE:HUDCO-EQ": "Construction",
    "NSE:KALPATARU-EQ": "Construction",
    "NSE:GPIL-EQ": "Construction",
    "NSE:BRLM-EQ": "Construction",
    "NSE:IGARASHI-EQ": "Construction",
    "NSE:AIA-EQ": "Construction",
    "NSE:TITAGARH-EQ": "Construction",
    "NSE:TEXRAIL-EQ": "Construction",
    "NSE:MUKANDENG-EQ": "Construction",
    "NSE:BEL-EQ": "Construction",
    "NSE:HAL-EQ": "Construction",
    "NSE:GRSE-EQ": "Construction",
    "NSE:COCHINSHIP-EQ": "Construction",
    "NSE:MAZAGON-EQ": "Construction",
    "NSE:LXCHEM-EQ": "Construction",
    "NSE:HINDWAREAP-EQ": "Construction",
    "NSE:CERA-EQ": "Construction",
    "NSE:HSIL-EQ": "Construction",
    "NSE:SOMANY-EQ": "Construction",
    "NSE:KAJARIACER-EQ": "Construction",
    "NSE:ORIENTBELL-EQ": "Construction",
    "NSE:NITCO-EQ": "Construction",
    "NSE:ASTRAL-EQ": "Construction",
    "NSE:SUPREME-EQ": "Construction",
    "NSE:NILKAMAL-EQ": "Construction",
    "NSE:SINTEX-EQ": "Construction",
    "NSE:KANSAINER-EQ": "Construction",
    "NSE:PRINCEPIPE-EQ": "Construction",
    "NSE:APOLLOPIPE-EQ": "Construction",
    
    # Agriculture & Fertilizers
    "NSE:UPL-EQ": "Agriculture",
    "NSE:GODREJAGRO-EQ": "Agriculture",
    "NSE:SUMICHEM-EQ": "Agriculture",
    "NSE:BASF-EQ": "Agriculture",
    "NSE:INSECTICID-EQ": "Agriculture",
    "NSE:DHANUKA-EQ": "Agriculture",
    "NSE:SHARDACROP-EQ": "Agriculture",
    "NSE:HERANBA-EQ": "Agriculture",
    "NSE:BHARAT-EQ": "Agriculture",
    "NSE:FACT-EQ": "Agriculture",
    "NSE:RCF-EQ": "Agriculture",
    "NSE:NFL-EQ": "Agriculture",
    "NSE:CHAMBLFERT-EQ": "Agriculture",
    "NSE:KRIBHCO-EQ": "Agriculture",
    "NSE:ZUARIAGRO-EQ": "Agriculture",
    "NSE:DEEPAKFERT-EQ": "Agriculture",
    "NSE:MADRAS-EQ": "Agriculture",
    "NSE:SOUTHERN-EQ": "Agriculture",
    "NSE:MANGALORE-EQ": "Agriculture",
    "NSE:NAGARJUNA-EQ": "Agriculture",
    "NSE:PARADEEP-EQ": "Agriculture",
    "NSE:COROMANDEL-EQ": "Agriculture",
    "NSE:IFCO-EQ": "Agriculture",
    "NSE:KHAITAN-EQ": "Agriculture",
    "NSE:KRBL-EQ": "Agriculture",
    "NSE:USHAMART-EQ": "Agriculture",
    "NSE:LAXMIORG-EQ": "Agriculture",
    "NSE:PREMIER-EQ": "Agriculture",
    "NSE:AVANTIFEED-EQ": "Agriculture",
    "NSE:GODHA-EQ": "Agriculture",
    "NSE:RUCHISOYA-EQ": "Agriculture",
    "NSE:ADANIWILMAR-EQ": "Agriculture",
    "NSE:BAJAJHIND-EQ": "Agriculture",
    "NSE:JUBLAGRI-EQ": "Agriculture",
    "NSE:PARAS-EQ": "Agriculture",
    "NSE:JKAGRI-EQ": "Agriculture",
    "NSE:NAVRATNA-EQ": "Agriculture",
    "NSE:NATIONAL-EQ": "Agriculture",
    "NSE:RAJSHREE-EQ": "Agriculture",
    "NSE:DWARIKESH-EQ": "Agriculture",
    "NSE:TRIVENI-EQ": "Agriculture",
    "NSE:BALRAMPUR-EQ": "Agriculture",
    "NSE:KOTHARI-EQ": "Agriculture",
    "NSE:MAWANA-EQ": "Agriculture",
    "NSE:DHAMPURSUG-EQ": "Agriculture",
    "NSE:RENUKA-EQ": "Agriculture",
    "NSE:KSL-EQ": "Agriculture",
    "NSE:TIRUPATI-EQ": "Agriculture",
    "NSE:SAKAR-EQ": "Agriculture",
    "NSE:VISHWARAJ-EQ": "Agriculture",
    "NSE:SAKTISUG-EQ": "Agriculture",
    "NSE:ANDHRSUGAR-EQ": "Agriculture",
    "NSE:BANNARI-EQ": "Agriculture",
    "NSE:MAGADSUGAR-EQ": "Agriculture",
    "NSE:AVADHSUGAR-EQ": "Agriculture",
    
    # Textiles
    "NSE:ARVIND-EQ": "Textiles",
    "NSE:TRIDENT-EQ": "Textiles",
    "NSE:VARDHMAN-EQ": "Textiles",
    "NSE:SUTLEJ-EQ": "Textiles",
    "NSE:GRASIM-EQ": "Textiles",
    "NSE:SPENTEX-EQ": "Textiles",
    "NSE:INDORAMA-EQ": "Textiles",
    "NSE:FILATEX-EQ": "Textiles",
    "NSE:ALOKTEXT-EQ": "Textiles",
    "NSE:BTIL-EQ": "Textiles",
    "NSE:MAFATLAL-EQ": "Textiles",
    "NSE:RAYMOND-EQ": "Textiles",
    "NSE:VIPIND-EQ": "Textiles",
    "NSE:DONEAR-EQ": "Textiles",
    "NSE:HIMATSEIDE-EQ": "Textiles",
    "NSE:CENTUM-EQ": "Textiles",
    "NSE:DOLLAR-EQ": "Textiles",
    "NSE:KITEX-EQ": "Textiles",
    "NSE:SHIVTEX-EQ": "Textiles",
    "NSE:BANSWARA-EQ": "Textiles",
    "NSE:BSL-EQ": "Textiles",
    "NSE:ALBK-EQ": "Textiles",
    "NSE:BIRLA-EQ": "Textiles",
    "NSE:DHANVARSHA-EQ": "Textiles",
    "NSE:GTN-EQ": "Textiles",
    "NSE:GOKUL-EQ": "Textiles",
    "NSE:HIRA-EQ": "Textiles",
    "NSE:KGDENIM-EQ": "Textiles",
    "NSE:LOYAL-EQ": "Textiles",
    "NSE:MONACO-EQ": "Textiles",
    "NSE:MSP-EQ": "Textiles",
    "NSE:NAHAR-EQ": "Textiles",
    "NSE:NITIN-EQ": "Textiles",
    "NSE:PRADEEP-EQ": "Textiles",
    "NSE:SARLA-EQ": "Textiles",
    "NSE:SHANTIGEAR-EQ": "Textiles",
    "NSE:SOMATEX-EQ": "Textiles",
    "NSE:STYLAMIND-EQ": "Textiles",
    "NSE:TEXINFRA-EQ": "Textiles",
    "NSE:TEXMOPIPES-EQ": "Textiles",
    "NSE:UNIPHOS-EQ": "Textiles",
    "NSE:VARDHACRLC-EQ": "Textiles",
    "NSE:VARDMNPOLY-EQ": "Textiles",
    "NSE:WEIZMANIND-EQ": "Textiles",
    
    # Media & Entertainment
    "NSE:ZEEL-EQ": "Media",
    "NSE:SUNTV-EQ": "Media",
    "NSE:PVRINOX-EQ": "Media",
    "NSE:NETWORK18-EQ": "Media",
    "NSE:TV18BRDCST-EQ": "Media",
    "NSE:JAGRAN-EQ": "Media",
    "NSE:SAREGAMA-EQ": "Media",
    "NSE:TIPSFILMS-EQ": "Media",
    "NSE:TIPSMUSIC-EQ": "Media",
    "NSE:RADIOCITY-EQ": "Media",
    "NSE:DBCORP-EQ": "Media",
    "NSE:HTMEDIA-EQ": "Media",
    "NSE:NAVNETEDUL-EQ": "Media",
    "NSE:NAZARA-EQ": "Media",
    "NSE:ONMOBILE-EQ": "Media",
    "NSE:UFO-EQ": "Media",
    "NSE:EROS-EQ": "Media",
    "NSE:BALAJITELE-EQ": "Media",
    "NSE:CINELINE-EQ": "Media",
    "NSE:CINEVISTA-EQ": "Media",
    "NSE:CELEBRITY-EQ": "Media",
    "NSE:SHEMAROO-EQ": "Media",
    "NSE:YASHRAJ-EQ": "Media",
    "NSE:PRITIKA-EQ": "Media",
    "NSE:RELCAPITAL-EQ": "Media",
    "NSE:RELMEDIA-EQ": "Media",
    "NSE:NEXTMEDIA-EQ": "Media",
    
    # Telecommunications
    "NSE:BHARTIARTL-EQ": "Telecommunications",
    "NSE:RJIO-EQ": "Telecommunications",
    "NSE:IDEA-EQ": "Telecommunications",
    "NSE:BSNL-EQ": "Telecommunications",
    "NSE:MTNL-EQ": "Telecommunications",
    "NSE:HFCL-EQ": "Telecommunications",
    "NSE:STLTECH-EQ": "Telecommunications",
    "NSE:GTPL-EQ": "Telecommunications",
    "NSE:DEN-EQ": "Telecommunications",
    "NSE:HATHWAY-EQ": "Telecommunications",
    "NSE:SITI-EQ": "Telecommunications",
    "NSE:ORTEL-EQ": "Telecommunications",
    "NSE:TEJAS-EQ": "Telecommunications",
    "NSE:RCOM-EQ": "Telecommunications",
    "NSE:OPTIEMUS-EQ": "Telecommunications",
    "NSE:ONEPOINT-EQ": "Telecommunications",
    "NSE:CIGNITITEC-EQ": "Telecommunications",
    "NSE:SMARTLINK-EQ": "Telecommunications",
    "NSE:VINDHYATEL-EQ": "Telecommunications",
    "NSE:TATACOMM-EQ": "Telecommunications",
    "NSE:TANLA-EQ": "Telecommunications",
    "NSE:ROUTE-EQ": "Telecommunications",
    "NSE:ZENTEC-EQ": "Telecommunications",
    "NSE:MOSCHIP-EQ": "Telecommunications",
    
    # Travel & Transportation
    "NSE:INDIGO-EQ": "Travel & Transport",
    "NSE:SPICEJET-EQ": "Travel & Transport",
    "NSE:JETAIRWAYS-EQ": "Travel & Transport",
    "NSE:TCI-EQ": "Travel & Transport",
    "NSE:VTL-EQ": "Travel & Transport",
    "NSE:ALLCARGO-EQ": "Travel & Transport",
    "NSE:BLUEDART-EQ": "Travel & Transport",
    "NSE:DELHIVERY-EQ": "Travel & Transport",
    "NSE:MAHLOG-EQ": "Travel & Transport",
    "NSE:SICAL-EQ": "Travel & Transport",
    "NSE:SNOWMAN-EQ": "Travel & Transport",
    "NSE:GATI-EQ": "Travel & Transport",
    "NSE:APOLLO-EQ": "Travel & Transport",
    "NSE:AEGISLOG-EQ": "Travel & Transport",
    "NSE:THOMASCOOK-EQ": "Travel & Transport",
    "NSE:COX&KINGS-EQ": "Travel & Transport",
    "NSE:KESARENT-EQ": "Travel & Transport",
    "NSE:YATRA-EQ": "Travel & Transport",
    "NSE:MAKEMYTRIP-EQ": "Travel & Transport",
    "NSE:EASEMYTRIP-EQ": "Travel & Transport",
    "NSE:IXIGO-EQ": "Travel & Transport",
    "NSE:ADANIPORTS-EQ": "Travel & Transport",
    "NSE:JSWINFRA-EQ": "Travel & Transport",
    "NSE:MHRIL-EQ": "Travel & Transport",
    "NSE:ESSELPACK-EQ": "Travel & Transport",
    "NSE:SAGCEM-EQ": "Travel & Transport",
    
    # Hotels & Tourism
    "NSE:INDIANHOTELS-EQ": "Hotels & Tourism",
    "NSE:LEMONTREE-EQ": "Hotels & Tourism",
    "NSE:CHALET-EQ": "Hotels & Tourism",
    "NSE:MAHINDRA-EQ": "Hotels & Tourism",
    "NSE:EIHOTEL-EQ": "Hotels & Tourism",
    "NSE:ITCHOTELS-EQ": "Hotels & Tourism",
    "NSE:ORIENTHOT-EQ": "Hotels & Tourism",
    "NSE:LEMON-EQ": "Hotels & Tourism",
    "NSE:TGBHOTELS-EQ": "Hotels & Tourism",
    "NSE:PARKHOTELS-EQ": "Hotels & Tourism",
    "NSE:KAMAT-EQ": "Hotels & Tourism",
    "NSE:ADVANI-EQ": "Hotels & Tourism",
    "NSE:SAMHI-EQ": "Hotels & Tourism",
    
    # Diversified & Conglomerates
    "NSE:RELIANCE-EQ": "Diversified",
    "NSE:ADANIENT-EQ": "Diversified",
    "NSE:ITC-EQ": "Diversified",
    "NSE:BAJAJHLDNG-EQ": "Diversified",
    "NSE:GODREJIND-EQ": "Diversified",
    "NSE:LT-EQ": "Diversified",
    "NSE:SIEMENS-EQ": "Diversified",
    "NSE:ABB-EQ": "Diversified",
    "NSE:HONEYWELL-EQ": "Diversified",
    "NSE:3M-EQ": "Diversified",
    "NSE:TATA-EQ": "Diversified",
    "NSE:BHARTI-EQ": "Diversified",
    "NSE:ESSAR-EQ": "Diversified",
    "NSE:JAIPRAKASH-EQ": "Diversified",
    "NSE:GAMMON-EQ": "Diversified",
    "NSE:PUNJ-EQ": "Diversified",
    "NSE:LANCO-EQ": "Diversified",
    "NSE:GMR-EQ": "Diversified",
    "NSE:GVK-EQ": "Diversified",
    "NSE:SIMPLEX-EQ": "Diversified",
    "NSE:EMKAY-EQ": "Diversified",
}

def get_sector_for_symbol(symbol):
    """Get sector for a given symbol"""
    return SECTOR_MAPPING.get(symbol, "Others")

# =============================================================================
# STOCK SYMBOLS (keeping existing list)
# =============================================================================

STOCK_SYMBOLS = ['NSE:TCS-EQ', 'NSE:INFY-EQ', 'NSE:WIPRO-EQ', 'NSE:HCLTECH-EQ', 'NSE:TECHM-EQ', 'NSE:LTIM-EQ', 'NSE:LTTS-EQ', 'NSE:MINDTREE-EQ', 'NSE:COFORGE-EQ', 'NSE:PERSISTENT-EQ', 'NSE:CYIENT-EQ', 'NSE:MPHASIS-EQ', 'NSE:INTELLECT-EQ', 'NSE:TATAELXSI-EQ', 'NSE:KPITTECH-EQ', 'NSE:MASTEK-EQ', 'NSE:NEWGEN-EQ', 'NSE:CYIENTDLM-EQ', 'NSE:OFSS-EQ', 'NSE:ZENSAR-EQ', 'NSE:HDFCBANK-EQ', 'NSE:ICICIBANK-EQ', 'NSE:AXISBANK-EQ', 'NSE:SBIN-EQ', 'NSE:KOTAKBANK-EQ', 'NSE:INDUSINDBK-EQ', 'NSE:BANDHANBNK-EQ', 'NSE:IDFCFIRSTB-EQ', 'NSE:FEDERALBNK-EQ', 'NSE:RBLBANK-EQ', 'NSE:YESBANK-EQ', 'NSE:AUBANK-EQ', 'NSE:BANKBARODA-EQ', 'NSE:PNB-EQ', 'NSE:CANBK-EQ', 'NSE:UNIONBANK-EQ', 'NSE:BANKINDIA-EQ', 'NSE:CENTRALBK-EQ', 'NSE:IOB-EQ', 'NSE:PSB-EQ', 'NSE:IDBI-EQ', 'NSE:UCOBANK-EQ', 'NSE:INDIANB-EQ', 'NSE:CSBBANK-EQ', 'NSE:DCBBANK-EQ', 'NSE:SOUTHBANK-EQ', 'NSE:TMB-EQ', 'NSE:KTKBANK-EQ', 'NSE:J&KBANK-EQ', 'NSE:DHANBANK-EQ', 'NSE:MAHABANK-EQ', 'NSE:KARURVYSYA-EQ', 'NSE:CUB-EQ', 'NSE:UTKARSHBNK-EQ', 'NSE:ESAFSFB-EQ', 'NSE:UJJIVANSFB-EQ', 'NSE:EQUITASBNK-EQ', 'NSE:CAPITALSFB-EQ', 'NSE:SURYODAY-EQ', 'NSE:FINPIPE-EQ', 'NSE:BAJFINANCE-EQ', 'NSE:BAJAJFINSV-EQ', 'NSE:HDFCLIFE-EQ', 'NSE:SBILIFE-EQ', 'NSE:ICICIGI-EQ', 'NSE:ICICIPRULI-EQ', 'NSE:LICI-EQ', 'NSE:NIACL-EQ', 'NSE:GODIGIT-EQ', 'NSE:STARHEALTH-EQ', 'NSE:NIVABUPA-EQ', 'NSE:HDFCAMC-EQ', 'NSE:UTIAMC-EQ', 'NSE:CRISIL-EQ', 'NSE:CREDITACC-EQ', 'NSE:BFSL-EQ', 'NSE:CHOLAFIN-EQ', 'NSE:MUTHOOTFIN-EQ', 'NSE:MANAPPURAM-EQ', 'NSE:PFC-EQ', 'NSE:RECLTD-EQ', 'NSE:IRFC-EQ', 'NSE:EDELWEISS-EQ', 'NSE:IIFL-EQ', 'NSE:M&MFIN-EQ', 'NSE:SHRIRAMFIN-EQ', 'NSE:BAJAJHFL-EQ', 'NSE:CANFINHOME-EQ', 'NSE:LICHSGFIN-EQ', 'NSE:PNBHOUSING-EQ', 'NSE:REPCO-EQ', 'NSE:HOMEFIRST-EQ', 'NSE:INDOSTAR-EQ', 'NSE:SPANDANA-EQ', 'NSE:PAISALO-EQ', 'NSE:JSFB-EQ', 'NSE:SBFC-EQ', 'NSE:ASIANFIN-EQ', 'NSE:RELIGARE-EQ', 'NSE:MOTILALOFS-EQ', 'NSE:ANGELONE-EQ', 'NSE:ANANDRATHI-EQ', 'NSE:ARIHANTCAP-EQ', 'NSE:GEOJITFSL-EQ', 'NSE:NUVAMA-EQ', 'NSE:KFINTECH-EQ', 'NSE:CDSL-EQ', 'NSE:BSE-EQ', 'NSE:MCX-EQ', 'NSE:IEX-EQ', 'NSE:CAMS-EQ', 'NSE:JIOFIN-EQ', 'NSE:RELIANCE-EQ', 'NSE:ONGC-EQ', 'NSE:IOC-EQ', 'NSE:BPCL-EQ', 'NSE:HINDPETRO-EQ', 'NSE:GAIL-EQ', 'NSE:OIL-EQ', 'NSE:MGL-EQ', 'NSE:IGL-EQ', 'NSE:GUJGASLTD-EQ', 'NSE:ATGL-EQ', 'NSE:CASTROLIND-EQ', 'NSE:GULF-EQ', 'NSE:GULFOILLUB-EQ', 'NSE:GULFPETRO-EQ', 'NSE:HINDOILEXP-EQ', 'NSE:SELAN-EQ', 'NSE:MRPL-EQ', 'NSE:TNPETRO-EQ', 'NSE:CHENNPETRO-EQ', 'NSE:HINDNATGLS-EQ', 'NSE:GSPL-EQ', 'NSE:ADANIGAS-EQ', 'NSE:GSFC-EQ', 'NSE:NTPC-EQ', 'NSE:POWERGRID-EQ', 'NSE:COALINDIA-EQ', 'NSE:TATAPOWER-EQ', 'NSE:ADANIPOWER-EQ', 'NSE:ADANIGREEN-EQ', 'NSE:JSW-ENERGY-EQ', 'NSE:NHPC-EQ', 'NSE:SJVN-EQ', 'NSE:IREDA-EQ', 'NSE:NTPCGREEN-EQ', 'NSE:ADANIENSOL-EQ', 'NSE:SUZLON-EQ', 'NSE:INOXWIND-EQ', 'NSE:ORIENTGEN-EQ', 'NSE:JPPOWER-EQ', 'NSE:JPINFRATEC-EQ', 'NSE:RPOWER-EQ', 'NSE:TORNTPOWER-EQ', 'NSE:CESC-EQ', 'NSE:TRENT-EQ', 'NSE:THERMAX-EQ', 'NSE:KEC-EQ', 'NSE:RTNPOWER-EQ', 'NSE:JSWENERGY-EQ', 'NSE:INOXGREEN-EQ', 'NSE:WAAREEENER-EQ', 'NSE:SWSOLAR-EQ', 'NSE:SOLARINDS-EQ', 'NSE:INOXWI-RE-EQ', 'NSE:WEBSOL-EQ', 'NSE:WEBELSOLAR-EQ', 'NSE:GREENPOWER-EQ', 'NSE:BOROSIL-EQ', 'NSE:MARUTI-EQ', 'NSE:TATAMOTORS-EQ', 'NSE:M&M-EQ', 'NSE:EICHERMOT-EQ', 'NSE:BAJAJ-AUTO-EQ', 'NSE:HEROMOTOCO-EQ', 'NSE:TVSMOTOR-EQ', 'NSE:ASHOKLEY-EQ', 'NSE:ESCORTS-EQ', 'NSE:BALKRISIND-EQ', 'NSE:MRF-EQ', 'NSE:APOLLOTYRE-EQ', 'NSE:CEAT-EQ', 'NSE:JK-TYRE-EQ', 'NSE:MOTHERSON-EQ', 'NSE:BOSCHLTD-EQ', 'NSE:EXIDEIND-EQ', 'NSE:AMARON-EQ', 'NSE:SUNDARAM-EQ', 'NSE:TIINDIA-EQ', 'NSE:LUMAX-EQ', 'NSE:MINDA-EQ', 'NSE:ENDURANCE-EQ', 'NSE:SUPRAJIT-EQ', 'NSE:SUBROS-EQ', 'NSE:TEAMLEASE-EQ', 'NSE:FORCEMOT-EQ', 'NSE:SJS-EQ', 'NSE:SANSERA-EQ', 'NSE:SANDHAR-EQ', 'NSE:SCHAEFFLER-EQ', 'NSE:TALBROS-EQ', 'NSE:RALLIS-EQ', 'NSE:AAUTOIND-EQ', 'NSE:JAMNAAUTO-EQ', 'NSE:WHEELS-EQ', 'NSE:AUTOAXLES-EQ', 'NSE:PPAP-EQ', 'NSE:FIEM-EQ', 'NSE:GABRIEL-EQ', 'NSE:JTEKT-EQ', 'NSE:VARROC-EQ', 'NSE:MSUMI-EQ', 'NSE:UNOMINDA-EQ', 'NSE:UNIPARTS-EQ', 'NSE:RICOAUTO-EQ', 'NSE:RAMKRISHNA-EQ', 'NSE:ANANDRISHIJI-EQ', 'NSE:BAJAJHLD-EQ', 'NSE:VINATIORGA-EQ', 'NSE:BAJAJCON-EQ', 'NSE:HINDMOTORS-EQ', 'NSE:OMAXAUTO-EQ', 'NSE:BHEL-EQ', 'NSE:HINDCOPPER-EQ', 'NSE:ATULAUTO-EQ', 'NSE:SHIVAMILLS-EQ', 'NSE:CUMMINSIND-EQ', 'NSE:HONDAPOWER-EQ', 'NSE:KIRLOSKP-EQ', 'NSE:SETCO-EQ', 'NSE:MAGMA-EQ', 'NSE:OLECTRA-EQ', 'NSE:OLAELEC-EQ', 'NSE:HYUNDAI-EQ', 'NSE:MAHINDCIE-EQ', 'NSE:TATASTEEL-EQ', 'NSE:HINDALCO-EQ', 'NSE:JSWSTEEL-EQ', 'NSE:SAIL-EQ', 'NSE:VEDL-EQ', 'NSE:HINDZINC-EQ', 'NSE:JINDALSTEL-EQ', 'NSE:NMDC-EQ', 'NSE:MOIL-EQ', 'NSE:NATIONALUM-EQ', 'NSE:BALRAMCHIN-EQ', 'NSE:APL-EQ', 'NSE:RATNAMANI-EQ', 'NSE:WELSPUNIND-EQ', 'NSE:JINDALPOLY-EQ', 'NSE:ORIENTCEM-EQ', 'NSE:STEELXIND-EQ', 'NSE:LLOYDSME-EQ', 'NSE:VISAKAIND-EQ', 'NSE:ARSS-EQ', 'NSE:KALYANI-EQ', 'NSE:KALYANIFRG-EQ', 'NSE:GRAPHITE-EQ', 'NSE:UGARSUGAR-EQ', 'NSE:RSWM-EQ', 'NSE:RAIN-EQ', 'NSE:GRAVITA-EQ', 'NSE:GVKPIL-EQ', 'NSE:MANORG-EQ', 'NSE:JKLAKSHMI-EQ', 'NSE:SREESTEEL-EQ', 'NSE:SUNFLAG-EQ', 'NSE:FACOR-EQ', 'NSE:BHUSHAN-EQ', 'NSE:ROHLTD-EQ', 'NSE:ZENITHSTL-EQ', 'NSE:VISHNU-EQ', 'NSE:UTTAMSTL-EQ', 'NSE:INDIACEM-EQ', 'NSE:RAMCOCEM-EQ', 'NSE:DALMIA-EQ', 'NSE:CENTURYPLY-EQ', 'NSE:CENTEXT-EQ', 'NSE:MAGNESITA-EQ', 'NSE:ORIENTREFR-EQ', 'NSE:MADRASFERT-EQ', 'NSE:MANDHANA-EQ', 'NSE:RAMASTEEL-EQ', 'NSE:PALLADINESTEEL-EQ', 'NSE:PALREDTEC-EQ', 'NSE:SALSTEEL-EQ', 'NSE:VSTL-EQ', 'NSE:STEELCAS-EQ', 'NSE:STEELCITY-EQ', 'NSE:STEL-EQ', 'NSE:SUNSTEEL-EQ', 'NSE:MAHASTEEL-EQ', 'NSE:HISARMETAL-EQ', 'NSE:ISGEC-EQ', 'NSE:KDDL-EQ', 'NSE:KIOCL-EQ', 'NSE:MEP-EQ', 'NSE:METALFORGE-EQ', 'NSE:MITTAL-EQ', 'NSE:MUKANDLTD-EQ', 'NSE:NCML-EQ', 'NSE:ORISSAMINE-EQ', 'NSE:POKARNA-EQ', 'NSE:RAMCOIND-EQ', 'NSE:SAMTEL-EQ', 'NSE:SILGO-EQ', 'NSE:UTTAM-EQ', 'NSE:WALCHANNAG-EQ', 'NSE:WELSPUN-EQ', 'NSE:ADANIENT-EQ', 'NSE:BEML-EQ', 'NSE:SUNPHARMA-EQ', 'NSE:DRREDDY-EQ', 'NSE:CIPLA-EQ', 'NSE:DIVISLAB-EQ', 'NSE:LUPIN-EQ', 'NSE:BIOCON-EQ', 'NSE:AUROPHARMA-EQ', 'NSE:TORNTPHARM-EQ', 'NSE:GLENMARK-EQ', 'NSE:CADILAHC-EQ', 'NSE:ALKEM-EQ', 'NSE:LALPATHLAB-EQ', 'NSE:METROPOLIS-EQ', 'NSE:FORTIS-EQ', 'NSE:APOLLOHOSP-EQ', 'NSE:HCG-EQ', 'NSE:MAXHEALTH-EQ', 'NSE:NARAYANHRU-EQ', 'NSE:RAINBOWHSPL-EQ', 'NSE:KRSNAA-EQ', 'NSE:MEDANTA-EQ', 'NSE:KIMS-EQ', 'NSE:SHALBY-EQ', 'NSE:THYROCARE-EQ', 'NSE:SEQUENT-EQ', 'NSE:GRANULES-EQ', 'NSE:LAURUSLABS-EQ', 'NSE:JUBLPHARMA-EQ', 'NSE:CAPLIN-EQ', 'NSE:AJANTPHARM-EQ', 'NSE:ERIS-EQ', 'NSE:SUVEN-EQ', 'NSE:NATCOPHARM-EQ', 'NSE:STRIDES-EQ', 'NSE:GUFICBIO-EQ', 'NSE:MARKSANS-EQ', 'NSE:SOLARA-EQ', 'NSE:ORCHPHARMA-EQ', 'NSE:IPCA-EQ', 'NSE:IPCALAB-EQ', 'NSE:SYNGENE-EQ', 'NSE:BLISSGVS-EQ', 'NSE:NEULANDLAB-EQ', 'NSE:MANKIND-EQ', 'NSE:EMCURE-EQ', 'NSE:PFIZER-EQ', 'NSE:GLAXO-EQ', 'NSE:ABBOTINDIA-EQ', 'NSE:SANOFI-EQ', 'NSE:NOVARTIS-EQ', 'NSE:MSD-EQ', 'NSE:BAYER-EQ', 'NSE:WOCKPHARMA-EQ', 'NSE:INDOCO-EQ', 'NSE:FDC-EQ', 'NSE:CENTRALDRUG-EQ', 'NSE:JAGSONPAL-EQ', 'NSE:ARISTO-EQ', 'NSE:ALEMBICLTD-EQ', 'NSE:UNICHEMLAB-EQ', 'NSE:MOREPEN-EQ', 'NSE:UNICHEM-EQ', 'NSE:ADVENZYMES-EQ', 'NSE:TATACHEM-EQ', 'NSE:DEEPAKNTR-EQ', 'NSE:PIDILITIND-EQ', 'NSE:AKZOINDIA-EQ', 'NSE:HINDUNILVR-EQ', 'NSE:ITC-EQ', 'NSE:BRITANNIA-EQ', 'NSE:NESTLEIND-EQ', 'NSE:DABUR-EQ', 'NSE:GODREJCP-EQ', 'NSE:MARICO-EQ', 'NSE:COLPAL-EQ', 'NSE:EMAMILTD-EQ', 'NSE:JYOTHYLAB-EQ', 'NSE:GILLETTE-EQ', 'NSE:PGHH-EQ', 'NSE:TATACONSUM-EQ', 'NSE:UBL-EQ', 'NSE:PATANJALI-EQ', 'NSE:RADICO-EQ', 'NSE:MCDOWELL-EQ', 'NSE:VSTIND-EQ', 'NSE:KPRMILL-EQ', 'NSE:WELSPUNLIV-EQ', 'NSE:VMART-EQ', 'NSE:SHOPERSTOP-EQ', 'NSE:ADITYA-EQ', 'NSE:VENKEYS-EQ', 'NSE:HATSUN-EQ', 'NSE:SULA-EQ', 'NSE:TASTYBITE-EQ', 'NSE:BIKAJI-EQ', 'NSE:JUBLFOOD-EQ', 'NSE:HERITGFOOD-EQ', 'NSE:GOCOLORS-EQ', 'NSE:NYKAA-EQ', 'NSE:HONASA-EQ', 'NSE:MANYAVAR-EQ', 'NSE:AHLUWALIA-EQ', 'NSE:RELAXO-EQ', 'NSE:BATA-EQ', 'NSE:LIBERTSHOE-EQ', 'NSE:KHADIM-EQ', 'NSE:MIRZA-EQ', 'NSE:VIP-EQ', 'NSE:SKUMAR-EQ', 'NSE:SYMPHONY-EQ', 'NSE:VOLTAS-EQ', 'NSE:BLUESTARCO-EQ', 'NSE:HAVELLS-EQ', 'NSE:CROMPTON-EQ', 'NSE:ORIENT-EQ', 'NSE:WHIRLPOOL-EQ', 'NSE:AMBER-EQ', 'NSE:BAJAJHCARE-EQ', 'NSE:VGUARD-EQ', 'NSE:POLYCAB-EQ', 'NSE:FINOLEX-EQ', 'NSE:KEI-EQ', 'NSE:DIXON-EQ', 'NSE:TITAN-EQ', 'NSE:KALYAN-EQ', 'NSE:THANGAMAY-EQ', 'NSE:SENCO-EQ', 'NSE:TBZ-EQ', 'NSE:PCJEWELLER-EQ', 'NSE:GITANJALI-EQ', 'NSE:ULTRACEMCO-EQ', 'NSE:AMBUJACEM-EQ', 'NSE:ACC-EQ', 'NSE:SHREECEM-EQ', 'NSE:JKCEMENT-EQ', 'NSE:HEIDELBERG-EQ', 'NSE:KAKATCEM-EQ', 'NSE:KESORAMIND-EQ', 'NSE:NUVOCO-EQ', 'NSE:STARCEMENT-EQ', 'NSE:PRISMCEM-EQ', 'NSE:UDAICEMENT-EQ', 'NSE:MAGADH-EQ', 'NSE:SAURASHCEM-EQ', 'NSE:MANGLMCEM-EQ', 'NSE:DECCAN-EQ', 'NSE:LT-EQ', 'NSE:DLF-EQ', 'NSE:GODREJPROP-EQ', 'NSE:OBEROIRLTY-EQ', 'NSE:BRIGADE-EQ', 'NSE:PHOENIXMILLS-EQ', 'NSE:PRESTIGE-EQ', 'NSE:SOBHA-EQ', 'NSE:SUNTECK-EQ', 'NSE:KOLTEPATIL-EQ', 'NSE:MAHLIFE-EQ', 'NSE:LODHA-EQ', 'NSE:SIGNATURE-EQ', 'NSE:RUSTOMJEE-EQ', 'NSE:MIDHANI-EQ', 'NSE:IRCON-EQ', 'NSE:RITES-EQ', 'NSE:RVNL-EQ', 'NSE:RAILTEL-EQ', 'NSE:CONCOR-EQ', 'NSE:NCC-EQ', 'NSE:HCC-EQ', 'NSE:IRB-EQ', 'NSE:SADBHAV-EQ', 'NSE:ASHOKA-EQ', 'NSE:KNR-EQ', 'NSE:PNC-EQ', 'NSE:PATEL-EQ', 'NSE:NBCC-EQ', 'NSE:HUDCO-EQ', 'NSE:KALPATARU-EQ', 'NSE:GPIL-EQ', 'NSE:BRLM-EQ', 'NSE:IGARASHI-EQ', 'NSE:AIA-EQ', 'NSE:TITAGARH-EQ', 'NSE:TEXRAIL-EQ', 'NSE:MUKANDENG-EQ', 'NSE:BEL-EQ', 'NSE:HAL-EQ', 'NSE:GRSE-EQ', 'NSE:COCHINSHIP-EQ', 'NSE:MAZAGON-EQ', 'NSE:LXCHEM-EQ', 'NSE:HINDWAREAP-EQ', 'NSE:CERA-EQ', 'NSE:HSIL-EQ', 'NSE:SOMANY-EQ', 'NSE:KAJARIACER-EQ', 'NSE:ORIENTBELL-EQ', 'NSE:NITCO-EQ', 'NSE:ASTRAL-EQ', 'NSE:SUPREME-EQ', 'NSE:NILKAMAL-EQ', 'NSE:SINTEX-EQ', 'NSE:KANSAINER-EQ', 'NSE:PRINCEPIPE-EQ', 'NSE:APOLLOPIPE-EQ', 'NSE:UPL-EQ', 'NSE:GODREJAGRO-EQ', 'NSE:SUMICHEM-EQ', 'NSE:BASF-EQ', 'NSE:INSECTICID-EQ', 'NSE:DHANUKA-EQ', 'NSE:SHARDACROP-EQ', 'NSE:HERANBA-EQ','NSE:BHARAT-EQ', 'NSE:FACT-EQ', 'NSE:RCF-EQ', 'NSE:NFL-EQ', 'NSE:CHAMBLFERT-EQ', 'NSE:KRIBHCO-EQ', 'NSE:ZUARIAGRO-EQ', 'NSE:DEEPAKFERT-EQ', 'NSE:MADRAS-EQ', 'NSE:SOUTHERN-EQ', 'NSE:MANGALORE-EQ', 'NSE:NAGARJUNA-EQ', 'NSE:PARADEEP-EQ', 'NSE:COROMANDEL-EQ', 'NSE:IFCO-EQ', 'NSE:KHAITAN-EQ', 'NSE:KRBL-EQ', 'NSE:USHAMART-EQ', 'NSE:LAXMIORG-EQ', 'NSE:PREMIER-EQ', 'NSE:AVANTIFEED-EQ', 'NSE:GODHA-EQ', 'NSE:RUCHISOYA-EQ', 'NSE:ADANIWILMAR-EQ', 'NSE:BAJAJHIND-EQ', 'NSE:JUBLAGRI-EQ', 'NSE:PARAS-EQ', 'NSE:JKAGRI-EQ', 'NSE:NAVRATNA-EQ', 'NSE:NATIONAL-EQ', 'NSE:RAJSHREE-EQ', 'NSE:DWARIKESH-EQ', 'NSE:TRIVENI-EQ', 'NSE:BALRAMPUR-EQ', 'NSE:KOTHARI-EQ', 'NSE:MAWANA-EQ', 'NSE:DHAMPURSUG-EQ', 'NSE:RENUKA-EQ', 'NSE:KSL-EQ', 'NSE:TIRUPATI-EQ', 'NSE:SAKAR-EQ', 'NSE:VISHWARAJ-EQ', 'NSE:SAKTISUG-EQ', 'NSE:ANDHRSUGAR-EQ', 'NSE:BANNARI-EQ', 'NSE:MAGADSUGAR-EQ', 'NSE:AVADHSUGAR-EQ', 'NSE:ARVIND-EQ', 'NSE:TRIDENT-EQ', 'NSE:VARDHMAN-EQ', 'NSE:SUTLEJ-EQ', 'NSE:GRASIM-EQ', 'NSE:SPENTEX-EQ', 'NSE:INDORAMA-EQ', 'NSE:FILATEX-EQ', 'NSE:ALOKTEXT-EQ', 'NSE:BTIL-EQ', 'NSE:MAFATLAL-EQ', 'NSE:RAYMOND-EQ', 'NSE:VIPIND-EQ', 'NSE:DONEAR-EQ', 'NSE:HIMATSEIDE-EQ', 'NSE:CENTUM-EQ', 'NSE:DOLLAR-EQ', 'NSE:KITEX-EQ', 'NSE:SHIVTEX-EQ', 'NSE:BANSWARA-EQ', 'NSE:BSL-EQ', 'NSE:ALBK-EQ', 'NSE:BIRLA-EQ', 'NSE:DHANVARSHA-EQ', 'NSE:GTN-EQ', 'NSE:GOKUL-EQ', 'NSE:HIRA-EQ', 'NSE:KGDENIM-EQ', 'NSE:LOYAL-EQ', 'NSE:MONACO-EQ', 'NSE:MSP-EQ', 'NSE:NAHAR-EQ', 'NSE:NITIN-EQ', 'NSE:PRADEEP-EQ', 'NSE:SARLA-EQ', 'NSE:SHANTIGEAR-EQ', 'NSE:SOMATEX-EQ', 'NSE:STYLAMIND-EQ', 'NSE:TEXINFRA-EQ', 'NSE:TEXMOPIPES-EQ', 'NSE:UNIPHOS-EQ', 'NSE:VARDHACRLC-EQ', 'NSE:VARDMNPOLY-EQ', 'NSE:WEIZMANIND-EQ', 'NSE:ZEEL-EQ', 'NSE:SUNTV-EQ', 'NSE:PVRINOX-EQ', 'NSE:NETWORK18-EQ', 'NSE:TV18BRDCST-EQ', 'NSE:JAGRAN-EQ', 'NSE:SAREGAMA-EQ', 'NSE:TIPSFILMS-EQ', 'NSE:TIPSMUSIC-EQ', 'NSE:RADIOCITY-EQ', 'NSE:DBCORP-EQ', 'NSE:HTMEDIA-EQ', 'NSE:NAVNETEDUL-EQ', 'NSE:NAZARA-EQ', 'NSE:ONMOBILE-EQ', 'NSE:UFO-EQ', 'NSE:EROS-EQ', 'NSE:BALAJITELE-EQ', 'NSE:CINELINE-EQ', 'NSE:CINEVISTA-EQ', 'NSE:CELEBRITY-EQ', 'NSE:SHEMAROO-EQ', 'NSE:YASHRAJ-EQ', 'NSE:PRITIKA-EQ', 'NSE:RELCAPITAL-EQ', 'NSE:RELMEDIA-EQ', 'NSE:NEXTMEDIA-EQ', 'NSE:BHARTIARTL-EQ', 'NSE:RJIO-EQ', 'NSE:IDEA-EQ', 'NSE:BSNL-EQ', 'NSE:MTNL-EQ', 'NSE:HFCL-EQ', 'NSE:STLTECH-EQ', 'NSE:GTPL-EQ', 'NSE:DEN-EQ', 'NSE:HATHWAY-EQ', 'NSE:SITI-EQ', 'NSE:ORTEL-EQ', 'NSE:TEJAS-EQ', 'NSE:RCOM-EQ', 'NSE:OPTIEMUS-EQ', 'NSE:ONEPOINT-EQ', 'NSE:CIGNITITEC-EQ', 'NSE:SMARTLINK-EQ', 'NSE:VINDHYATEL-EQ', 'NSE:TATACOMM-EQ', 'NSE:TANLA-EQ', 'NSE:ROUTE-EQ', 'NSE:ZENTEC-EQ', 'NSE:MOSCHIP-EQ', 'NSE:INDIGO-EQ', 'NSE:SPICEJET-EQ', 'NSE:JETAIRWAYS-EQ', 'NSE:TCI-EQ', 'NSE:VTL-EQ', 'NSE:ALLCARGO-EQ', 'NSE:BLUEDART-EQ', 'NSE:DELHIVERY-EQ', 'NSE:MAHLOG-EQ', 'NSE:SICAL-EQ', 'NSE:SNOWMAN-EQ', 'NSE:GATI-EQ', 'NSE:APOLLO-EQ', 'NSE:AEGISLOG-EQ', 'NSE:THOMASCOOK-EQ', 'NSE:COX&KINGS-EQ', 'NSE:KESARENT-EQ', 'NSE:YATRA-EQ', 'NSE:MAKEMYTRIP-EQ', 'NSE:EASEMYTRIP-EQ', 'NSE:IXIGO-EQ', 'NSE:ADANIPORTS-EQ', 'NSE:JSWINFRA-EQ', 'NSE:MHRIL-EQ', 'NSE:ESSELPACK-EQ', 'NSE:SAGCEM-EQ', 'NSE:INDIANHOTELS-EQ', 'NSE:LEMONTREE-EQ', 'NSE:CHALET-EQ', 'NSE:MAHINDRA-EQ', 'NSE:EIHOTEL-EQ', 'NSE:ITCHOTELS-EQ', 'NSE:ORIENTHOT-EQ', 'NSE:LEMON-EQ', 'NSE:TGBHOTELS-EQ', 'NSE:PARKHOTELS-EQ', 'NSE:KAMAT-EQ', 'NSE:ADVANI-EQ', 'NSE:SAMHI-EQ', 'NSE:BAJAJHLDNG-EQ', 'NSE:GODREJIND-EQ', 'NSE:SIEMENS-EQ', 'NSE:ABB-EQ', 'NSE:HONEYWELL-EQ', 'NSE:3M-EQ', 'NSE:TATA-EQ', 'NSE:BHARTI-EQ', 'NSE:ESSAR-EQ', 'NSE:JAIPRAKASH-EQ', 'NSE:GAMMON-EQ', 'NSE:PUNJ-EQ', 'NSE:LANCO-EQ', 'NSE:GMR-EQ', 'NSE:GVK-EQ', 'NSE:SIMPLEX-EQ', 'NSE:EMKAY-EQ']

MAX_SYMBOLS = len(STOCK_SYMBOLS)

# =============================================================================
# TELEGRAM HANDLER FOR DETECTOR AUTHENTICATION
# =============================================================================

class TelegramHandler:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        
    def send_message(self, message):
        """Send a message to Telegram"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, data=data, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending Telegram message: {e}")
            return False
    
    def get_updates(self):
        """Get latest messages from Telegram"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                "offset": self.last_update_id + 1,
                "timeout": 30
            }
            response = requests.get(url, params=params, timeout=35)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok") and data.get("result"):
                    updates = data["result"]
                    if updates:
                        self.last_update_id = updates[-1]["update_id"]
                        print(f"[TELEGRAM] Received {len(updates)} updates, last ID: {self.last_update_id}", flush=True)
                    return updates
                else:
                    print(f"[TELEGRAM] Response OK but no results: {data}", flush=True)
            else:
                print(f"[TELEGRAM] Bad response: {response.status_code}", flush=True)
            
            return []
        except Exception as e:
            print(f"[TELEGRAM] Error getting updates: {e}", flush=True)
            return []
    
    def extract_auth_code(self, message_text):
        """Extract auth_code from Fyers redirect URL using regex"""
        try:
            pattern1 = r'auth_code=([^&]+)&state=None'
            match1 = re.search(pattern1, message_text)
            if match1:
                auth_code = match1.group(1)
                print(f"Auth code extracted (with state=None): {auth_code[:20]}...")
                return auth_code
            
            pattern2 = r'auth_code=([^&\s]+)'
            match2 = re.search(pattern2, message_text)
            if match2:
                auth_code = match2.group(1)
                print(f"Auth code extracted (general pattern): {auth_code[:20]}...")
                return auth_code
            
            print("No auth_code found in message")
            return None
        except Exception as e:
            print(f"Error extracting auth code: {e}")
            return None
    
    def wait_for_auth_code(self, timeout_seconds=TELEGRAM_AUTH_TIMEOUT):
        """Wait for auth code message from Telegram"""
        print(f"Waiting for auth code from Telegram (timeout: {timeout_seconds}s)...")
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                updates = self.get_updates()
                
                for update in updates:
                    if "message" in update and "text" in update["message"]:
                        message_text = update["message"]["text"]
                        
                        if "fyersauth.vercel.app" in message_text and "auth_code=" in message_text:
                            auth_code = self.extract_auth_code(message_text)
                            if auth_code:
                                return auth_code
                
                time.sleep(TELEGRAM_POLLING_INTERVAL)
                
            except Exception as e:
                print(f"Telegram connection error: {e}")
                time.sleep(10)
        
        print("Timeout waiting for auth code from Telegram")
        return None
    
    def check_for_restart_command(self):
        """Check for restart command in Telegram messages"""
        try:
            print("Checking Telegram for restart command...", flush=True)
            updates = self.get_updates()
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"].strip()
                    print(f"Received message: '{message_text}'", flush=True)
                    
                    # Check various forms of restart command (case insensitive)
                    if message_text.lower() in ["restart", "restart!", "restart.", "restart bot", "restart system", "reboot"]:
                        print(f"Restart command received: '{message_text}'", flush=True)
                        self.send_message("Restart command received! Initiating restart...")
                        return True
            
            return False
        except Exception as e:
            print(f"Error checking for restart command: {e}", flush=True)
            return False

    def check_for_force_command(self):
        """Check for Force command in Telegram messages"""
        try:
            print("Checking Telegram for force command...", flush=True)
            updates = self.get_updates()
            print(f"Got {len(updates)} Telegram updates", flush=True)
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"].strip()
                    print(f"Received message: '{message_text}'", flush=True)
                    
                    # Check various forms of force command (case insensitive)
                    if message_text.lower() in ["force", "start", "start now"]:
                        print(f"Force command detected: '{message_text}'", flush=True)
                        self.send_message("Force command received! Starting detector immediately, bypassing market hours.")
                        return True
            
            print("No force command found in messages", flush=True)
            return False
        except Exception as e:
            print(f"Error checking for Force command: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False

# =============================================================================
# SUMMARY TELEGRAM HANDLER - SEPARATE BOT
# =============================================================================

class SummaryTelegramHandler:
    def __init__(self):
        self.bot_token = SUMMARY_TELEGRAM_BOT_TOKEN
        self.chat_id = SUMMARY_TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        self.stop_sending_today = False
        
    def send_message(self, message):
        """Send a message to Telegram"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                print("Summary message sent successfully")
                return True
            else:
                print(f"Failed to send summary message: {response.text}")
                return False
        except Exception as e:
            print(f"Error sending summary message: {e}")
            return False
    
    def get_updates(self):
        """Get latest messages from Telegram"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                "offset": self.last_update_id + 1,
                "timeout": 10
            }
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get("ok") and data.get("result"):
                    updates = data["result"]
                    if updates:
                        self.last_update_id = updates[-1]["update_id"]
                    return updates
            return []
        except Exception as e:
            print(f"Error getting Telegram updates: {e}")
            return []
    
    def check_for_done_message(self):
        """Check if 'done' message has been received"""
        try:
            print("Checking Telegram for done command...", flush=True)
            updates = self.get_updates()
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    text = update["message"]["text"].strip().lower()
                    print(f"Summary bot received: '{text}'", flush=True)
                    if text in ["done", "stop", "stop summary", "enough"]:
                        print("'Done' message received - stopping summaries for today", flush=True)
                        self.send_message("✅ Summaries stopped for today. Will resume tomorrow.")
                        self.stop_sending_today = True
                        return True
            return False
        except Exception as e:
            print(f"Error checking for done message: {e}", flush=True)
            return False
    
    def check_for_send_message(self):
        """Check if 'send' message has been received"""
        try:
            updates = self.get_updates()
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    text = update["message"]["text"].strip().lower()
                    if text in ["send", "send summary", "summary", "report"]:
                        print("'Send' message received - will send immediate summary", flush=True)
                        self.send_message("⏳ Generating summary...")
                        return True
            return False
        except Exception as e:
            print(f"Error checking for send message: {e}", flush=True)
            return False

# =============================================================================
# DAILY SUMMARY GENERATOR
# =============================================================================

class DailySummaryGenerator:
    def __init__(self):
        self.sheets_id = GOOGLE_SHEETS_ID
        self.credentials = GOOGLE_CREDENTIALS
        self.worksheet = None
        
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if not self.credentials:
                print("Google Sheets credentials not available")
                return False
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = Credentials.from_service_account_info(self.credentials, scopes=scopes)
            client = gspread.authorize(creds)
            
            spreadsheet = client.open_by_key(self.sheets_id)
            self.worksheet = spreadsheet.sheet1
            
            print("Google Sheets initialized for summary")
            return True
        except Exception as e:
            print(f"Error initializing sheets for summary: {e}")
            return False
    
    def get_today_data(self):
        """Get all data for today's date from Google Sheets"""
        try:
            if not self.worksheet:
                if not self.initialize_sheets():
                    return []
            
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            today_formats = [
                now.strftime("%Y-%m-%d"),
                now.strftime("%d-%m-%Y"),
                now.strftime("%d/%m/%Y"),
                now.strftime("%m/%d/%Y"),
            ]
            
            all_values = self.worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                print("No data in sheet")
                return []
            
            headers = all_values[0]
            print(f"Sheet headers: {headers}")
            
            date_col_idx = None
            symbol_col_idx = None
            value_col_idx = None
            
            for idx, header in enumerate(headers):
                header_lower = header.lower().strip()
                
                if header_lower == 'date':
                    date_col_idx = idx
                elif header_lower == 'symbol':
                    symbol_col_idx = idx
                elif 'trd' in header_lower and 'val' in header_lower and 'cr' in header_lower:
                    value_col_idx = idx
            
            if date_col_idx is None or symbol_col_idx is None or value_col_idx is None:
                print(f"Required columns not found!")
                return []
            
            today_records = []
            
            for row in all_values[1:]:
                if len(row) <= max(date_col_idx, symbol_col_idx, value_col_idx):
                    continue
                
                date_value = str(row[date_col_idx]).strip()
                
                is_today = False
                for date_format in today_formats:
                    if date_value == date_format or date_value.startswith(date_format):
                        is_today = True
                        break
                
                if is_today:
                    symbol = str(row[symbol_col_idx]).strip()
                    value_str = str(row[value_col_idx]).strip()
                    
                    record = {
                        'Date': date_value,
                        'Symbol': symbol,
                        'Trd_Val_Cr': value_str
                    }
                    today_records.append(record)
            
            print(f"Records for today: {len(today_records)}")
            return today_records
            
        except Exception as e:
            print(f"Error getting today's data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def generate_top_15_summary(self):
        """Generate top 15 stocks summary based on count and total value"""
        try:
            print("\nGENERATING TOP 15 SUMMARY")
            
            today_records = self.get_today_data()
            
            if not today_records:
                print("No records found for today")
                return None
            
            print(f"Processing {len(today_records)} records...")
            
            symbol_stats = {}
            
            for record in today_records:
                symbol = record.get('Symbol', '').strip()
                value_str = str(record.get('Trd_Val_Cr', '0')).strip()
                
                if not symbol or symbol == 'Unknown' or symbol == '':
                    continue
                
                try:
                    value_clean = re.sub(r'[^\d.\-]', '', value_str)
                    trd_val = float(value_clean) if value_clean and value_clean != '-' else 0.0
                except (ValueError, AttributeError):
                    trd_val = 0.0
                
                if symbol not in symbol_stats:
                    symbol_stats[symbol] = {
                        'count': 0,
                        'total_trd_val_cr': 0.0
                    }
                
                symbol_stats[symbol]['count'] += 1
                symbol_stats[symbol]['total_trd_val_cr'] += trd_val
            
            sorted_symbols = sorted(
                symbol_stats.items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )
            
            top_15 = sorted_symbols[:15]
            
            print(f"TOP 15 SYMBOLS BY COUNT")
            for i, (symbol, stats) in enumerate(top_15, 1):
                print(f"{i}. {symbol}: {stats['count']} trades, Rs{stats['total_trd_val_cr']:.2f} Cr")
            
            return top_15, len(today_records), len(symbol_stats)
            
        except Exception as e:
            print(f"Error generating summary: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def format_summary_message(self):
        """Format the summary message for Telegram"""
        try:
            result = self.generate_top_15_summary()
            
            if not result:
                return "No volume spike data available for today's summary"
            
            top_15, total_records, unique_symbols = result
            today_date = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%m-%Y")
            
            total_top15_value = sum(stats['total_trd_val_cr'] for _, stats in top_15)
            
            message = f"""<b>📊 Daily Volume Spike Summary</b>
📅 Date: {today_date}
📈 Total Records: {total_records}
🔢 Unique Symbols: {unique_symbols}
💰 Top 15 Total Value: ₹{total_top15_value:,.2f} Cr

<b>🏆 TOP 15 RANKINGS (by Count):</b>

"""
            
            for idx, (symbol, stats) in enumerate(top_15, 1):
                count = stats['count']
                total_trd_val_cr = stats['total_trd_val_cr']
                avg_per_trade = total_trd_val_cr / count if count > 0 else 0
                
                message += f"""{idx}. <b>{symbol}</b>
   📊 Count: <b>{count}</b> trades
   💵 Total Value: ₹{total_trd_val_cr:,.2f} Cr
   📉 Avg per Trade: ₹{avg_per_trade:.2f} Cr
   
"""
            
            message += f"""━━━━━━━━━━━━━━━━━━━━
📊 <i>Analysis Complete for {today_date}</i>
🎯 <i>Ranked by highest trade count</i>
📈 <i>Values from Trd_Val_Cr column</i>

Reply 'send' for fresh summary or 'done' to stop today
"""
            
            return message
            
        except Exception as e:
            print(f"Error formatting summary message: {e}")
            import traceback
            traceback.print_exc()
            return f"Error generating summary: {str(e)}"

# =============================================================================
# SUMMARY SCHEDULER
# =============================================================================

def summary_scheduler():
    """Background thread to handle summary sending"""
    print("Summary scheduler started")
    
    summary_handler = SummaryTelegramHandler()
    summary_generator = DailySummaryGenerator()
    
    last_sent_date = None
    last_sent_time = None
    
    while True:
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            current_date = now.strftime("%d-%m-%Y")
            current_time = now.strftime("%H:%M")
            
            # Reset on new day
            if last_sent_date != current_date:
                summary_handler.stop_sending_today = False
                last_sent_date = current_date
                last_sent_time = None
                print(f"New day started: {current_date}")
            
            # Check for done command
            summary_handler.check_for_done_message()
            
            # Check for send command
            if summary_handler.check_for_send_message():
                print("Manual send command received - sending summary now")
                summary_message = summary_generator.format_summary_message()
                summary_handler.send_message(summary_message)
                last_sent_time = current_time
            
            # Automatic sending logic
            if (current_time >= SUMMARY_SEND_TIME and 
                not summary_handler.stop_sending_today):
                
                should_send = False
                
                if last_sent_time is None:
                    should_send = True
                else:
                    last_dt = datetime.strptime(f"{current_date} {last_sent_time}", "%d-%m-%Y %H:%M")
                    current_dt = datetime.strptime(f"{current_date} {current_time}", "%d-%m-%Y %H:%M")
                    time_diff = (current_dt - last_dt).total_seconds()
                    
                    if time_diff >= SUMMARY_SEND_INTERVAL:
                        should_send = True
                
                if should_send:
                    print(f"Sending summary at {current_time}")
                    summary_message = summary_generator.format_summary_message()
                    
                    if summary_handler.send_message(summary_message):
                        last_sent_time = current_time
                        print(f"Summary sent successfully at {current_time}")
                    else:
                        print(f"Failed to send summary at {current_time}")
            
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in summary scheduler: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)

# =============================================================================
# FYERS AUTHENTICATOR CLASS
# =============================================================================

class FyersAuthenticator:
    def __init__(self):
        self.client_id = FYERS_CLIENT_ID
        self.secret_key = FYERS_SECRET_KEY
        self.redirect_uri = FYERS_REDIRECT_URI
        self.totp_secret = FYERS_TOTP_SECRET
        self.pin = FYERS_PIN
        self.access_token = None
        self.fyers_model = None
        self.telegram = TelegramHandler()
        self.last_relogin_time = 0
        self.relogin_interval = 300
        self.is_authenticated = False
        
    def generate_totp(self):
        totp = pyotp.TOTP(self.totp_secret)
        return totp.now()
    
    def load_saved_token(self):
        try:
            is_valid, message = validate_fyers_token_from_json()
            
            if is_valid:
                self.access_token = FYERS_ACCESS_TOKEN
                self.is_authenticated = True
                print("Using Fyers access token from JSON file")
                return True
            else:
                print(f"{message}")
                self.is_authenticated = False
                self.send_relogin_message(message)
                return False
            
        except Exception as e:
            print(f"Error loading saved token: {e}")
            self.is_authenticated = False
            self.send_relogin_message(f"Token loading error: {str(e)}")
            return False
    
    def send_relogin_message(self, reason):
        """Send Telegram message requesting re-authentication with retry logic"""
        current_time = time.time()
        
        if current_time - self.last_relogin_time < self.relogin_interval:
            remaining_time = int(self.relogin_interval - (current_time - self.last_relogin_time))
            print(f"Relogin message sent recently. Next message in {remaining_time} seconds")
            return False
        
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )
            
            auth_url = session.generate_authcode()
            totp_code = self.generate_totp()
            
            relogin_message = f"""
🔐 <b>Fyers Re-Authentication Required</b>

<b>Reason:</b> {reason}

<b>Authorization URL:</b>
<code>{auth_url}</code>

<b>TOTP Code:</b> <code>{totp_code}</code>

<b>Steps:</b>
1. Click the URL above
2. Login with your Fyers credentials
3. Use TOTP code if prompted for 2FA
4. After successful login, copy the entire redirect URL
5. Send the redirect URL back to this bot

<b>Timeout:</b> {TELEGRAM_AUTH_TIMEOUT} seconds
            """
            
            if self.telegram.send_message(relogin_message):
                self.last_relogin_time = current_time
                print("Re-authentication request sent to Telegram")
                return True
            else:
                print("Failed to send re-authentication request to Telegram")
                return False
                
        except Exception as e:
            print(f"Error sending re-authentication message: {e}")
            return False
    
    def save_token(self, token):
        global FYERS_ACCESS_TOKEN, FYERS_TOKEN_TIMESTAMP, FYERS_TOKEN_CREATED_AT
        
        FYERS_ACCESS_TOKEN = token
        FYERS_TOKEN_TIMESTAMP = time.time()
        FYERS_TOKEN_CREATED_AT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        save_fyers_token_to_json(token, FYERS_TOKEN_TIMESTAMP, FYERS_TOKEN_CREATED_AT)
        
        self.is_authenticated = True
        self.last_relogin_time = 0
        
        print("Token updated and saved to JSON file")
    
    def authenticate(self, max_retries=3):
        print("Starting Fyers authentication...", flush=True)
        
        print("Attempting to load saved token...", flush=True)
        if self.load_saved_token():
            print("Creating Fyers model with saved token...", flush=True)
            self.fyers_model = fyersModel.FyersModel(
                client_id=self.client_id,
                token=self.access_token,
                log_path=""
            )
            
            try:
                print("Testing connection with saved token...", flush=True)
                profile = self.fyers_model.get_profile()
                if profile['s'] == 'ok':
                    print("Token is valid and connection successful", flush=True)
                    self.is_authenticated = True
                    return True
                else:
                    print(f"Token validation failed: {profile}", flush=True)
                    self.is_authenticated = False
                    self.send_relogin_message("Token validation failed")
                    return False
            except Exception as e:
                print(f"Connection test failed: {e}", flush=True)
                self.is_authenticated = False
                self.send_relogin_message(f"Connection test failed: {str(e)}")
                return False
        
        print("No valid saved token, need fresh authentication", flush=True)
        
        for attempt in range(max_retries):
            print(f"Authentication attempt {attempt + 1}/{max_retries}...", flush=True)
            try:
                session = fyersModel.SessionModel(
                    client_id=self.client_id,
                    secret_key=self.secret_key,
                    redirect_uri=self.redirect_uri,
                    response_type="code",
                    grant_type="authorization_code"
                )
                
                response = session.generate_authcode()
                totp_code = self.generate_totp()
                
                telegram_message = f"""
🔐 <b>Fyers Authentication Required</b>

<b>Authorization URL:</b>
<code>{response}</code>

<b>TOTP Code:</b> <code>{totp_code}</code>

<b>Steps:</b>
1. Click the URL above
2. Login with credentials
3. Send redirect URL back

<b>Timeout:</b> {TELEGRAM_AUTH_TIMEOUT} seconds
                """
                
                print("Sending auth request to Telegram...", flush=True)
                self.telegram.send_message(telegram_message)
                
                print("Waiting for auth code from Telegram...", flush=True)
                auth_code = self.telegram.wait_for_auth_code()
                
                if not auth_code:
                    print("No auth code received, retrying...", flush=True)
                    continue
                
                print("Auth code received, generating token...", flush=True)
                session.set_token(auth_code)
                token_response = session.generate_token()
                
                if token_response and token_response.get('s') == 'ok':
                    print("Token generated successfully!", flush=True)
                    self.access_token = token_response['access_token']
                    self.save_token(self.access_token)
                    
                    self.fyers_model = fyersModel.FyersModel(
                        client_id=self.client_id,
                        token=self.access_token,
                        log_path=""
                    )
                    
                    self.telegram.send_message("✅ Authentication successful!")
                    self.is_authenticated = True
                    return True
                else:
                    print(f"Token generation failed: {token_response}", flush=True)
                    
            except Exception as e:
                print(f"Authentication error: {e}", flush=True)
                import traceback
                traceback.print_exc()
                continue
        
        print("All authentication attempts failed!", flush=True)
        return False
    
    def get_fyers_model(self):
        if not self.fyers_model:
            if not self.authenticate():
                raise Exception("Authentication failed")
        return self.fyers_model

# =============================================================================
# GOOGLE SHEETS MANAGER
# =============================================================================

class GoogleSheetsManager:
    def __init__(self, detector=None):
        self.gc = None
        self.worksheet = None
        self.lock = threading.Lock()
        self.detector = detector
        self.sheets_initialized = self.initialize_sheets()
        
        if not self.sheets_initialized:
            print("Google Sheets initialization failed")
    
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if GOOGLE_CREDENTIALS is None:
                print("Google credentials not available")
                return False
                
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scope)
            self.gc = gspread.authorize(creds)
            
            try:
                sheet = self.gc.open_by_key(GOOGLE_SHEETS_ID)
                self.worksheet = sheet.sheet1
                print(f"Connected to Google Sheet!")
                
                try:
                    headers = self.worksheet.row_values(1)
                    if not headers or len(headers) < 8:
                        headers = [
                            'Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike',
                            'Trd_Val_Cr', 'Spike_Type', 'Sector'
                        ]
                        self.worksheet.insert_row(headers, 1)
                        print("Added headers to sheet")
                except:
                    headers = [
                        'Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike',
                        'Trd_Val_Cr', 'Spike_Type', 'Sector'
                    ]
                    self.worksheet.append_row(headers)
                
            except gspread.SpreadsheetNotFound:
                print(f"Could not access Google Sheet")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error initializing Google Sheets: {e}")
            return False
    
    def add_trade_to_sheets(self, symbol, ltp, volume_spike, trade_value,
                           spike_type, previous_volume, current_volume,
                           previous_ltp=None, ltp_color_format=None):
        """Add a new trade record to Google Sheets"""
        try:
            if self.worksheet is None:
                return False
                
            with self.lock:
                current_time = datetime.now()
                sector = get_sector_for_symbol(symbol)
                
                row = [
                    current_time.strftime('%Y-%m-%d'),
                    current_time.strftime('%H:%M:%S'),
                    symbol,
                    round(ltp, 2),
                    int(volume_spike),
                    round(trade_value / 10000000, 2),
                    spike_type,
                    sector
                ]
                
                self.worksheet.append_row(row)
                print(f"Added to Google Sheets: {symbol} ({sector}) - Rs{trade_value/10000000:.2f} crore")
                
                return True
                
        except Exception as e:
            print(f"Error adding to Google Sheets: {e}")
            return False

# =============================================================================
# VOLUME SPIKE DETECTOR
# =============================================================================

class VolumeSpikeDetector:
    def __init__(self):
        self.authenticator = FyersAuthenticator()
        self.sheets_manager = GoogleSheetsManager(self)
        self.access_token = None
        self.fyers_ws = None
        self.total_ticks = 0
        self.individual_trades_detected = 0
        self.start_time = time.time()
        self.stop_event = None
        
        self.previous_volumes = {}
        self.last_alert_time = {}
        self.previous_ltp = {}
        self.sector_counts = {}
        
        self.websocket_retry_count = 0
        self.max_websocket_retries = 1
        
    def initialize(self):
        print("Initializing Volume Spike Detector...", flush=True)
        
        print("Attempting authentication...", flush=True)
        if not self.authenticator.authenticate():
            print("Authentication failed!", flush=True)
            return False
        
        print("Authentication successful!", flush=True)
        self.access_token = self.authenticator.access_token
    
        try:
            print("Getting Fyers model...", flush=True)
            fyers = self.authenticator.get_fyers_model()
            
            print("Getting profile...", flush=True)
            profile = fyers.get_profile()
            
            if profile['s'] == 'ok':
                print(f"Connected! User: {profile['data']['name']}", flush=True)
                return True
            else:
                print(f"Profile check failed: {profile}", flush=True)
                return False
        except Exception as e:
            print(f"Connection error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False
    
    def on_tick_received(self, *args):
        try:
            message = args[-1] if args else None
            
            if isinstance(message, dict):
                if message.get('type') in ['cn', 'ful', 'sub']:
                    return
                
                if 'symbol' in message:
                    self.detect_individual_trade(message)
                        
        except Exception as e:
            print(f"Error in tick handler: {e}")
    
    def detect_individual_trade(self, tick_data):
        """Detect individual large trades"""
        try:
            self.total_ticks += 1
            
            symbol = tick_data.get('symbol', '')
            ltp = float(tick_data.get('ltp', 0))
            current_volume = float(tick_data.get('vol_traded_today', 0))
            
            if not symbol or ltp <= 0 or current_volume <= 0:
                return
            
            previous_volume = self.previous_volumes.get(symbol, current_volume)
            previous_ltp = self.previous_ltp.get(symbol, None)
            
            volume_spike = current_volume - previous_volume
            
            self.previous_volumes[symbol] = current_volume
            self.previous_ltp[symbol] = ltp
            
            if volume_spike <= MIN_VOLUME_SPIKE:
                return
            
            individual_trade_value = ltp * volume_spike
            
            if individual_trade_value >= INDIVIDUAL_TRADE_THRESHOLD:
                last_alert = self.last_alert_time.get(symbol, 0)
                time_since_last = time.time() - last_alert
                
                if time_since_last > 60:
                    self.individual_trades_detected += 1
                    self.last_alert_time[symbol] = time.time()
                    
                    sector = get_sector_for_symbol(symbol)
                    self.sector_counts[sector] = self.sector_counts.get(sector, 0) + 1
                    
                    spike_percentage = (volume_spike / previous_volume * 100) if previous_volume > 0 else 0
                    if spike_percentage > 50:
                        spike_type = "Large Spike"
                    elif spike_percentage > 20:
                        spike_type = "Medium Spike"
                    else:
                        spike_type = "Volume Increase"

                    ltp_change = ltp - previous_ltp if previous_ltp else 0
                    
                    print(f"\nLARGE TRADE: {symbol} ({sector}) - Rs{individual_trade_value/10000000:.2f} Cr")
                    
                    self.sheets_manager.add_trade_to_sheets(
                        symbol=symbol,
                        ltp=ltp,
                        volume_spike=volume_spike,
                        trade_value=individual_trade_value,
                        spike_type=spike_type,
                        previous_volume=previous_volume,
                        current_volume=current_volume,
                        previous_ltp=previous_ltp
                    )
                    
                    telegram_alert = f"""
<b>🚨 LARGE TRADE DETECTED</b>

<b>Symbol:</b> {symbol}
<b>Sector:</b> {sector}
<b>LTP:</b> Rs{ltp:,.2f}
<b>Volume:</b> {volume_spike:,.0f}
<b>Value:</b> Rs{individual_trade_value/10000000:.2f} Cr
<b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    self.authenticator.telegram.send_message(telegram_alert)
                
        except Exception as e:
            print(f"Error detecting trade: {e}")
    
    def start_monitoring(self):
        """Start monitoring"""
        try:
            print("Creating WebSocket connection...")
            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{FYERS_CLIENT_ID}:{self.access_token}",
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_message=self.on_tick_received
            )
            
            symbols_to_monitor = STOCK_SYMBOLS[:MAX_SYMBOLS]
            print(f"Subscribing to {len(symbols_to_monitor)} symbols...")
            
            self.fyers_ws.connect()
            time.sleep(3)
            self.fyers_ws.subscribe(symbols=symbols_to_monitor, data_type="SymbolUpdate")
            
            print("Monitoring started")
            
            while True:
                if self.stop_event and self.stop_event.is_set():
                    break
                time.sleep(5)
                
        except Exception as e:
            print(f"Monitoring error: {e}")
            raise
        finally:
            if self.fyers_ws:
                try:
                    self.fyers_ws.close_connection()
                except:
                    pass

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import sys
    
    # Flush output immediately for better logging
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    try:
        print("="*70, flush=True)
        print("Fyers Volume Spike Detector with Summary", flush=True)
        print("="*70, flush=True)
        
        # Start summary scheduler in background
        summary_thread = threading.Thread(target=summary_scheduler, daemon=True)
        summary_thread.start()
        print("Summary scheduler started", flush=True)
        
        print("\nCommands available:", flush=True)
        print("   Detector Bot: 'force' or 'restart'", flush=True)
        print("   Summary Bot: 'send' or 'done'", flush=True)
        print("\nSupervisor will check for commands every 10 seconds", flush=True)
        print("="*70, flush=True)
        
        print("\nStarting supervisor loop...", flush=True)
        supervisor_loop()
            
    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
        _stop_stream_once()
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        _stop_stream_once()
        sys.exit(1)
