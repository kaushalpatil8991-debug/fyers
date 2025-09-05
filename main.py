#!/usr/bin/env python3
"""
Fyers Volume Spike Detector - Google Sheets Integration with Sector Classification
Detects large individual trades and updates Google Sheets in real-time with sector information
"""

import json
import os
import time
import threading
import requests
import re
from datetime import datetime
import pyotp
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import gspread
from google.oauth2.service_account import Credentials

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
INDIVIDUAL_TRADE_THRESHOLD = 30000000  # ₹3 crore for individual trades
MIN_VOLUME_SPIKE = 1000  # Minimum volume spike to consider

# Google Sheets Configuration
# GOOGLE_SHEETS_ID = "1l_6Sx_u1czhY-5JdT22tpmCV8Os3XuZmZ3U2ShKDLHw"  # Your Google Sheet ID
GOOGLE_SHEETS_ID = os.getenv('GOOGLE_SHEETS_ID')

# Google Service Account Credentials from Environment Variables
# GOOGLE_CREDENTIALS = {
#     "type": "service_account",
#     "project_id": "shoonya-alerts",
#     "private_key_id": "bd60e40609581073510770e1400c49a2028ea008",
#     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDibx3E9Z9LC+Ju\nbp1WxDF8us4qObPEJ+Dftvo5/M2cc61GfORSfLMMF/YP15zNk+aM01oWbT8uv6Xt\ndk+DFpOGAOe9rt/Vs3gT/8SCELfp/bdDJfWEhVqKVeA5Fp07YLA6riGDRyvJ7AYm\n0l0kclbM+mFCT3Grp+to+LDRz+1UsVsgRgqEsqE09BOnSQr72u628jp5HrKsisP1\nNEkd6hefbyWDpGfEMXEE/WWyqFVYLXpGHIeHqsnIpqYYlrPcGA0kncn1gsuNLKl2\nllkgwAwVOwBgPH3xKEkP0zIw9gSFUItNTvuJUeHuRbg09e6rOIoxJnfWVHMRijaD\ng+mXR7vRAgMBAAECggEAOeiR8/OfG+m7rGNkoLKHN29s11avdzx9oakhgF/7U4Yv\n68V3/PKANdkQ6EdLhjXLcfuBYBfrXzDLJhoqRoupCc3Ednm5K+V8kZzJLDxLVK+6\nxRT5n70dBSDmOaNjbbKSD0fGMVUryTWv8xC8mlwVf9GOuw8nMm+84Dktu1LSTuSb\nk99iuWQsV25cN6lHQJsOOyNrxGj4WHi9uhBRTDn9UYpUclEGfyrOpQ/edN9MOemx\nHHn2VQ1GsWJc4tVipQvPiB5hfa0lyzKXTkwsL4LUmvJGwVOx2n19XNDbdf3f2Ubg\nYUCzRIPaWQz3qIUyP3dTyIsC0chGeOBrhvZeP731swKBgQDxlRlezQl1SJ7/cmuZ\nTx5wpbw7LTw4ZP8igNgx5hwyg+wO4A7fsDpOS9JKWV2biLFbGDYmQB5xF1aCHSWg\neirRvJXgRDDfuNwIl1g5Y0O3Wa5axy3AtWa0kxzY0DrAIjA0ZZ5Qst1Iyhs7/eoW\n5yOsY/IX3KA7U4cyd5z2VbOczwKBgQDv8pSAgrr2VS18hxPSIcLTdhQBosw8G4ZA\nTqPP2XmqbpHhqqA39IrG7bP/9XUEno5KNB96Jypp1dMgVhqj87m0OY+i0gUAc53y\nBFQK3Y2ekBynnr923vIxGidF9ZJT3Xe6MRaPV1nZDoxlY2GcJDEqU4rpcdMwhd+y\nNczjuIeFXwKBgQCmYks76Ll35pFjXBnRWBWd/ffbRfdw29aAm/7KtzJ6dDOTtytn\nUoeFv2DRRiVbZtTH6a/5vjV5LFqveIWetiVhmKIc+Lo0i6w32oyv0bckw4Z7DS1s\nyUM4YQ4AIcIk7CcJH2ffKqGPbWs+cU3VglImfBuT5acR1SCLWKhpXHM9LwKBgFIk\n1bn5B1B1cJEFHT/+1tfVwJuexBR/x8IUKfhqF0DFgaOj6h291hSsI8conNrr/QJi\nPbRv3BFHZnPXhl9CfPy6B/ZRt+yjqBrGaI8fse/qniS0MA/d20P8FB3bKDEVzHst\neu+vk86/MEk1cKEnsr0uxIuOsCIYcrBrwqzi62I5AoGBAMwZ7Zv3+Cy3V4RwL0NV\nmOYrPMfKJYen3KDW0aestB7CcLbjkJ5JkTrHRbww0lTDt5fGqNNkzCRjumwEibx9\nHh9PtTjkL6Py/ejyvWyfQ+uCJ1ouUaD4eQhCl/L2gzTuhQXi5loloYEuAE/dSmb6\n4QkNDjDK+NIeg1IXKeM5yGHB\n-----END PRIVATE KEY-----\n",
#     "client_email": "fyers-alert@shoonya-alerts.iam.gserviceaccount.com",
#     "client_id": "106562060430559772108",
#     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#     "token_uri": "https://oauth2.googleapis.com/token",
#     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/fyers-alert%40shoonya-alerts.iam.gserviceaccount.com",
#     "universe_domain": "googleapis.com"
# }

# Load Google Credentials from Individual Environment Variables
GOOGLE_CREDENTIALS = {
    "type": "service_account",
    "project_id": os.getenv('GOOGLE_PROJECT_ID'),
    "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
    "private_key": os.getenv('GOOGLE_PRIVATE_KEY'),
    "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
    "client_id": os.getenv('GOOGLE_CLIENT_ID'),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
    "universe_domain": "googleapis.com"
}

# Check if all required Google credentials are present
required_google_fields = ['GOOGLE_PROJECT_ID', 'GOOGLE_PRIVATE_KEY_ID', 'GOOGLE_PRIVATE_KEY', 
                          'GOOGLE_CLIENT_EMAIL', 'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_X509_CERT_URL']
missing_google_fields = [field for field in required_google_fields if not os.getenv(field)]

if missing_google_fields:
    print(f"❌ Missing Google credentials environment variables: {', '.join(missing_google_fields)}")
    GOOGLE_CREDENTIALS = None

# Fyers Access Token from Environment Variables
# FYERS_ACCESS_TOKEN = ""
# FYERS_TOKEN_TIMESTAMP = 1757005253.571254
# FYERS_TOKEN_CREATED_AT = "2025-09-04 22:30:53"

FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')

# Telegram Configuration from Environment Variables
# TELEGRAM_BOT_TOKEN = "8360146544:AAEObU8_9LoGTZk66PVSwcayD5Hw5fnHTgY"  # Replace with your bot token
# TELEGRAM_CHAT_ID = "5715256800"      # Replace with your chat ID

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_POLLING_INTERVAL = 5  # Seconds to wait between checking for new messages
TELEGRAM_AUTH_TIMEOUT = 300    # 5 minutes timeout for auth code input

# Scheduling Configuration
MARKET_START_TIME = "09:13"    # Market start time (HH:MM format)
MARKET_END_TIME = "16:00"      # Market end time (HH:MM format)
SCHEDULING_ENABLED = True      # Set to False to disable scheduling and run continuously

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
    "NSE:MANORG-EQ": "Metals & Mining",
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
    "NSE:HINDCOPPER-EQ": "Metals & Mining",
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
    "NSE:LLOYDSME-EQ": "Metals & Mining",
    "NSE:MEP-EQ": "Metals & Mining",
    "NSE:METALFORGE-EQ": "Metals & Mining",
    "NSE:MITTAL-EQ": "Metals & Mining",
    "NSE:MUKANDLTD-EQ": "Metals & Mining",
    "NSE:NCML-EQ": "Metals & Mining",
    "NSE:ORISSAMINE-EQ": "Metals & Mining",
    "NSE:POKARNA-EQ": "Metals & Mining",
    "NSE:RAMCOIND-EQ": "Metals & Mining",
    "NSE:ROHLTD-EQ": "Metals & Mining",
    "NSE:SALSTEEL-EQ": "Metals & Mining",
    "NSE:SAMTEL-EQ": "Metals & Mining",
    "NSE:SILGO-EQ": "Metals & Mining",
    "NSE:STEELXIND-EQ": "Metals & Mining",
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
    "NSE:CAPLIN-EQ": "Pharmaceuticals",
    "NSE:SUNPHARMA-EQ": "Pharmaceuticals",
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
    "NSE:TRENT-EQ": "FMCG",
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
    "NSE:RAMCOCEM-EQ": "Cement",
    "NSE:INDIACEM-EQ": "Cement",
    "NSE:KAKATCEM-EQ": "Cement",
    "NSE:ORIENTCEM-EQ": "Cement",
    "NSE:KESORAMIND-EQ": "Cement",
    "NSE:NUVOCO-EQ": "Cement",
    "NSE:STARCEMENT-EQ": "Cement",
    "NSE:PRISMCEM-EQ": "Cement",
    "NSE:UDAICEMENT-EQ": "Cement",
    "NSE:MAGADH-EQ": "Cement",
    "NSE:SAURASHCEM-EQ": "Cement",
    "NSE:MANGLMCEM-EQ": "Cement",
    "NSE:CENTEXT-EQ": "Cement",
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
    "NSE:ARSS-EQ": "Construction",
    "NSE:NBCC-EQ": "Construction",
    "NSE:HUDCO-EQ": "Construction",
    "NSE:KALPATARU-EQ": "Construction",
    "NSE:KEC-EQ": "Construction",
    "NSE:GPIL-EQ": "Construction",
    "NSE:BRLM-EQ": "Construction",
    "NSE:IGARASHI-EQ": "Construction",
    "NSE:AIA-EQ": "Construction",
    "NSE:TITAGARH-EQ": "Construction",
    "NSE:TEXRAIL-EQ": "Construction",
    "NSE:MUKANDENG-EQ": "Construction",
    "NSE:BEL-EQ": "Construction",
    "NSE:HAL-EQ": "Construction",
    "NSE:BHEL-EQ": "Construction",
    "NSE:GRSE-EQ": "Construction",
    "NSE:COCHINSHIP-EQ": "Construction",
    "NSE:MAZAGON-EQ": "Construction",
    "NSE:BEML-EQ": "Construction",
    "NSE:CRISIL-EQ": "Construction",
    "NSE:THERMAX-EQ": "Construction",
    "NSE:CREDITACC-EQ": "Construction",
    "NSE:LXCHEM-EQ": "Construction",
    "NSE:HINDWAREAP-EQ": "Construction",
    "NSE:CERA-EQ": "Construction",
    "NSE:HSIL-EQ": "Construction",
    "NSE:SOMANY-EQ": "Construction",
    "NSE:KAJARIACER-EQ": "Construction",
    "NSE:ORIENTBELL-EQ": "Construction",
    "NSE:NITCO-EQ": "Construction",
    "NSE:MAGMA-EQ": "Construction",
    "NSE:ASTRAL-EQ": "Construction",
    "NSE:SUPREME-EQ": "Construction",
    "NSE:NILKAMAL-EQ": "Construction",
    "NSE:SINTEX-EQ": "Construction",
    "NSE:FINOLEX-EQ": "Construction",
    "NSE:KANSAINER-EQ": "Construction",
    "NSE:PRINCEPIPE-EQ": "Construction",
    "NSE:APOLLOPIPE-EQ": "Construction",
    
    # Agriculture & Fertilizers
    "NSE:UPL-EQ": "Agriculture",
    "NSE:RALLIS-EQ": "Agriculture",
    "NSE:GODREJAGRO-EQ": "Agriculture",
    "NSE:SUMICHEM-EQ": "Agriculture",
    "NSE:BASF-EQ": "Agriculture",
    "NSE:BAYER-EQ": "Agriculture",
    "NSE:INSECTICID-EQ": "Agriculture",
    "NSE:DHANUKA-EQ": "Agriculture",
    "NSE:SHARDACROP-EQ": "Agriculture",
    "NSE:HERANBA-EQ": "Agriculture",
    "NSE:BHARAT-EQ": "Agriculture",
    "NSE:GSFC-EQ": "Agriculture",
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
    "NSE:BAJAJHIND-EQ": "Agriculture",
    "NSE:UTTAM-EQ": "Agriculture",
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
    "NSE:RAJSHREE-EQ": "Agriculture",
    "NSE:MAGADSUGAR-EQ": "Agriculture",
    "NSE:AVADHSUGAR-EQ": "Agriculture",
    
    # Textiles
    "NSE:ARVIND-EQ": "Textiles",
    "NSE:WELSPUNIND-EQ": "Textiles",
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
    "NSE:RSWM-EQ": "Textiles",
    "NSE:RAYMOND-EQ": "Textiles",
    "NSE:VIPIND-EQ": "Textiles",
    "NSE:DONEAR-EQ": "Textiles",
    "NSE:MPHASIS-EQ": "Textiles",
    "NSE:ADITYA-EQ": "Textiles",
    "NSE:HIMATSEIDE-EQ": "Textiles",
    "NSE:SPENTEX-EQ": "Textiles",
    "NSE:CENTUM-EQ": "Textiles",
    "NSE:DOLLAR-EQ": "Textiles",
    "NSE:KITEX-EQ": "Textiles",
    "NSE:SHIVTEX-EQ": "Textiles",
    "NSE:BANSWARA-EQ": "Textiles",
    "NSE:BSL-EQ": "Textiles",
    "NSE:KOTHARI-EQ": "Textiles",
    "NSE:ALBK-EQ": "Textiles",
    "NSE:BIRLA-EQ": "Textiles",
    "NSE:DHANVARSHA-EQ": "Textiles",
    "NSE:GTN-EQ": "Textiles",
    "NSE:GPIL-EQ": "Textiles",
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
    "NSE:SHIVAMILLS-EQ": "Textiles",
    "NSE:SOMATEX-EQ": "Textiles",
    "NSE:STYLAMIND-EQ": "Textiles",
    "NSE:TEXINFRA-EQ": "Textiles",
    "NSE:TEXMOPIPES-EQ": "Textiles",
    "NSE:TEXRAIL-EQ": "Textiles",
    "NSE:UNIPHOS-EQ": "Textiles",
    "NSE:VARDHACRLC-EQ": "Textiles",
    "NSE:VARDMNPOLY-EQ": "Textiles",
    "NSE:WEIZMANIND-EQ": "Textiles",
    "NSE:WELSPUNLIV-EQ": "Textiles",
    
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
    "NSE:RAILTEL-EQ": "Telecommunications",
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
    "NSE:IRB-EQ": "Travel & Transport",
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
    "NSE:MAHINDRA-EQ": "Diversified",
    "NSE:TATA-EQ": "Diversified",
    "NSE:BHARTI-EQ": "Diversified",
    "NSE:ESSAR-EQ": "Diversified",
    "NSE:WELSPUN-EQ": "Diversified",
    "NSE:JAIPRAKASH-EQ": "Diversified",
    "NSE:GAMMON-EQ": "Diversified",
    "NSE:PUNJ-EQ": "Diversified",
    "NSE:NAGARJUNA-EQ": "Diversified",
    "NSE:LANCO-EQ": "Diversified",
    "NSE:GMR-EQ": "Diversified",
    "NSE:GVK-EQ": "Diversified",
    "NSE:SIMPLEX-EQ": "Diversified",
    "NSE:EMKAY-EQ": "Diversified",
    "NSE:GITANJALI-EQ": "Diversified",
    
    # Default fallback for unmapped symbols
}

def get_sector_for_symbol(symbol):
    """Get sector for a given symbol"""
    return SECTOR_MAPPING.get(symbol, "Others")

# =============================================================================
# SCHEDULING UTILITIES
# =============================================================================

def is_market_time():
    """Check if current time is within market hours"""
    if not SCHEDULING_ENABLED:
        return True  # Always return True if scheduling is disabled
    
    current_time = datetime.now()
    current_time_str = current_time.strftime("%H:%M")
    
    # Check if current time is between market start and end time
    return MARKET_START_TIME <= current_time_str <= MARKET_END_TIME

def get_time_until_market_start():
    """Get time until market starts (in seconds)"""
    current_time = datetime.now()
    start_hour, start_minute = map(int, MARKET_START_TIME.split(":"))
    market_start = current_time.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    
    # If market start time has passed today, return 0
    if current_time.time() >= market_start.time():
        return 0
    
    # Calculate time until market start
    time_diff = market_start - current_time
    return time_diff.total_seconds()

def get_time_until_market_end():
    """Get time until market ends (in seconds)"""
    current_time = datetime.now()
    end_hour, end_minute = map(int, MARKET_END_TIME.split(":"))
    market_end = current_time.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    
    # If market end time has passed today, return 0
    if current_time.time() >= market_end.time():
        return 0
    
    # Calculate time until market end
    time_diff = market_end - current_time
    return time_diff.total_seconds()

def wait_for_market_start():
    """Wait until market start time"""
    if not SCHEDULING_ENABLED:
        return
    
    while not is_market_time():
        time_until_start = get_time_until_market_start()
        if time_until_start > 0:
            hours = int(time_until_start // 3600)
            minutes = int((time_until_start % 3600) // 60)
            seconds = int(time_until_start % 60)
            
            print(f"⏰ Waiting for market to start at {MARKET_START_TIME}...")
            print(f"   Time remaining: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # Send status to Telegram every 30 minutes
            if int(time_until_start) % 1800 == 0:  # Every 30 minutes
                status_message = f"""
⏰ <b>Market Schedule Status</b>

🕐 <b>Current Time:</b> {datetime.now().strftime('%H:%M:%S')}
🚀 <b>Market Start:</b> {MARKET_START_TIME}
⏳ <b>Time Remaining:</b> {hours:02d}:{minutes:02d}:{seconds:02d}

📊 <b>Status:</b> Waiting for market to open
                """
                telegram_handler = TelegramHandler()
                telegram_handler.send_message(status_message)
            
            time.sleep(60)  # Check every minute
        else:
            break
    
    print(f"🚀 Market is now open! Starting monitoring at {datetime.now().strftime('%H:%M:%S')}")

def check_market_end():
    """Check if market has ended and stop monitoring"""
    if not SCHEDULING_ENABLED:
        return False  # Never stop if scheduling is disabled
    
    if not is_market_time():
        print(f"🔚 Market has ended at {MARKET_END_TIME}. Stopping monitoring...")
        
        # Send end of day message to Telegram
        end_message = f"""
🔚 <b>Market Session Ended</b>

🕐 <b>End Time:</b> {datetime.now().strftime('%H:%M:%S')}
📊 <b>Session Duration:</b> {MARKET_START_TIME} - {MARKET_END_TIME}

📈 <b>Monitoring Status:</b> Stopped
🔄 <b>Next Session:</b> Tomorrow at {MARKET_START_TIME}
        """
        telegram_handler = TelegramHandler()
        telegram_handler.send_message(end_message)
        
        return True  # Signal to stop monitoring
    
    return False  # Continue monitoring

# Top 100 Most Liquid Stocks (same as original)
STOCK_SYMBOLS = [
    "NSE:20MICRONS-EQ", "NSE:21STCENMGM-EQ", "NSE:360ONE-EQ", "NSE:3IINFOLTD-EQ",
    "NSE:3MINDIA-EQ", "NSE:3PLAND-EQ", "NSE:5PAISA-EQ", "NSE:63MOONS-EQ",
    "NSE:A2ZINFRA-EQ", "NSE:AAATECH-EQ", "NSE:AADHARHFC-EQ", "NSE:AAKASH-EQ",
    "NSE:AAREYDRUGS-EQ", "NSE:AARON-EQ", "NSE:AARTECH-EQ", "NSE:AARTIDRUGS-EQ",
    "NSE:AARTIIND-EQ", "NSE:AARTIPHARM-EQ", "NSE:AARTISURF-EQ", "NSE:AARVEEDEN-EQ",
    "NSE:AARVI-EQ", "NSE:AAVAS-EQ", "NSE:ABAN-EQ", "NSE:ABB-EQ",
    "NSE:ABBOTINDIA-EQ", "NSE:ABCAPITAL-EQ", "NSE:ABDL-EQ", "NSE:ABFRL-EQ",
    "NSE:ABINFRA-EQ", "NSE:ABLBL-EQ", "NSE:ABMINTLLTD-EQ", "NSE:ABREL-EQ",
    "NSE:ABSLAMC-EQ", "NSE:ACC-EQ", "NSE:ACCELYA-EQ", "NSE:ACCURACY-EQ",
    "NSE:ACE-EQ", "NSE:ACEINTEG-EQ", "NSE:ACI-EQ", "NSE:ACL-EQ",
    "NSE:ACLGATI-EQ", "NSE:ACMESOLAR-EQ", "NSE:ACUTAAS-EQ", "NSE:ADANIENSOL-EQ",
    "NSE:ADANIENT-EQ", "NSE:ADANIGREEN-EQ", "NSE:ADANIPORTS-EQ", "NSE:ADANIPOWER-EQ",
    "NSE:ADFFOODS-EQ", "NSE:ADL-EQ", "NSE:ADOR-EQ", "NSE:ADROITINFO-EQ",
    "NSE:ADSL-EQ", "NSE:ADVANIHOTR-EQ", "NSE:ADVENZYMES-EQ", "NSE:AEGISLOG-EQ",
    "NSE:AEGISVOPAK-EQ", "NSE:AEROENTER-EQ", "NSE:AEROFLEX-EQ", "NSE:AETHER-EQ",
    "NSE:AFCONS-EQ", "NSE:AFFLE-EQ", "NSE:AFFORDABLE-EQ", "NSE:AFIL-EQ",
    "NSE:AFSL-EQ", "NSE:AGARIND-EQ", "NSE:AGARWALEYE-EQ", "NSE:AGI-EQ",
    "NSE:AGIIL-EQ", "NSE:AGRITECH-EQ", "NSE:AGROPHOS-EQ", "NSE:AGSTRA-EQ",
    "NSE:AHLADA-EQ", "NSE:AHLEAST-EQ", "NSE:AHLUCONT-EQ", "NSE:AIAENG-EQ",
]
# Top Most Liquid and Valid NSE Stocks (Filtered for valid symbols)
STOCK_SYMBOLS = ['NSE:TCS-EQ', 'NSE:INFY-EQ', 'NSE:WIPRO-EQ', 'NSE:HCLTECH-EQ', 'NSE:TECHM-EQ', 'NSE:LTIM-EQ', 'NSE:LTTS-EQ', 'NSE:MINDTREE-EQ', 'NSE:COFORGE-EQ', 'NSE:PERSISTENT-EQ', 'NSE:CYIENT-EQ', 'NSE:MPHASIS-EQ', 'NSE:INTELLECT-EQ', 'NSE:TATAELXSI-EQ', 'NSE:KPITTECH-EQ', 'NSE:MASTEK-EQ', 'NSE:NEWGEN-EQ', 'NSE:CYIENTDLM-EQ', 'NSE:OFSS-EQ', 'NSE:ZENSAR-EQ', 'NSE:HDFCBANK-EQ', 'NSE:ICICIBANK-EQ', 'NSE:AXISBANK-EQ', 'NSE:SBIN-EQ', 'NSE:KOTAKBANK-EQ', 'NSE:INDUSINDBK-EQ', 'NSE:BANDHANBNK-EQ', 'NSE:IDFCFIRSTB-EQ', 'NSE:FEDERALBNK-EQ', 'NSE:RBLBANK-EQ', 'NSE:YESBANK-EQ', 'NSE:AUBANK-EQ', 'NSE:BANKBARODA-EQ', 'NSE:PNB-EQ', 'NSE:CANBK-EQ', 'NSE:UNIONBANK-EQ', 'NSE:BANKINDIA-EQ', 'NSE:CENTRALBK-EQ', 'NSE:IOB-EQ', 'NSE:PSB-EQ', 'NSE:IDBI-EQ', 'NSE:UCOBANK-EQ', 'NSE:INDIANB-EQ', 'NSE:CSBBANK-EQ', 'NSE:DCBBANK-EQ', 'NSE:SOUTHBANK-EQ', 'NSE:TMB-EQ', 'NSE:KTKBANK-EQ', 'NSE:J&KBANK-EQ', 'NSE:DHANBANK-EQ', 'NSE:MAHABANK-EQ', 'NSE:KARURVYSYA-EQ', 'NSE:CUB-EQ', 'NSE:UTKARSHBNK-EQ', 'NSE:ESAFSFB-EQ', 'NSE:UJJIVANSFB-EQ', 'NSE:EQUITASBNK-EQ', 'NSE:CAPITALSFB-EQ', 'NSE:SURYODAY-EQ', 'NSE:FINPIPE-EQ', 'NSE:BAJFINANCE-EQ', 'NSE:BAJAJFINSV-EQ', 'NSE:HDFCLIFE-EQ', 'NSE:SBILIFE-EQ', 'NSE:ICICIGI-EQ', 'NSE:ICICIPRULI-EQ', 'NSE:LICI-EQ', 'NSE:NIACL-EQ', 'NSE:GODIGIT-EQ', 'NSE:STARHEALTH-EQ', 'NSE:NIVABUPA-EQ', 'NSE:HDFCAMC-EQ', 'NSE:UTIAMC-EQ', 'NSE:CRISIL-EQ', 'NSE:CREDITACC-EQ', 'NSE:BFSL-EQ', 'NSE:CHOLAFIN-EQ', 'NSE:MUTHOOTFIN-EQ', 'NSE:MANAPPURAM-EQ', 'NSE:PFC-EQ', 'NSE:RECLTD-EQ', 'NSE:IRFC-EQ', 'NSE:EDELWEISS-EQ', 'NSE:IIFL-EQ', 'NSE:M&MFIN-EQ', 'NSE:SHRIRAMFIN-EQ', 'NSE:BAJAJHFL-EQ', 'NSE:CANFINHOME-EQ', 'NSE:LICHSGFIN-EQ', 'NSE:PNBHOUSING-EQ', 'NSE:REPCO-EQ', 'NSE:HOMEFIRST-EQ', 'NSE:INDOSTAR-EQ', 'NSE:SPANDANA-EQ', 'NSE:PAISALO-EQ', 'NSE:JSFB-EQ', 'NSE:SBFC-EQ', 'NSE:ASIANFIN-EQ', 'NSE:RELIGARE-EQ', 'NSE:MOTILALOFS-EQ', 'NSE:ANGELONE-EQ', 'NSE:ANANDRATHI-EQ', 'NSE:ARIHANTCAP-EQ', 'NSE:GEOJITFSL-EQ', 'NSE:NUVAMA-EQ', 'NSE:KFINTECH-EQ', 'NSE:CDSL-EQ', 'NSE:BSE-EQ', 'NSE:MCX-EQ', 'NSE:IEX-EQ', 'NSE:CAMS-EQ', 'NSE:JIOFIN-EQ', 'NSE:RELIANCE-EQ', 'NSE:ONGC-EQ', 'NSE:IOC-EQ', 'NSE:BPCL-EQ', 'NSE:HINDPETRO-EQ', 'NSE:GAIL-EQ', 'NSE:OIL-EQ', 'NSE:MGL-EQ', 'NSE:IGL-EQ', 'NSE:GUJGASLTD-EQ', 'NSE:ATGL-EQ', 'NSE:CASTROLIND-EQ', 'NSE:GULF-EQ', 'NSE:GULFOILLUB-EQ', 'NSE:GULFPETRO-EQ', 'NSE:HINDOILEXP-EQ', 'NSE:SELAN-EQ', 'NSE:MRPL-EQ', 'NSE:TNPETRO-EQ', 'NSE:CHENNPETRO-EQ', 'NSE:HINDNATGLS-EQ', 'NSE:GSPL-EQ', 'NSE:ADANIGAS-EQ', 'NSE:GSFC-EQ', 'NSE:NTPC-EQ', 'NSE:POWERGRID-EQ', 'NSE:COALINDIA-EQ', 'NSE:TATAPOWER-EQ', 'NSE:ADANIPOWER-EQ', 'NSE:ADANIGREEN-EQ', 'NSE:JSW-ENERGY-EQ', 'NSE:NHPC-EQ', 'NSE:SJVN-EQ', 'NSE:IREDA-EQ', 'NSE:NTPCGREEN-EQ', 'NSE:ADANIENSOL-EQ', 'NSE:SUZLON-EQ', 'NSE:INOXWIND-EQ', 'NSE:ORIENTGEN-EQ', 'NSE:JPPOWER-EQ', 'NSE:JPINFRATEC-EQ', 'NSE:RPOWER-EQ', 'NSE:TORNTPOWER-EQ', 'NSE:CESC-EQ', 'NSE:TRENT-EQ', 'NSE:THERMAX-EQ', 'NSE:KEC-EQ', 'NSE:RTNPOWER-EQ', 'NSE:JSWENERGY-EQ', 'NSE:INOXGREEN-EQ', 'NSE:WAAREEENER-EQ', 'NSE:SWSOLAR-EQ', 'NSE:SOLARINDS-EQ', 'NSE:INOXWI-RE-EQ', 'NSE:WEBSOL-EQ', 'NSE:WEBELSOLAR-EQ', 'NSE:GREENPOWER-EQ', 'NSE:BOROSIL-EQ', 'NSE:MARUTI-EQ', 'NSE:TATAMOTORS-EQ', 'NSE:M&M-EQ', 'NSE:EICHERMOT-EQ', 'NSE:BAJAJ-AUTO-EQ', 'NSE:HEROMOTOCO-EQ', 'NSE:TVSMOTOR-EQ', 'NSE:ASHOKLEY-EQ', 'NSE:ESCORTS-EQ', 'NSE:BALKRISIND-EQ', 'NSE:MRF-EQ', 'NSE:APOLLOTYRE-EQ', 'NSE:CEAT-EQ', 'NSE:JK-TYRE-EQ', 'NSE:MOTHERSON-EQ', 'NSE:BOSCHLTD-EQ', 'NSE:EXIDEIND-EQ', 'NSE:AMARON-EQ', 'NSE:SUNDARAM-EQ', 'NSE:TIINDIA-EQ', 'NSE:LUMAX-EQ', 'NSE:MINDA-EQ', 'NSE:ENDURANCE-EQ', 'NSE:SUPRAJIT-EQ', 'NSE:SUBROS-EQ', 'NSE:TEAMLEASE-EQ', 'NSE:FORCEMOT-EQ', 'NSE:SJS-EQ', 'NSE:SANSERA-EQ', 'NSE:SANDHAR-EQ', 'NSE:SCHAEFFLER-EQ', 'NSE:TALBROS-EQ', 'NSE:RALLIS-EQ', 'NSE:AAUTOIND-EQ', 'NSE:JAMNAAUTO-EQ', 'NSE:WHEELS-EQ', 'NSE:AUTOAXLES-EQ', 'NSE:PPAP-EQ', 'NSE:FIEM-EQ', 'NSE:GABRIEL-EQ', 'NSE:JTEKT-EQ', 'NSE:VARROC-EQ', 'NSE:MSUMI-EQ', 'NSE:UNOMINDA-EQ', 'NSE:UNIPARTS-EQ', 'NSE:RICOAUTO-EQ', 'NSE:RAMKRISHNA-EQ', 'NSE:ANANDRISHIJI-EQ', 'NSE:BAJAJHLD-EQ', 'NSE:VINATIORGA-EQ', 'NSE:BAJAJCON-EQ', 'NSE:HINDMOTORS-EQ', 'NSE:OMAXAUTO-EQ', 'NSE:BHEL-EQ', 'NSE:HINDCOPPER-EQ', 'NSE:ATULAUTO-EQ', 'NSE:SHIVAMILLS-EQ', 'NSE:CUMMINSIND-EQ', 'NSE:HONDAPOWER-EQ', 'NSE:KIRLOSKP-EQ', 'NSE:SETCO-EQ', 'NSE:MAGMA-EQ', 'NSE:OLECTRA-EQ', 'NSE:OLAELEC-EQ', 'NSE:HYUNDAI-EQ', 'NSE:MAHINDCIE-EQ', 'NSE:TATASTEEL-EQ', 'NSE:HINDALCO-EQ', 'NSE:JSWSTEEL-EQ', 'NSE:SAIL-EQ', 'NSE:VEDL-EQ', 'NSE:HINDZINC-EQ', 'NSE:JINDALSTEL-EQ', 'NSE:NMDC-EQ', 'NSE:MOIL-EQ', 'NSE:NATIONALUM-EQ', 'NSE:BALRAMCHIN-EQ', 'NSE:APL-EQ', 'NSE:RATNAMANI-EQ', 'NSE:WELSPUNIND-EQ', 'NSE:JINDALPOLY-EQ', 'NSE:ORIENTCEM-EQ', 'NSE:STEELXIND-EQ', 'NSE:LLOYDSME-EQ', 'NSE:VISAKAIND-EQ', 'NSE:ARSS-EQ', 'NSE:KALYANI-EQ', 'NSE:KALYANIFRG-EQ', 'NSE:GRAPHITE-EQ', 'NSE:UGARSUGAR-EQ', 'NSE:RSWM-EQ', 'NSE:RAIN-EQ', 'NSE:GRAVITA-EQ', 'NSE:GVKPIL-EQ', 'NSE:MANORG-EQ', 'NSE:JKLAKSHMI-EQ', 'NSE:SREESTEEL-EQ', 'NSE:SUNFLAG-EQ', 'NSE:FACOR-EQ', 'NSE:BHUSHAN-EQ', 'NSE:ROHLTD-EQ', 'NSE:ZENITHSTL-EQ', 'NSE:VISHNU-EQ', 'NSE:UTTAMSTL-EQ', 'NSE:INDIACEM-EQ', 'NSE:RAMCOCEM-EQ', 'NSE:DALMIA-EQ', 'NSE:CENTURYPLY-EQ', 'NSE:CENTEXT-EQ', 'NSE:MAGNESITA-EQ', 'NSE:ORIENTREFR-EQ', 'NSE:MADRASFERT-EQ', 'NSE:MANDHANA-EQ', 'NSE:RAMASTEEL-EQ', 'NSE:PALLADINESTEEL-EQ', 'NSE:PALREDTEC-EQ', 'NSE:SALSTEEL-EQ', 'NSE:VSTL-EQ', 'NSE:STEELCAS-EQ', 'NSE:STEELCITY-EQ', 'NSE:STEL-EQ', 'NSE:SUNSTEEL-EQ', 'NSE:MAHASTEEL-EQ', 'NSE:HISARMETAL-EQ', 'NSE:ISGEC-EQ', 'NSE:KDDL-EQ', 'NSE:KIOCL-EQ', 'NSE:MEP-EQ', 'NSE:METALFORGE-EQ', 'NSE:MITTAL-EQ', 'NSE:MUKANDLTD-EQ', 'NSE:NCML-EQ', 'NSE:ORISSAMINE-EQ', 'NSE:POKARNA-EQ', 'NSE:RAMCOIND-EQ', 'NSE:SAMTEL-EQ', 'NSE:SILGO-EQ', 'NSE:UTTAM-EQ', 'NSE:WALCHANNAG-EQ', 'NSE:WELSPUN-EQ', 'NSE:ADANIENT-EQ', 'NSE:BEML-EQ', 'NSE:SUNPHARMA-EQ', 'NSE:DRREDDY-EQ', 'NSE:CIPLA-EQ', 'NSE:DIVISLAB-EQ', 'NSE:LUPIN-EQ', 'NSE:BIOCON-EQ', 'NSE:AUROPHARMA-EQ', 'NSE:TORNTPHARM-EQ', 'NSE:GLENMARK-EQ', 'NSE:CADILAHC-EQ', 'NSE:ALKEM-EQ', 'NSE:LALPATHLAB-EQ', 'NSE:METROPOLIS-EQ', 'NSE:FORTIS-EQ', 'NSE:APOLLOHOSP-EQ', 'NSE:HCG-EQ', 'NSE:MAXHEALTH-EQ', 'NSE:NARAYANHRU-EQ', 'NSE:RAINBOWHSPL-EQ', 'NSE:KRSNAA-EQ', 'NSE:MEDANTA-EQ', 'NSE:KIMS-EQ', 'NSE:SHALBY-EQ', 'NSE:THYROCARE-EQ', 'NSE:SEQUENT-EQ', 'NSE:GRANULES-EQ', 'NSE:LAURUSLABS-EQ', 'NSE:JUBLPHARMA-EQ', 'NSE:CAPLIN-EQ', 'NSE:AJANTPHARM-EQ', 'NSE:ERIS-EQ', 'NSE:SUVEN-EQ', 'NSE:NATCOPHARM-EQ', 'NSE:STRIDES-EQ', 'NSE:GUFICBIO-EQ', 'NSE:MARKSANS-EQ', 'NSE:SOLARA-EQ', 'NSE:ORCHPHARMA-EQ', 'NSE:IPCA-EQ', 'NSE:IPCALAB-EQ', 'NSE:SYNGENE-EQ', 'NSE:BLISSGVS-EQ', 'NSE:NEULANDLAB-EQ', 'NSE:MANKIND-EQ', 'NSE:EMCURE-EQ', 'NSE:PFIZER-EQ', 'NSE:GLAXO-EQ', 'NSE:ABBOTINDIA-EQ', 'NSE:SANOFI-EQ', 'NSE:NOVARTIS-EQ', 'NSE:MSD-EQ', 'NSE:BAYER-EQ', 'NSE:WOCKPHARMA-EQ', 'NSE:INDOCO-EQ', 'NSE:FDC-EQ', 'NSE:CENTRALDRUG-EQ', 'NSE:JAGSONPAL-EQ', 'NSE:ARISTO-EQ', 'NSE:ALEMBICLTD-EQ', 'NSE:UNICHEMLAB-EQ', 'NSE:MOREPEN-EQ', 'NSE:UNICHEM-EQ', 'NSE:ADVENZYMES-EQ', 'NSE:TATACHEM-EQ', 'NSE:DEEPAKNTR-EQ', 'NSE:PIDILITIND-EQ', 'NSE:AKZOINDIA-EQ', 'NSE:HINDUNILVR-EQ', 'NSE:ITC-EQ', 'NSE:BRITANNIA-EQ', 'NSE:NESTLEIND-EQ', 'NSE:DABUR-EQ', 'NSE:GODREJCP-EQ', 'NSE:MARICO-EQ', 'NSE:COLPAL-EQ', 'NSE:EMAMILTD-EQ', 'NSE:JYOTHYLAB-EQ', 'NSE:GILLETTE-EQ', 'NSE:PGHH-EQ', 'NSE:TATACONSUM-EQ', 'NSE:UBL-EQ', 'NSE:PATANJALI-EQ', 'NSE:RADICO-EQ', 'NSE:MCDOWELL-EQ', 'NSE:VSTIND-EQ', 'NSE:KPRMILL-EQ', 'NSE:WELSPUNLIV-EQ', 'NSE:VMART-EQ', 'NSE:SHOPERSTOP-EQ', 'NSE:ADITYA-EQ', 'NSE:VENKEYS-EQ', 'NSE:HATSUN-EQ', 'NSE:SULA-EQ', 'NSE:TASTYBITE-EQ', 'NSE:BIKAJI-EQ', 'NSE:JUBLFOOD-EQ', 'NSE:HERITGFOOD-EQ', 'NSE:GOCOLORS-EQ', 'NSE:NYKAA-EQ', 'NSE:HONASA-EQ', 'NSE:MANYAVAR-EQ', 'NSE:AHLUWALIA-EQ', 'NSE:RELAXO-EQ', 'NSE:BATA-EQ', 'NSE:LIBERTSHOE-EQ', 'NSE:KHADIM-EQ', 'NSE:MIRZA-EQ', 'NSE:VIP-EQ', 'NSE:SKUMAR-EQ', 'NSE:SYMPHONY-EQ', 'NSE:VOLTAS-EQ', 'NSE:BLUESTARCO-EQ', 'NSE:HAVELLS-EQ', 'NSE:CROMPTON-EQ', 'NSE:ORIENT-EQ', 'NSE:WHIRLPOOL-EQ', 'NSE:AMBER-EQ', 'NSE:BAJAJHCARE-EQ', 'NSE:VGUARD-EQ', 'NSE:POLYCAB-EQ', 'NSE:FINOLEX-EQ', 'NSE:KEI-EQ', 'NSE:DIXON-EQ', 'NSE:TITAN-EQ', 'NSE:KALYAN-EQ', 'NSE:THANGAMAY-EQ', 'NSE:SENCO-EQ', 'NSE:TBZ-EQ', 'NSE:PCJEWELLER-EQ', 'NSE:GITANJALI-EQ', 'NSE:ULTRACEMCO-EQ', 'NSE:AMBUJACEM-EQ', 'NSE:ACC-EQ', 'NSE:SHREECEM-EQ', 'NSE:JKCEMENT-EQ', 'NSE:HEIDELBERG-EQ', 'NSE:KAKATCEM-EQ', 'NSE:KESORAMIND-EQ', 'NSE:NUVOCO-EQ', 'NSE:STARCEMENT-EQ', 'NSE:PRISMCEM-EQ', 'NSE:UDAICEMENT-EQ', 'NSE:MAGADH-EQ', 'NSE:SAURASHCEM-EQ', 'NSE:MANGLMCEM-EQ', 'NSE:DECCAN-EQ', 'NSE:LT-EQ', 'NSE:DLF-EQ', 'NSE:GODREJPROP-EQ', 'NSE:OBEROIRLTY-EQ', 'NSE:BRIGADE-EQ', 'NSE:PHOENIXMILLS-EQ', 'NSE:PRESTIGE-EQ', 'NSE:SOBHA-EQ', 'NSE:SUNTECK-EQ', 'NSE:KOLTEPATIL-EQ', 'NSE:MAHLIFE-EQ', 'NSE:LODHA-EQ', 'NSE:SIGNATURE-EQ', 'NSE:RUSTOMJEE-EQ', 'NSE:MIDHANI-EQ', 'NSE:IRCON-EQ', 'NSE:RITES-EQ', 'NSE:RVNL-EQ', 'NSE:RAILTEL-EQ', 'NSE:CONCOR-EQ', 'NSE:NCC-EQ', 'NSE:HCC-EQ', 'NSE:IRB-EQ', 'NSE:SADBHAV-EQ', 'NSE:ASHOKA-EQ', 'NSE:KNR-EQ', 'NSE:PNC-EQ', 'NSE:PATEL-EQ', 'NSE:NBCC-EQ', 'NSE:HUDCO-EQ', 'NSE:KALPATARU-EQ', 'NSE:GPIL-EQ', 'NSE:BRLM-EQ', 'NSE:IGARASHI-EQ', 'NSE:AIA-EQ', 'NSE:TITAGARH-EQ', 'NSE:TEXRAIL-EQ', 'NSE:MUKANDENG-EQ', 'NSE:BEL-EQ', 'NSE:HAL-EQ', 'NSE:GRSE-EQ', 'NSE:COCHINSHIP-EQ', 'NSE:MAZAGON-EQ', 'NSE:LXCHEM-EQ', 'NSE:HINDWAREAP-EQ', 'NSE:CERA-EQ', 'NSE:HSIL-EQ', 'NSE:SOMANY-EQ', 'NSE:KAJARIACER-EQ', 'NSE:ORIENTBELL-EQ', 'NSE:NITCO-EQ', 'NSE:ASTRAL-EQ', 'NSE:SUPREME-EQ', 'NSE:NILKAMAL-EQ', 'NSE:SINTEX-EQ', 'NSE:KANSAINER-EQ', 'NSE:PRINCEPIPE-EQ', 'NSE:APOLLOPIPE-EQ', 'NSE:UPL-EQ', 'NSE:GODREJAGRO-EQ', 'NSE:SUMICHEM-EQ', 'NSE:BASF-EQ', 'NSE:INSECTICID-EQ', 'NSE:DHANUKA-EQ', 'NSE:SHARDACROP-EQ', 'NSE:HERANBA-EQ', 'NSE:BHARAT-EQ', 'NSE:FACT-EQ', 'NSE:RCF-EQ', 'NSE:NFL-EQ', 'NSE:CHAMBLFERT-EQ', 'NSE:KRIBHCO-EQ', 'NSE:ZUARIAGRO-EQ', 'NSE:DEEPAKFERT-EQ', 'NSE:MADRAS-EQ', 'NSE:SOUTHERN-EQ', 'NSE:MANGALORE-EQ', 'NSE:NAGARJUNA-EQ', 'NSE:PARADEEP-EQ', 'NSE:COROMANDEL-EQ', 'NSE:IFCO-EQ', 'NSE:KHAITAN-EQ', 'NSE:KRBL-EQ', 'NSE:USHAMART-EQ', 'NSE:LAXMIORG-EQ', 'NSE:PREMIER-EQ', 'NSE:AVANTIFEED-EQ', 'NSE:GODHA-EQ', 'NSE:RUCHISOYA-EQ', 'NSE:ADANIWILMAR-EQ', 'NSE:BAJAJHIND-EQ', 'NSE:JUBLAGRI-EQ', 'NSE:PARAS-EQ', 'NSE:JKAGRI-EQ', 'NSE:NAVRATNA-EQ', 'NSE:NATIONAL-EQ', 'NSE:RAJSHREE-EQ', 'NSE:DWARIKESH-EQ', 'NSE:TRIVENI-EQ', 'NSE:BALRAMPUR-EQ', 'NSE:KOTHARI-EQ', 'NSE:MAWANA-EQ', 'NSE:DHAMPURSUG-EQ', 'NSE:RENUKA-EQ', 'NSE:KSL-EQ', 'NSE:TIRUPATI-EQ', 'NSE:SAKAR-EQ', 'NSE:VISHWARAJ-EQ', 'NSE:SAKTISUG-EQ', 'NSE:ANDHRSUGAR-EQ', 'NSE:BANNARI-EQ', 'NSE:MAGADSUGAR-EQ', 'NSE:AVADHSUGAR-EQ', 'NSE:ARVIND-EQ', 'NSE:TRIDENT-EQ', 'NSE:VARDHMAN-EQ', 'NSE:SUTLEJ-EQ', 'NSE:GRASIM-EQ', 'NSE:SPENTEX-EQ', 'NSE:INDORAMA-EQ', 'NSE:FILATEX-EQ', 'NSE:ALOKTEXT-EQ', 'NSE:BTIL-EQ', 'NSE:MAFATLAL-EQ', 'NSE:RAYMOND-EQ', 'NSE:VIPIND-EQ', 'NSE:DONEAR-EQ', 'NSE:HIMATSEIDE-EQ', 'NSE:CENTUM-EQ', 'NSE:DOLLAR-EQ', 'NSE:KITEX-EQ', 'NSE:SHIVTEX-EQ', 'NSE:BANSWARA-EQ', 'NSE:BSL-EQ', 'NSE:ALBK-EQ', 'NSE:BIRLA-EQ', 'NSE:DHANVARSHA-EQ', 'NSE:GTN-EQ', 'NSE:GOKUL-EQ', 'NSE:HIRA-EQ', 'NSE:KGDENIM-EQ', 'NSE:LOYAL-EQ', 'NSE:MONACO-EQ', 'NSE:MSP-EQ', 'NSE:NAHAR-EQ', 'NSE:NITIN-EQ', 'NSE:PRADEEP-EQ', 'NSE:SARLA-EQ', 'NSE:SHANTIGEAR-EQ', 'NSE:SOMATEX-EQ', 'NSE:STYLAMIND-EQ', 'NSE:TEXINFRA-EQ', 'NSE:TEXMOPIPES-EQ', 'NSE:UNIPHOS-EQ', 'NSE:VARDHACRLC-EQ', 'NSE:VARDMNPOLY-EQ', 'NSE:WEIZMANIND-EQ', 'NSE:ZEEL-EQ', 'NSE:SUNTV-EQ', 'NSE:PVRINOX-EQ', 'NSE:NETWORK18-EQ', 'NSE:TV18BRDCST-EQ', 'NSE:JAGRAN-EQ', 'NSE:SAREGAMA-EQ', 'NSE:TIPSFILMS-EQ', 'NSE:TIPSMUSIC-EQ', 'NSE:RADIOCITY-EQ', 'NSE:DBCORP-EQ', 'NSE:HTMEDIA-EQ', 'NSE:NAVNETEDUL-EQ', 'NSE:NAZARA-EQ', 'NSE:ONMOBILE-EQ', 'NSE:UFO-EQ', 'NSE:EROS-EQ', 'NSE:BALAJITELE-EQ', 'NSE:CINELINE-EQ', 'NSE:CINEVISTA-EQ', 'NSE:CELEBRITY-EQ', 'NSE:SHEMAROO-EQ', 'NSE:YASHRAJ-EQ', 'NSE:PRITIKA-EQ', 'NSE:RELCAPITAL-EQ', 'NSE:RELMEDIA-EQ', 'NSE:NEXTMEDIA-EQ', 'NSE:BHARTIARTL-EQ', 'NSE:RJIO-EQ', 'NSE:IDEA-EQ', 'NSE:BSNL-EQ', 'NSE:MTNL-EQ', 'NSE:HFCL-EQ', 'NSE:STLTECH-EQ', 'NSE:GTPL-EQ', 'NSE:DEN-EQ', 'NSE:HATHWAY-EQ', 'NSE:SITI-EQ', 'NSE:ORTEL-EQ', 'NSE:TEJAS-EQ', 'NSE:RCOM-EQ', 'NSE:OPTIEMUS-EQ', 'NSE:ONEPOINT-EQ', 'NSE:CIGNITITEC-EQ', 'NSE:SMARTLINK-EQ', 'NSE:VINDHYATEL-EQ', 'NSE:TATACOMM-EQ', 'NSE:TANLA-EQ', 'NSE:ROUTE-EQ', 'NSE:ZENTEC-EQ', 'NSE:MOSCHIP-EQ', 'NSE:INDIGO-EQ', 'NSE:SPICEJET-EQ', 'NSE:JETAIRWAYS-EQ', 'NSE:TCI-EQ', 'NSE:VTL-EQ', 'NSE:ALLCARGO-EQ', 'NSE:BLUEDART-EQ', 'NSE:DELHIVERY-EQ', 'NSE:MAHLOG-EQ', 'NSE:SICAL-EQ', 'NSE:SNOWMAN-EQ', 'NSE:GATI-EQ', 'NSE:APOLLO-EQ', 'NSE:AEGISLOG-EQ', 'NSE:THOMASCOOK-EQ', 'NSE:COX&KINGS-EQ', 'NSE:KESARENT-EQ', 'NSE:YATRA-EQ', 'NSE:MAKEMYTRIP-EQ', 'NSE:EASEMYTRIP-EQ', 'NSE:IXIGO-EQ', 'NSE:ADANIPORTS-EQ', 'NSE:JSWINFRA-EQ', 'NSE:MHRIL-EQ', 'NSE:ESSELPACK-EQ', 'NSE:SAGCEM-EQ', 'NSE:INDIANHOTELS-EQ', 'NSE:LEMONTREE-EQ', 'NSE:CHALET-EQ', 'NSE:MAHINDRA-EQ', 'NSE:EIHOTEL-EQ', 'NSE:ITCHOTELS-EQ', 'NSE:ORIENTHOT-EQ', 'NSE:LEMON-EQ', 'NSE:TGBHOTELS-EQ', 'NSE:PARKHOTELS-EQ', 'NSE:KAMAT-EQ', 'NSE:ADVANI-EQ', 'NSE:SAMHI-EQ', 'NSE:BAJAJHLDNG-EQ', 'NSE:GODREJIND-EQ', 'NSE:SIEMENS-EQ', 'NSE:ABB-EQ', 'NSE:HONEYWELL-EQ', 'NSE:3M-EQ', 'NSE:TATA-EQ', 'NSE:BHARTI-EQ', 'NSE:ESSAR-EQ', 'NSE:JAIPRAKASH-EQ', 'NSE:GAMMON-EQ', 'NSE:PUNJ-EQ', 'NSE:LANCO-EQ', 'NSE:GMR-EQ', 'NSE:GVK-EQ', 'NSE:SIMPLEX-EQ', 'NSE:EMKAY-EQ']

# Maximum symbols to monitor (set to monitor all symbols)
MAX_SYMBOLS = len(STOCK_SYMBOLS)

# =============================================================================
# TELEGRAM HANDLER FOR AUTOMATED AUTHENTICATION
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
            if response.status_code == 200:
                return True
            else:
                print(f"❌ Failed to send Telegram message: {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error sending Telegram message: {e}")
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
                    return updates
            return []
        except Exception as e:
            print(f"❌ Error getting Telegram updates: {e}")
            return []
    
    def extract_auth_code(self, message_text):
        """Extract auth_code from Fyers redirect URL using regex"""
        try:
            # Regex pattern to match auth_code between "auth_code=" and "&state=None"
            pattern = r'auth_code=([^&]+)&state=None'
            match = re.search(pattern, message_text)
            if match:
                auth_code = match.group(1)
                print(f"✅ Auth code extracted: {auth_code[:20]}...")
                return auth_code
            else:
                print("❌ No auth_code found in message")
                return None
        except Exception as e:
            print(f"❌ Error extracting auth code: {e}")
            return None
    
    def wait_for_auth_code(self, timeout_seconds=TELEGRAM_AUTH_TIMEOUT):
        """Wait for auth code message from Telegram"""
        print(f"📱 Waiting for auth code from Telegram (timeout: {timeout_seconds}s)...")
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            updates = self.get_updates()
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"]
                    
                    # Check if message contains Fyers auth URL
                    if "fyersauth.vercel.app" in message_text and "auth_code=" in message_text:
                        auth_code = self.extract_auth_code(message_text)
                        if auth_code:
                            return auth_code
            
            time.sleep(TELEGRAM_POLLING_INTERVAL)
        
        print("⏰ Timeout waiting for auth code from Telegram")
        return None
    
    def check_for_restart_command(self):
        """Check for restart command in Telegram messages"""
        try:
            updates = self.get_updates()
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"].strip().lower()
                    
                    # Check if message is "restart" command
                    if message_text == "restart":
                        print("🔄 Restart command received from Telegram!")
                        return True
            
            return False
        except Exception as e:
            print(f"❌ Error checking for restart command: {e}")
            return False

# =============================================================================
# AUTHENTICATION CLASS
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
        self.last_relogin_time = 0  # Track when we last sent a relogin message
        self.relogin_interval = 300  # 5 minutes in seconds
        self.is_authenticated = False  # Track authentication status
        
    def generate_totp(self):
        totp = pyotp.TOTP(self.totp_secret)
        return totp.now()
    
    def load_saved_token(self):
        try:
            # Use hardcoded token instead of file
            current_time = time.time()
            token_time = FYERS_TOKEN_TIMESTAMP
            
            # Check if token is missing or empty
            if not FYERS_ACCESS_TOKEN or FYERS_ACCESS_TOKEN.strip() == "":
                print("❌ Fyers access token is missing or empty")
                self.is_authenticated = False
                self.send_relogin_message("Access token is missing")
                return False
            
            if current_time - token_time < 28800:  # 8 hours
                self.access_token = FYERS_ACCESS_TOKEN
                self.is_authenticated = True
                print("✅ Using hardcoded access token")
                return True
            else:
                print("🔄 Hardcoded token expired, need fresh authentication")
                self.is_authenticated = False
                self.send_relogin_message("Token expired (8 hours)")
                return False
            
        except Exception as e:
            print(f"❌ Error loading hardcoded token: {e}")
            self.is_authenticated = False
            self.send_relogin_message(f"Token loading error: {str(e)}")
            return False
    
    def send_relogin_message(self, reason):
        """Send Telegram message requesting re-authentication with retry logic"""
        current_time = time.time()
        
        # Check if enough time has passed since last relogin message
        if current_time - self.last_relogin_time < self.relogin_interval:
            remaining_time = int(self.relogin_interval - (current_time - self.last_relogin_time))
            print(f"⏳ Relogin message sent recently. Next message in {remaining_time} seconds")
            return False
        
        try:
            # Generate new auth URL
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

❌ <b>Reason:</b> {reason}

🌐 <b>Authorization URL:</b>
<code>{auth_url}</code>

🔢 <b>TOTP Code:</b> <code>{totp_code}</code>

📋 <b>Steps:</b>
1. Click the URL above
2. Login with your Fyers credentials
3. Use TOTP code if prompted for 2FA
4. After successful login, copy the entire redirect URL
5. Send the redirect URL back to this bot

⏰ <b>Timeout:</b> {TELEGRAM_AUTH_TIMEOUT} seconds
🔄 <b>Auto-retry:</b> Will send new link every 5 minutes until successful
            """
            
            if self.telegram.send_message(relogin_message):
                self.last_relogin_time = current_time
                print("📱 Re-authentication request sent to Telegram")
                return True
            else:
                print("⚠️ Failed to send re-authentication request to Telegram")
                return False
                
        except Exception as e:
            print(f"❌ Error sending re-authentication message: {e}")
            return False
    
    def save_token(self, token):
        # Update the hardcoded token constants with new values
        global FYERS_ACCESS_TOKEN, FYERS_TOKEN_TIMESTAMP, FYERS_TOKEN_CREATED_AT
        
        FYERS_ACCESS_TOKEN = token
        FYERS_TOKEN_TIMESTAMP = time.time()
        FYERS_TOKEN_CREATED_AT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Mark authentication as successful and reset retry timer
        self.is_authenticated = True
        self.last_relogin_time = 0  # Reset retry timer
        
        # Auto-update the code file with new constants
        self.update_code_file_with_new_token(token)
        
        print("💾 Hardcoded token updated with fresh authentication")
        print(f"🕐 Token created at: {FYERS_TOKEN_CREATED_AT}")
        print("✅ Code file automatically updated with new token constants")
        print("🔄 Retry timer reset - authentication successful")
    
    def update_code_file_with_new_token(self, token):
        """Automatically update the main.py file with new token constants"""
        try:
            # Read the current file
            with open(__file__, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Update the token constants
            new_timestamp = str(time.time())
            new_created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Replace the old constants with new ones
            content = content.replace(
                f'FYERS_ACCESS_TOKEN = "{FYERS_ACCESS_TOKEN}"',
                f'FYERS_ACCESS_TOKEN = "{token}"'
            )
            content = content.replace(
                f'FYERS_TOKEN_TIMESTAMP = {FYERS_TOKEN_TIMESTAMP}',
                f'FYERS_TOKEN_TIMESTAMP = {new_timestamp}'
            )
            content = content.replace(
                f'FYERS_TOKEN_CREATED_AT = "{FYERS_TOKEN_CREATED_AT}"',
                f'FYERS_TOKEN_CREATED_AT = "{new_created_at}"'
            )
            
            # Write back to file
            with open(__file__, 'w', encoding='utf-8') as f:
                f.write(content)
                
        except Exception as e:
            print(f"⚠️ Could not auto-update code file: {e}")
            print("💡 Please manually update the token constants in the code")
    
    def check_authentication_status(self):
        """Check if authentication is needed and send retry message if necessary"""
        if self.is_authenticated:
            return True
        
        # If not authenticated, try to send relogin message
        return self.send_relogin_message("Connection timeout - authentication required")
    
    def authenticate(self):
        print("🔐 Starting Fyers authentication...")
        
        if self.load_saved_token():
            self.fyers_model = fyersModel.FyersModel(
                client_id=self.client_id,
                token=self.access_token,
                log_path=""
            )
            
            # Test the connection to verify token is valid
            try:
                profile = self.fyers_model.get_profile()
                if profile['s'] == 'ok':
                    print("✅ Token is valid and connection successful")
                    self.is_authenticated = True
                    return True
                else:
                    print(f"❌ Token validation failed: {profile}")
                    self.is_authenticated = False
                    self.send_relogin_message("Token validation failed - invalid credentials")
                    return False
            except Exception as e:
                print(f"❌ Connection test failed: {e}")
                self.is_authenticated = False
                self.send_relogin_message(f"Connection test failed: {str(e)}")
                return False
        
        try:
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )
            
            response = session.generate_authcode()
            print(f"🌐 Authorization URL generated: {response}")
            
            totp_code = self.generate_totp()
            print(f"🔢 Current TOTP code (if needed): {totp_code}")
            
            # Send auth URL and instructions to Telegram
            telegram_message = f"""
🔐 <b>Fyers Authentication Required</b>

🌐 <b>Authorization URL:</b>
<code>{response}</code>

🔢 <b>TOTP Code:</b> <code>{totp_code}</code>

📋 <b>Steps:</b>
1. Click the URL above
2. Login with your Fyers credentials
3. Use TOTP code if prompted for 2FA
4. After successful login, copy the entire redirect URL
5. Send the redirect URL back to this bot

⏰ <b>Timeout:</b> {TELEGRAM_AUTH_TIMEOUT} seconds
            """
            
            if self.telegram.send_message(telegram_message):
                print("📱 Authentication instructions sent to Telegram")
            else:
                print("⚠️ Failed to send to Telegram, falling back to manual input")
            
            # Wait for auth code from Telegram
            auth_code = self.telegram.wait_for_auth_code()
            
            if not auth_code:
                print("❌ No auth code received from Telegram")
                print("🔄 Falling back to manual input...")
                auth_code = input("\n🔑 Enter the auth_code from redirect URL: ").strip()
                
                if not auth_code:
                    print("❌ No auth code provided!")
                    return False
            
            session.set_token(auth_code)
            token_response = session.generate_token()
            
            if token_response and token_response.get('s') == 'ok':
                self.access_token = token_response['access_token']
                self.save_token(self.access_token)
                
                self.fyers_model = fyersModel.FyersModel(
                    client_id=self.client_id,
                    token=self.access_token,
                    log_path=""
                )
                
                # Send success message to Telegram
                success_message = f"""
✅ <b>Authentication Successful!</b>

🕐 Token created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏰ Valid for: 8 hours
🔄 Auto-updated in code
                """
                self.telegram.send_message(success_message)
                
                print("✅ Authentication successful!")
                return True
            else:
                error_message = f"❌ <b>Authentication Failed:</b>\n<code>{token_response}</code>"
                self.telegram.send_message(error_message)
                self.is_authenticated = False
                print(f"❌ Authentication failed: {token_response}")
                return False
                
        except Exception as e:
            error_message = f"❌ <b>Authentication Error:</b>\n<code>{str(e)}</code>"
            self.telegram.send_message(error_message)
            self.is_authenticated = False
            print(f"❌ Authentication error: {e}")
            return False
    
    def get_fyers_model(self):
        if not self.fyers_model:
            if not self.authenticate():
                raise Exception("Authentication failed")
        return self.fyers_model

# =============================================================================
# GOOGLE SHEETS MANAGER WITH SECTOR SUPPORT
# =============================================================================

class GoogleSheetsManager:
    def __init__(self, detector=None):
        self.gc = None
        self.worksheet = None
        self.lock = threading.Lock()
        self.detector = detector  # Reference to the detector for accessing counts
        self.sheets_initialized = self.initialize_sheets()
        
        if not self.sheets_initialized:
            print("⚠️  Google Sheets initialization failed. Data will not be saved to sheets.")
    
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            # Check if credentials are available
            if GOOGLE_CREDENTIALS is None:
                print("❌ Google credentials not available from environment variables")
                return False
                
            # Define the scope
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            # Load credentials
            # Use credentials from environment variables
            creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scope)
            self.gc = gspread.authorize(creds)
            
            # Open your specific Google Sheet by ID
            try:
                sheet = self.gc.open_by_key(GOOGLE_SHEETS_ID)
                self.worksheet = sheet.sheet1
                print(f"✅ Connected to your Google Sheet!")
                print(f"📝 Sheet URL: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}")
                
                # Check if headers exist, if not add them
                try:
                    headers = self.worksheet.row_values(1)
                    if not headers or len(headers) < 10:
                        # Updated headers with Sector in column L
                        headers = [
                            'Date',           # A
                            'Time',           # B
                            'Symbol',         # C
                            'LTP',            # D
                            'Volume_Spike',   # E
                            'Trd_Val_Cr',     # F
                            'Spike_Type',     # G
                            'Sector',         # H
                            'Symbol_Count',   # I
                            'Sector_Count'    # J
                        ]
                        self.worksheet.insert_row(headers, 1)
                        print("✅ Added headers with Sector column to your sheet")
                except:
                    # Add headers if sheet is empty
                    headers = [
                        'Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike', 
                        'Trd_Val_Cr', 'Spike_Type', 'Sector', 'Symbol_Count', 'Sector_Count'
                    ]
                    self.worksheet.append_row(headers)
                    print("✅ Added headers with Sector column to your sheet")
                
            except gspread.SpreadsheetNotFound:
                print(f"❌ Could not access Google Sheet with ID: {GOOGLE_SHEETS_ID}")
                print("📋 Please make sure:")
                print("1. The sheet exists and is accessible")
                print("2. You've shared the sheet with your service account email")
                print("3. The service account has 'Editor' permissions")
                return False
            except Exception as e:
                print(f"❌ Error accessing Google Sheet: {e}")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ Error initializing Google Sheets: {e}")
            print("🔧 Troubleshooting steps:")
            print("1. Check if GOOGLE_SHEETS_ID is correct")
            print("2. Verify service account email has access to the sheet")
            print("3. Ensure Google credentials are valid")
            print("4. Check if the sheet exists and is not deleted")
            self.worksheet = None  # Ensure worksheet is None on failure
            return False
    
    def get_ltp_color_filter(self, symbol, current_ltp, previous_ltp):
        """
        Filter function to determine symbol color based on LTP change
        Returns color format for Google Sheets
        """
        try:
            if previous_ltp is None or previous_ltp == 0:
                return None  # No color formatting for first time
            
            ltp_change = current_ltp - previous_ltp
            
            if ltp_change > 0:
                # Green color for positive change
                return {
                    "backgroundColor": {
                        "red": 0.85,    # Light green background
                        "green": 0.95,
                        "blue": 0.85
                    },
                    "textFormat": {
                        "foregroundColor": {
                            "red": 0.0,   # Dark green text
                            "green": 0.5,
                            "blue": 0.0
                        },
                        "bold": True
                    }
                }
            elif ltp_change < 0:
                # Red color for negative change
                return {
                    "backgroundColor": {
                        "red": 0.95,    # Light red background
                        "green": 0.85,
                        "blue": 0.85
                    },
                    "textFormat": {
                        "foregroundColor": {
                            "red": 0.8,   # Dark red text
                            "green": 0.0,
                            "blue": 0.0
                        },
                        "bold": True
                    }
                }
            else:
                return None  # No change, no color
                
        except Exception as e:
            print(f"❌ Error in LTP color filter: {e}")
            return None
    
    def add_trade_to_sheets(self, symbol, ltp, volume_spike, trade_value, 
                           spike_type, previous_volume, current_volume, 
                           previous_ltp=None, ltp_color_format=None):
        """Add a new trade record to Google Sheets with sector information"""
        try:
            # Check if worksheet is properly initialized
            if self.worksheet is None:
                print(f"❌ Google Sheets not initialized. Cannot add {symbol} to sheets.")
                return False
                
            with self.lock:
                current_time = datetime.now()
                
                # Get sector for the symbol
                sector = get_sector_for_symbol(symbol)
                
                # Calculate LTP change
                ltp_change = round(ltp - previous_ltp, 2) if previous_ltp else 0
                
                # Get symbol count and sector count from the detector
                symbol_count = self.detector.individual_trades_detected if self.detector else 0
                sector_count = self.detector.sector_counts.get(sector, 0) if self.detector else 0
                
                row = [
                    current_time.strftime('%Y-%m-%d'),  # A - Date
                    current_time.strftime('%H:%M:%S'),  # B - Time
                    symbol,                             # C - Symbol
                    round(ltp, 2),                      # D - LTP
                    int(volume_spike),                  # E - Volume Spikeand dont remove the commented fields keep them commented as it is 
                    round(trade_value / 10000000, 2),   # F - Trade Value Crores
                    spike_type,                         # G - Spike Type
                    sector,                             # H - Sector
                    # symbol_count,                       # I - Symbol Count
                    # sector_count                        # J - Sector Count
                ]
                
                # Add the row first
                self.worksheet.append_row(row)
                
                # Apply color formatting to the symbol column if LTP color format is provided
                if ltp_color_format:
                    try:
                        # Get the last row number (the row we just added)
                        last_row = len(self.worksheet.get_all_values())
                        
                        # Format the symbol cell (column C, which is index 3)
                        cell_range = f"C{last_row}"
                        
                        # Apply formatting using the Sheets API
                        self.worksheet.format(cell_range, ltp_color_format)
                        
                        color_indicator = "🟢" if ltp_change > 0 else "🔴" if ltp_change < 0 else "⚪"
                        print(f"✅ Added to Google Sheets: {symbol} ({sector}) {color_indicator} - ₹{trade_value/10000000:.2f} crore (LTP: ₹{ltp:,.2f}, Change: {ltp_change:+.2f})")
                    except Exception as e:
                        print(f"⚠️ Added to sheets but color formatting failed: {e}")
                        print(f"✅ Added to Google Sheets: {symbol} ({sector}) - ₹{trade_value/10000000:.2f} crore")
                else:
                    print(f"✅ Added to Google Sheets: {symbol} ({sector}) - ₹{trade_value/10000000:.2f} crore")
                
                return True
                
        except Exception as e:
            print(f"❌ Error adding to Google Sheets: {e}")
            return False

# =============================================================================
# VOLUME SPIKE DETECTOR WITH SECTOR CLASSIFICATION
# =============================================================================

class VolumeSpikeDetector:
    def __init__(self):
        self.authenticator = FyersAuthenticator()
        self.sheets_manager = GoogleSheetsManager(self)  # Pass self as detector reference
        self.access_token = None
        self.fyers_ws = None
        self.total_ticks = 0
        self.individual_trades_detected = 0
        self.start_time = time.time()
        
        # Track previous volumes to detect spikes
        self.previous_volumes = {}
        self.last_alert_time = {}
        
        # Track previous LTP for color filter
        self.previous_ltp = {}
        
        # Sector statistics
        self.sector_counts = {}
        
        # WebSocket retry mechanism
        self.websocket_retry_count = 0
        self.max_websocket_retries = 10
        self.websocket_retry_delay = 5  # seconds
        
    def initialize(self):
        print("🚀 Initializing Enhanced Volume Spike Detector with Sector Classification...")
        print(f"📊 Monitoring {len(STOCK_SYMBOLS[:MAX_SYMBOLS])} symbols")
        print(f"💰 Individual trade threshold: ₹{INDIVIDUAL_TRADE_THRESHOLD:,} ({INDIVIDUAL_TRADE_THRESHOLD/10000000:.1f} crore)")
        print(f"📝 Google Sheet: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}")
        print(f"🏷️  Sector mapping: {len(SECTOR_MAPPING)} symbols mapped")
        
        # Send startup message to Telegram
        startup_message = f"""
🚀 <b>Volume Spike Detector Starting...</b>

📊 <b>Configuration:</b>
• Symbols: {len(STOCK_SYMBOLS[:MAX_SYMBOLS])}
• Threshold: ₹{INDIVIDUAL_TRADE_THRESHOLD:,} ({INDIVIDUAL_TRADE_THRESHOLD/10000000:.1f} crore)
• Sectors: {len(SECTOR_MAPPING)} mapped
• Google Sheet: <a href="https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}">View Sheet</a>

🕐 <b>Started:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        self.authenticator.telegram.send_message(startup_message)
        
        if not self.authenticator.authenticate():
            return False
        
        self.access_token = self.authenticator.access_token
        
        try:
            fyers = self.authenticator.get_fyers_model()
            profile = fyers.get_profile()
            if profile['s'] == 'ok':
                print(f"✅ Connected! User: {profile['data']['name']}")
                
                # Send connection success message
                connection_message = f"""
✅ <b>Fyers Connected Successfully!</b>

👤 <b>User:</b> {profile['data']['name']}
🔗 <b>Status:</b> Ready for monitoring
📡 <b>WebSocket:</b> Connecting...
                """
                self.authenticator.telegram.send_message(connection_message)
                
                return True
            else:
                print(f"❌ Connection test failed: {profile}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def on_connect(self, *args):
        print("✅ WebSocket connected successfully!")
        # Reset retry count on successful connection
        self.websocket_retry_count = 0
        print("🔄 WebSocket retry count reset to 0")
    
    def attempt_websocket_reconnection(self):
        """Attempt to reconnect WebSocket with retry logic"""
        if self.websocket_retry_count >= self.max_websocket_retries:
            print(f"❌ Maximum retries reached. Triggering re-authentication...")
            # Send Telegram notification about re-authentication
            reauth_message = f"""
🔄 <b>WebSocket Reconnection Failed</b>

❌ <b>Status:</b> Maximum retries ({self.max_websocket_retries}) reached
🔐 <b>Action:</b> Triggering re-authentication
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
            """
            self.authenticator.telegram.send_message(reauth_message)
            
            # Mark as not authenticated to trigger re-login
            self.authenticator.is_authenticated = False
            return False
        
        print(f"🔄 Attempting WebSocket reconnection {self.websocket_retry_count + 1}/{self.max_websocket_retries}")
        
        try:
            # Close existing connection if any
            if self.fyers_ws:
                self.fyers_ws.close_connection()
                time.sleep(2)
            
            # Create new WebSocket connection
            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{FYERS_CLIENT_ID}:{self.access_token}",
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=self.on_connect,
                on_close=self.on_disconnect,
                on_error=self.on_error,
                on_message=self.on_tick_received
            )
            
            print("🔌 Reconnecting to WebSocket...")
            self.fyers_ws.connect()
            time.sleep(3)
            
            # Resubscribe to symbols
            symbols_to_monitor = STOCK_SYMBOLS[:MAX_SYMBOLS]
            print("📡 Resubscribing to symbols...")
            self.fyers_ws.subscribe(symbols=symbols_to_monitor, data_type="SymbolUpdate")
            print("✅ WebSocket reconnection successful!")
            
            # Send success notification to Telegram
            success_message = f"""
✅ <b>WebSocket Reconnected Successfully!</b>

🔄 <b>Attempt:</b> {self.websocket_retry_count + 1}/{self.max_websocket_retries}
📡 <b>Status:</b> Connected and monitoring
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
            """
            self.authenticator.telegram.send_message(success_message)
            
            return True
            
        except Exception as e:
            print(f"❌ WebSocket reconnection attempt failed: {e}")
            return False
    
    def on_disconnect(self, *args):
        print("⚠️ WebSocket disconnected!")
        self.websocket_retry_count += 1
        print(f"🔄 WebSocket retry attempt {self.websocket_retry_count}/{self.max_websocket_retries}")
        
        if self.websocket_retry_count >= self.max_websocket_retries:
            print(f"❌ Maximum WebSocket retries ({self.max_websocket_retries}) reached. Triggering re-login...")
            # Mark as not authenticated to trigger re-login
            self.authenticator.is_authenticated = False
            # Reset retry count for next session
            self.websocket_retry_count = 0
        else:
            print(f"⏳ Waiting {self.websocket_retry_delay} seconds before retry...")
    
    def on_error(self, *args):
        error = args[-1] if args else "Unknown error"
        print(f"❌ WebSocket error: {error}")
        self.websocket_retry_count += 1
        print(f"🔄 WebSocket retry attempt {self.websocket_retry_count}/{self.max_websocket_retries}")
        
        if self.websocket_retry_count >= self.max_websocket_retries:
            print(f"❌ Maximum WebSocket retries ({self.max_websocket_retries}) reached. Triggering re-login...")
            # Mark as not authenticated to trigger re-login
            self.authenticator.is_authenticated = False
            # Reset retry count for next session
            self.websocket_retry_count = 0
        else:
            print(f"⏳ Waiting {self.websocket_retry_delay} seconds before retry...")
    
    def on_tick_received(self, *args):
        try:
            # Message is typically the last argument
            message = args[-1] if args else None
            
            if isinstance(message, dict):
                if message.get('type') in ['cn', 'ful', 'sub']:
                    print(f"📋 WebSocket Status: {message}")
                    return
                
                if 'symbol' in message:
                    self.detect_individual_trade(message)
                        
        except Exception as e:
            print(f"❌ Error in tick handler: {e}")
    
    def detect_individual_trade(self, tick_data):
        """Detect individual large trades via volume spikes with sector classification"""
        try:
            self.total_ticks += 1
            
            # Extract data
            symbol = tick_data.get('symbol', '')
            ltp = float(tick_data.get('ltp', 0))
            current_volume = float(tick_data.get('vol_traded_today', 0))
            
            if not symbol or ltp <= 0 or current_volume <= 0:
                return
            
            # Get previous volume
            previous_volume = self.previous_volumes.get(symbol, current_volume)
            
            # Get previous LTP for color filter
            previous_ltp = self.previous_ltp.get(symbol, None)
            
            # Calculate volume spike
            volume_spike = current_volume - previous_volume
            
            # Update tracking
            self.previous_volumes[symbol] = current_volume
            self.previous_ltp[symbol] = ltp
            
            # Only consider positive, significant volume spikes
            if volume_spike <= MIN_VOLUME_SPIKE:
                return
            
            # Calculate trade value for the volume spike
            individual_trade_value = ltp * volume_spike
            
            # Show first few spikes for debugging
            if self.total_ticks <= 10 and volume_spike > 100:
                sector = get_sector_for_symbol(symbol)
                print(f"\n📊 VOLUME SPIKE: {symbol} ({sector})")
                print(f"   LTP: ₹{ltp:,.2f}")
                print(f"   Volume Spike: {volume_spike:,.0f}")
                print(f"   Spike Trade Value: ₹{individual_trade_value:,.0f} ({individual_trade_value/10000000:.2f} crore)")
            
            # Check if this volume spike represents a large individual trade
            if individual_trade_value >= INDIVIDUAL_TRADE_THRESHOLD:
                # Avoid spam alerts
                last_alert = self.last_alert_time.get(symbol, 0)
                time_since_last = time.time() - last_alert
                
                if time_since_last > 60:  # 1 minute between alerts
                    self.individual_trades_detected += 1
                    self.last_alert_time[symbol] = time.time()
                    
                    # Get sector and update sector statistics
                    sector = get_sector_for_symbol(symbol)
                    self.sector_counts[sector] = self.sector_counts.get(sector, 0) + 1
                    
                    # Determine spike type
                    spike_percentage = (volume_spike / previous_volume * 100) if previous_volume > 0 else 0
                    if spike_percentage > 50:
                        spike_type = "LS"
                    elif spike_percentage > 20:
                        spike_type = "MS"
                    else:
                        spike_type = "VI"

                    ltp_change = ltp - previous_ltp if previous_ltp else 0
                    
                    print(f"\n🚨 LARGE INDIVIDUAL TRADE DETECTED!")
                    print(f"   Symbol: {symbol}")
                    print(f"   Sector: {sector}")
                    print(f"   LTP: ₹{ltp:,.2f} (Change: {ltp_change:+.2f})")
                    print(f"   Volume Spike: {volume_spike:,.0f} shares")
                    print(f"   Individual Trade Value: ₹{individual_trade_value:,.0f} ({individual_trade_value/10000000:.2f} crore)")
                    print(f"   Spike Type: {spike_type}")
                    
                    # Send Telegram alert
                    telegram_alert = f"""
🚨 <b>LARGE INDIVIDUAL TRADE DETECTED!</b>

📈 <b>Symbol:</b> <code>{symbol}</code>
🏷️ <b>Sector:</b> {sector}
💰 <b>LTP:</b> ₹{ltp:,.2f} ({ltp_change:+.2f})
📊 <b>Volume Spike:</b> {volume_spike:,.0f} shares
💵 <b>Trade Value:</b> ₹{individual_trade_value:,.0f} ({individual_trade_value/10000000:.2f} crore)
🎯 <b>Spike Type:</b> {spike_type}
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    self.authenticator.telegram.send_message(telegram_alert)
                    
                    # Get LTP color filter
                    ltp_color_format = self.sheets_manager.get_ltp_color_filter(symbol, ltp, previous_ltp)
                    
                    # Add to Google Sheets with sector information
                    success = self.sheets_manager.add_trade_to_sheets(
                        symbol=symbol,
                        ltp=ltp,
                        volume_spike=volume_spike,
                        trade_value=individual_trade_value,
                        spike_type=spike_type,
                        previous_volume=previous_volume,
                        current_volume=current_volume,
                        previous_ltp=previous_ltp,
                        ltp_color_format=ltp_color_format
                    )
                    
                    if not success:
                        print(f"   ❌ Failed to add to Google Sheets")
            
            # Print statistics every 2000 ticks
            if self.total_ticks % 2000 == 0:
                runtime = time.time() - self.start_time
                tracked_symbols = len(self.previous_volumes)
                
                print(f"\n📊 Statistics:")
                print(f"   Ticks processed: {self.total_ticks:,}")
                print(f"   Individual trades detected: {self.individual_trades_detected}")
                print(f"   Symbols tracked: {tracked_symbols}")
                print(f"   Runtime: {runtime/60:.1f} minutes")
                
                # Show sector breakdown
                if self.sector_counts:
                    print(f"   📈 Sector Breakdown:")
                    sorted_sectors = sorted(self.sector_counts.items(), key=lambda x: x[1], reverse=True)
                    for sector, count in sorted_sectors[:5]:  # Top 5 sectors
                        print(f"      {sector}: {count} trades")
                
                # Send statistics to Telegram every 10,000 ticks
                if self.total_ticks % 10000 == 0:
                    stats_message = f"""
📊 <b>Volume Spike Detector Statistics</b>

⏱️ <b>Runtime:</b> {runtime/60:.1f} minutes
📈 <b>Ticks Processed:</b> {self.total_ticks:,}
🚨 <b>Large Trades Detected:</b> {self.individual_trades_detected}
🎯 <b>Symbols Tracked:</b> {tracked_symbols}

📈 <b>Top Sectors:</b>
"""
                    if self.sector_counts:
                        sorted_sectors = sorted(self.sector_counts.items(), key=lambda x: x[1], reverse=True)
                        for sector, count in sorted_sectors[:5]:
                            stats_message += f"• {sector}: {count} trades\n"
                    
                    self.authenticator.telegram.send_message(stats_message)
                
        except Exception as e:
            print(f"❌ Error detecting individual trade: {e}")
    
    def start_monitoring(self):
        try:
            # Reset retry count for new monitoring session
            self.websocket_retry_count = 0
            print("🔄 WebSocket retry count reset for new monitoring session")
            
            # Wait for market to start if scheduling is enabled
            if SCHEDULING_ENABLED:
                wait_for_market_start()
            
            print(f"🔗 Creating WebSocket connection...")
            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{FYERS_CLIENT_ID}:{self.access_token}",
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=self.on_connect,
                on_close=self.on_disconnect,
                on_error=self.on_error,
                on_message=self.on_tick_received
            )
            
            symbols_to_monitor = STOCK_SYMBOLS[:MAX_SYMBOLS]
            print(f"📡 Subscribing to {len(symbols_to_monitor)} symbols...")
            print(f"🎯 Sample symbols: {symbols_to_monitor[:5]}...")
            
            print("🔌 Connecting to WebSocket...")
            self.fyers_ws.connect()
            time.sleep(3)  # Increased wait time for connection
            
            print("📊 Subscribing to symbol updates...")
            self.fyers_ws.subscribe(symbols=symbols_to_monitor, data_type="SymbolUpdate")
            print("✅ Successfully subscribed to symbols")
            print("⏳ Monitoring for volume spikes with sector classification...")
            print("📝 Data will be automatically added to Google Sheets with sector info")
            print("💡 Press Ctrl+C to stop")
            print("🔄 Waiting for market data...")
            
            # Keep the connection alive
            tick_count = 0
            while True:
                time.sleep(1)
                tick_count += 1
                
                # Check if WebSocket is still connected
                if hasattr(self.fyers_ws, 'ws') and (self.fyers_ws.ws is None or not self.fyers_ws.ws.connected):
                    print("⚠️ WebSocket connection lost during monitoring!")
                    self.websocket_retry_count += 1
                    print(f"🔄 WebSocket retry attempt {self.websocket_retry_count}/{self.max_websocket_retries}")
                    
                    if self.websocket_retry_count >= self.max_websocket_retries:
                        print(f"❌ Maximum WebSocket retries ({self.max_websocket_retries}) reached. Triggering re-login...")
                        # Mark as not authenticated to trigger re-login
                        self.authenticator.is_authenticated = False
                        # Reset retry count for next session
                        self.websocket_retry_count = 0
                        break
                    else:
                        print(f"⏳ Attempting to reconnect...")
                        if not self.attempt_websocket_reconnection():
                            print("❌ Reconnection failed, continuing with retry logic...")
                            time.sleep(self.websocket_retry_delay)
                        else:
                            print("✅ Reconnection successful, continuing monitoring...")
                            continue
                
                # Check if market has ended (every minute)
                if tick_count % 60 == 0:
                    if check_market_end():
                        print("🔚 Market session ended. Stopping monitoring...")
                        break
                
                # Check authentication status every 5 minutes (300 seconds)
                if tick_count % 300 == 0:
                    if not self.authenticator.is_authenticated:
                        print(f"🔐 Checking authentication status at tick {tick_count}")
                        self.authenticator.check_authentication_status()
                
                # Print a heartbeat every 30 seconds
                if tick_count % 30 == 0:
                    print(f"💓 Heartbeat: {tick_count}s - Ticks received: {self.total_ticks}")
                
                # Send heartbeat to Telegram every 5 minutes (300 seconds)
                if tick_count % 300 == 0:
                    runtime = time.time() - self.start_time
                    heartbeat_message = f"""
💓 <b>System Heartbeat</b>

⏱️ <b>Runtime:</b> {runtime/60:.1f} minutes
📈 <b>Ticks Received:</b> {self.total_ticks:,}
🚨 <b>Large Trades:</b> {self.individual_trades_detected}
🔗 <b>Status:</b> Active & Monitoring
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    print(f"📱 Sending Telegram heartbeat at tick {tick_count}")
                    success = self.authenticator.telegram.send_message(heartbeat_message)
                    if success:
                        print(f"✅ Telegram heartbeat sent successfully")
                    else:
                        print(f"❌ Failed to send Telegram heartbeat")
                
                # Check for restart command from Telegram every 30 seconds
                if tick_count % 30 == 0:
                    if self.authenticator.telegram.check_for_restart_command():
                        print("🔄 Restart command received! Triggering re-authentication...")
                        restart_message = f"""
🔄 <b>Manual Restart Requested</b>

📱 <b>Source:</b> Telegram Command
🔐 <b>Action:</b> Triggering re-authentication
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                        """
                        self.authenticator.telegram.send_message(restart_message)
                        
                        # Mark as not authenticated to trigger re-login
                        self.authenticator.is_authenticated = False
                        # Reset retry count
                        self.websocket_retry_count = 0
                        # Return True to indicate restart is needed
                        return True
                
        except KeyboardInterrupt:
            print("\n👋 Stopping by user request...")
            raise
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            self.websocket_retry_count += 1
            print(f"🔄 WebSocket retry attempt {self.websocket_retry_count}/{self.max_websocket_retries}")
            
            if self.websocket_retry_count >= self.max_websocket_retries:
                print(f"❌ Maximum WebSocket retries ({self.max_websocket_retries}) reached. Triggering re-login...")
                # Mark as not authenticated to trigger re-login
                self.authenticator.is_authenticated = False
                # Reset retry count for next session
                self.websocket_retry_count = 0
            else:
                print(f"⏳ Waiting {self.websocket_retry_delay} seconds before retry...")
                time.sleep(self.websocket_retry_delay)
                # Try to reconnect using the new method
                if not self.attempt_websocket_reconnection():
                    print("❌ Reconnection attempt failed, continuing with retry logic...")
        
        # Return False by default (no restart needed)
        return False
    
    def run(self):
        if self.initialize():
            print("✅ System initialized successfully!")
            print("=" * 70)
            
            try:
                restart_requested = self.start_monitoring()
                if restart_requested:
                    print("🔄 Restart requested via Telegram command. Triggering re-authentication...")
                    # Send notification to Telegram
                    reauth_message = f"""
🔄 <b>Restart Requested</b>

📱 <b>Source:</b> Telegram Command
🔐 <b>Action:</b> Triggering re-authentication
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    self.authenticator.telegram.send_message(reauth_message)
                    
                    # Try to restart monitoring after a delay
                    print("⏳ Waiting 30 seconds before attempting to restart...")
                    time.sleep(30)
                    self.run()  # Recursive restart
                    return  # Exit after recursive call
            except KeyboardInterrupt:
                print("\n👋 Enhanced Volume Spike Detector stopped. Goodbye!")
                if self.fyers_ws:
                    self.fyers_ws.close_connection()
                    
                # Final sector statistics
                if self.sector_counts:
                    print(f"\n📊 Final Sector Statistics:")
                    sorted_sectors = sorted(self.sector_counts.items(), key=lambda x: x[1], reverse=True)
                    for sector, count in sorted_sectors:
                        print(f"   {sector}: {count} trades")
                        
            except Exception as e:
                print(f"❌ Monitoring session failed: {e}")
                # Check if we need to trigger re-authentication
                if self.websocket_retry_count >= self.max_websocket_retries:
                    print("🔄 Maximum retries reached, triggering re-authentication...")
                    # Send notification to Telegram
                    reauth_message = f"""
🔄 <b>Monitoring Session Failed</b>

❌ <b>Error:</b> {str(e)}
🔄 <b>Retries:</b> {self.websocket_retry_count}/{self.max_websocket_retries}
🔐 <b>Action:</b> Triggering re-authentication
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    self.authenticator.telegram.send_message(reauth_message)
                    
                    # Mark as not authenticated to trigger re-login
                    self.authenticator.is_authenticated = False
                    # Reset retry count
                    self.websocket_retry_count = 0
                    
                    # Try to restart monitoring after a delay
                    print("⏳ Waiting 30 seconds before attempting to restart...")
                    time.sleep(30)
                    self.run()  # Recursive restart
                else:
                    print(f"🔄 Retry count: {self.websocket_retry_count}/{self.max_websocket_retries}")
                    print("⏳ Attempting to restart monitoring...")
                    time.sleep(10)
                    self.run()  # Recursive restart
                        
            finally:
                print(f"\n📊 Final Statistics:")
                print(f"   Total ticks: {self.total_ticks:,}")
                print(f"   Individual trades detected: {self.individual_trades_detected}")
                print(f"   Google Sheet: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}")
                
                # Send final statistics to Telegram
                if self.individual_trades_detected > 0:
                    final_stats_message = f"""
📊 <b>Session Statistics</b>

📈 <b>Total Ticks:</b> {self.total_ticks:,}
🚨 <b>Large Trades Detected:</b> {self.individual_trades_detected}
📊 <b>Google Sheet:</b> <a href="https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}">View Data</a>

📈 <b>Top Sectors:</b>
"""
                    if self.sector_counts:
                        sorted_sectors = sorted(self.sector_counts.items(), key=lambda x: x[1], reverse=True)
                        for sector, count in sorted_sectors[:5]:
                            final_stats_message += f"• {sector}: {count} trades\n"
                    
                    self.authenticator.telegram.send_message(final_stats_message)
        else:
            print("❌ System initialization failed!")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("🎯 Enhanced Fyers Volume Spike Detector - Google Sheets with Sector Classification")
    print("=" * 80)
    print("🔍 Detects large individual trades and updates Google Sheets")
    print("📊 Real-time updates with date, timestamp, and sector information")
    print("🏷️  Comprehensive sector mapping for NSE stocks")
    if SCHEDULING_ENABLED:
        print(f"⏰ Scheduling: {MARKET_START_TIME} - {MARKET_END_TIME}")
    else:
        print("⏰ Scheduling: Disabled (continuous monitoring)")
    print("=" * 80)
    
    # Check required packages
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        print("✅ All required packages available")
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("💡 Please install: pip install gspread google-auth fyers-apiv3 pyotp")
        return
    
    print("✅ Google Sheets credentials are hardcoded and ready to use")
    
    print(f"🏷️  Sector Mapping Statistics:")
    print(f"   Total symbols mapped: {len(SECTOR_MAPPING)}")
    
    # Count sectors
    sector_distribution = {}
    for symbol, sector in SECTOR_MAPPING.items():
        sector_distribution[sector] = sector_distribution.get(sector, 0) + 1
    
    print(f"   Unique sectors: {len(sector_distribution)}")
    print(f"   Top sectors by count:")
    sorted_sectors = sorted(sector_distribution.items(), key=lambda x: x[1], reverse=True)
    for sector, count in sorted_sectors[:5]:
        print(f"      {sector}: {count} symbols")
    
    detector = VolumeSpikeDetector()
    detector.run()

if __name__ == "__main__":
    main()
