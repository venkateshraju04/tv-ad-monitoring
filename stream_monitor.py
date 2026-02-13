#!/usr/bin/env python3
"""
Radio Stream Monitor - Captures, transcribes, and detects brands
Uses Groq Whisper for transcription and Llama for brand detection
Sends results to n8n webhook for Google Sheets integration

Usage: python3 stream_monitor.py
"""

import subprocess
import os
import time
import json
import tempfile
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============ CONFIGURATION ============

# Radio stream URL
STREAM_URL = os.getenv("STREAM_URL", "http://stream.live.vc.bbcmedia.co.uk/bbc_radio_one")

# Groq API settings
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# n8n webhook URL (create a webhook trigger in n8n and paste URL here)
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")

# Segment duration in seconds
SEGMENT_DURATION = 30

# Keep MP3 files after processing? (for debugging)
KEEP_AUDIO_FILES = os.getenv("KEEP_AUDIO_FILES", "false").lower() == "true"

# Output directory for audio files (if keeping them)
OUTPUT_DIR = Path("audio_segments")
OUTPUT_DIR.mkdir(exist_ok=True)

# Groq API endpoints
GROQ_WHISPER_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"

# =======================================


def check_dependencies():
    """Check if required dependencies are installed."""
    # Check ffmpeg
    try:
        subprocess.run(['ffmpeg', '-version'], 
                      capture_output=True, 
                      check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        print("❌ FFmpeg not found! Install it first:")
        print("   macOS: brew install ffmpeg")
        print("   Ubuntu: sudo apt install ffmpeg")
        return False
    
    # Check API keys
    if not GROQ_API_KEY:
        print("❌ GROQ_API_KEY not found in .env file!")
        print("   Get your key from: https://console.groq.com/keys")
        return False
    
    if not N8N_WEBHOOK_URL:
        print("⚠️  N8N_WEBHOOK_URL not set - results will only be printed to console")
        print("   Create a webhook in n8n and add URL to .env file")
    
    return True


def capture_audio_segment(segment_number):
    """Capture a single audio segment from the stream."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create temporary file or permanent file based on config
    if KEEP_AUDIO_FILES:
        output_path = OUTPUT_DIR / f"segment_{timestamp}_{segment_number:04d}.mp3"
    else:
        # Use temporary file that will be auto-deleted
        temp_file = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False)
        output_path = Path(temp_file.name)
        temp_file.close()
    
    # FFmpeg command
    ffmpeg_cmd = [
        'ffmpeg',
        '-i', STREAM_URL,
        '-t', str(SEGMENT_DURATION),
        '-acodec', 'libmp3lame',
        '-b:a', '128k',
        '-ar', '16000',  # 16kHz for speech recognition
        '-ac', '1',      # Mono
        '-y',
        '-loglevel', 'error',
        str(output_path)
    ]
    
    try:
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=SEGMENT_DURATION + 10)
        
        if result.returncode == 0 and output_path.exists():
            return output_path
        else:
            print(f"   ❌ FFmpeg error: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        print("   ❌ FFmpeg timeout")
        return None
    except Exception as e:
        print(f"   ❌ Capture error: {e}")
        return None


def transcribe_with_groq(audio_path):
    """Transcribe audio using Groq Whisper API."""
    try:
        with open(audio_path, 'rb') as audio_file:
            files = {
                'file': (audio_path.name, audio_file, 'audio/mpeg'),
            }
            data = {
                'model': 'whisper-large-v3-turbo',
                'response_format': 'json',
                'language': 'en'  # Adjust if needed
            }
            headers = {
                'Authorization': f'Bearer {GROQ_API_KEY}'
            }
            
            response = requests.post(
                GROQ_WHISPER_URL,
                files=files,
                data=data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get('text', '')
            else:
                print(f"   ❌ Groq Whisper error: {response.status_code} - {response.text}")
                return None
                
    except Exception as e:
        print(f"   ❌ Transcription error: {e}")
        return None


def extract_brands_with_llama(transcript):
    """Extract brand names using Llama via Groq."""
    try:
        prompt = f"""You are a brand detection AI. Analyze the following transcript and extract all brand names, product names, and company names mentioned.

Transcript: "{transcript}"

Return ONLY a JSON object with this exact format:
{{
  "brands": [
    {{"name": "Brand Name", "type": "organization"}},
    {{"name": "Product Name", "type": "product"}}
  ]
}}

If no brands are found, return: {{"brands": []}}

Important: Return ONLY the JSON object, no other text."""

        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a brand detection AI that returns only JSON responses."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 500
        }
        
        headers = {
            'Authorization': f'Bearer {GROQ_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(
            GROQ_CHAT_URL,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            
            # Parse the JSON response
            try:
                # Try to extract JSON from the response
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    brands_data = json.loads(json_match.group())
                    return brands_data.get('brands', [])
                else:
                    return []
            except json.JSONDecodeError:
                print(f"   ⚠️  Could not parse brand detection response")
                return []
        else:
            print(f"   ❌ Groq Llama error: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"   ❌ Brand extraction error: {e}")
        return []


def send_to_n8n(data):
    """Send results to n8n webhook."""
    if not N8N_WEBHOOK_URL:
        return False
    
    try:
        response = requests.post(
            N8N_WEBHOOK_URL,
            json=data,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            return True
        else:
            print(f"   ⚠️  n8n webhook error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ⚠️  n8n send error: {e}")
        return False


def process_segment(segment_number):
    """Process a single audio segment: capture, transcribe, extract brands, send to n8n."""
    timestamp = datetime.now()
    
    print(f"\n{'='*70}")
    print(f"🎬 Processing Segment #{segment_number}")
    print(f"⏰ {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    # Step 1: Capture audio
    print("📻 Capturing audio...", end=' ', flush=True)
    audio_path = capture_audio_segment(segment_number)
    if not audio_path:
        print("Failed!")
        return False
    file_size = audio_path.stat().st_size / 1024
    print(f"✅ ({file_size:.1f} KB)")
    
    # Step 2: Transcribe
    print("🎤 Transcribing...", end=' ', flush=True)
    transcript = transcribe_with_groq(audio_path)
    if not transcript:
        print("Failed!")
        if not KEEP_AUDIO_FILES:
            audio_path.unlink()
        return False
    print(f"✅")
    print(f"   📝 \"{transcript[:100]}{'...' if len(transcript) > 100 else ''}\"")
    
    # Step 3: Extract brands
    print("🏷️  Detecting brands...", end=' ', flush=True)
    brands = extract_brands_with_llama(transcript)
    print(f"✅ (Found {len(brands)})")
    if brands:
        brand_names = [b['name'] for b in brands]
        print(f"   🔖 {', '.join(brand_names)}")
    
    # Step 4: Send to n8n
    data = {
        "timestamp": timestamp.isoformat(),
        "segment_number": segment_number,
        "transcript": transcript,
        "brands": brands,
        "stream_url": STREAM_URL,
        "duration_seconds": SEGMENT_DURATION
    }
    
    if N8N_WEBHOOK_URL:
        print("📤 Sending to n8n...", end=' ', flush=True)
        if send_to_n8n(data):
            print("✅")
        else:
            print("Failed!")
    
    # Cleanup audio file if not keeping
    if not KEEP_AUDIO_FILES and audio_path.exists():
        audio_path.unlink()
    
    print(f"{'='*70}\n")
    return True


def run_monitor():
    """Main monitoring loop."""
    print("\n" + "="*70)
    print("🎙️  RADIO STREAM MONITOR")
    print("="*70)
    print(f"📻 Stream: {STREAM_URL}")
    print(f"⏱️  Segment Duration: {SEGMENT_DURATION} seconds")
    print(f"🤖 Transcription: Groq Whisper (whisper-large-v3-turbo)")
    print(f"🧠 Brand Detection: Llama 3.3 70B via Groq")
    print(f"📤 n8n Webhook: {'✅ Configured' if N8N_WEBHOOK_URL else '❌ Not configured'}")
    print(f"💾 Keep Audio Files: {'Yes' if KEEP_AUDIO_FILES else 'No'}")
    print("="*70)
    print("\nPress Ctrl+C to stop\n")
    
    segment_number = 0
    
    try:
        while True:
            segment_number += 1
            
            start_time = time.time()
            process_segment(segment_number)
            elapsed = time.time() - start_time
            
            # Small delay to avoid hammering the stream
            if elapsed < 2:
                time.sleep(2 - elapsed)
                
    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        print("🛑 Monitor stopped")
        print(f"📊 Total segments processed: {segment_number}")
        print("="*70)


if __name__ == "__main__":
    if check_dependencies():
        run_monitor()
