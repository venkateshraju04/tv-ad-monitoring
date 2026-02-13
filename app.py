 #  Start by making sure the `websocket-client` and `pyaudio` packages are installed.
# If not, you can install it by running the following command:
# pip install websocket-client pyaudio supabase python-dotenv

import os
import ssl
import pyaudio
import websocket
import json
import threading
import time
import wave
from urllib.parse import urlencode
from datetime import datetime
import certifi

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Run: pip install python-dotenv")

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("Warning: supabase package not installed. Run: pip install supabase")

# Replace with your chosen API key, this is the "default" account api key
API_KEY = "0bcfdb832dd94ceda407053fd06c075c"

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "")  # Add your Supabase URL
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")  # Add your Supabase anon/service key
SUPABASE_TABLE = "transcripts"  # Table name in Supabase

# Auto-save configuration
AUTO_SAVE_INTERVAL = 60  # Save JSON file every 1 minute (in seconds)
AUTO_SAVE_TRANSCRIPT_COUNT = 50  # Or save after 50 transcripts, whichever comes first

CONNECTION_PARAMS = {
    "sample_rate": 16000,
    "format_turns": True,
    "enable_extra_session_information": True,
    "entity_detection": True  # Enable brand/entity detection
}
API_ENDPOINT_BASE_URL = "wss://streaming.assemblyai.com/v3/ws"
API_ENDPOINT = f"{API_ENDPOINT_BASE_URL}?{urlencode(CONNECTION_PARAMS)}"

# Audio Configuration
FRAMES_PER_BUFFER = 800  # 50ms of audio (0.05s * 16000Hz)
SAMPLE_RATE = CONNECTION_PARAMS["sample_rate"]
CHANNELS = 1
FORMAT = pyaudio.paInt16

# Global variables for audio stream and websocket
audio = None
stream = None
ws_app = None
audio_thread = None
stop_event = threading.Event()  # To signal the audio thread to stop

# WAV recording variables
recorded_frames = []  # Store audio frames for WAV file
recording_lock = threading.Lock()  # Thread-safe access to recorded_frames

# Transcript storage
transcript_data = []  # Temporary storage for periodic JSON saves
transcript_lock = threading.Lock()
session_start_time = None
session_id = None
last_save_time = None
transcript_count = 0

# Supabase client
supabase_client = None

# Auto-save thread
auto_save_thread = None

# --- WebSocket Event Handlers ---

def on_open(ws):
    """Called when the WebSocket connection is established."""
    global last_save_time, supabase_client
    print("WebSocket connection opened.")
    print(f"Connected to: {API_ENDPOINT}")
    last_save_time = time.time()
    
    # Initialize Supabase client
    if SUPABASE_AVAILABLE and SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✅ Supabase connected - real-time uploads enabled")
        except Exception as e:
            print(f"⚠️  Supabase connection failed: {e}")
            supabase_client = None
    else:
        print("⚠️  Supabase not configured - only local JSON saves will work")
    
    # Start auto-save thread
    start_auto_save_thread()

    # Start sending audio data in a separate thread
    def stream_audio():
        global stream
        print("Starting audio streaming...")
        while not stop_event.is_set():
            try:
                audio_data = stream.read(FRAMES_PER_BUFFER, exception_on_overflow=False)

                # Store audio data for WAV recording
                with recording_lock:
                    recorded_frames.append(audio_data)

                # Send audio data as binary message
                ws.send(audio_data, websocket.ABNF.OPCODE_BINARY)
            except Exception as e:
                print(f"Error streaming audio: {e}")
                # If stream read fails, likely means it's closed, stop the loop
                break
        print("Audio streaming stopped.")

    global audio_thread
    audio_thread = threading.Thread(target=stream_audio)
    audio_thread.daemon = (
        True  # Allow main thread to exit even if this thread is running
    )
    audio_thread.start()

def on_message(ws, message):
    global session_id, session_start_time, transcript_count
    try:
        data = json.loads(message)
        msg_type = data.get('type')

        if msg_type == "Begin":
            session_id = data.get('id')
            expires_at = data.get('expires_at')
            session_start_time = datetime.now()
            print(f"\nSession began: ID={session_id}, ExpiresAt={datetime.fromtimestamp(expires_at)}")
            print(f"Auto-save: Every {AUTO_SAVE_INTERVAL//60} minutes OR {AUTO_SAVE_TRANSCRIPT_COUNT} transcripts\n")
            
        elif msg_type == "Turn":
            transcript = data.get('transcript', '')
            formatted = data.get('turn_is_formatted', False)
            entities = data.get('entities', [])

            if formatted and transcript.strip():  # Only process non-empty formatted transcripts
                # Extract brands from entities
                brands = []
                for entity in entities:
                    entity_type = entity.get('entity_type', '').lower()
                    if entity_type in ['organization', 'product', 'brand', 'company']:
                        brands.append({
                            'name': entity.get('text'),
                            'type': entity_type,
                            'confidence': entity.get('confidence', 0)
                        })
                
                # Create structured transcript entry
                transcript_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'session_id': session_id,
                    'transcript': transcript,
                    'brands': brands,
                    'entities': entities,
                    'word_count': len(transcript.split())
                }
                
                # Store in memory for periodic saves
                with transcript_lock:
                    transcript_data.append(transcript_entry)
                    transcript_count += 1
                
                # Upload to Supabase immediately
                upload_to_supabase_realtime(transcript_entry)
                
                # Display
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] {transcript}")
                if brands:
                    brand_names = ', '.join([b['name'] for b in brands])
                    print(f"  🏷️  BRANDS: {brand_names}")
                
            else:
                # Live updates (partial transcripts)
                print(f"\r[LIVE] {transcript}", end='', flush=True)
                
        elif msg_type == "Termination":
            audio_duration = data.get('audio_duration_seconds', 0)
            session_duration = data.get('session_duration_seconds', 0)
            print(f"\nSession Terminated: Audio Duration={audio_duration}s, Session Duration={session_duration}s")
            
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}")
    except Exception as e:
        print(f"Error handling message: {e}")

def on_error(ws, error):
    """Called when a WebSocket error occurs."""
    print(f"\nWebSocket Error: {error}")
    # Attempt to signal stop on error
    stop_event.set()

def on_close(ws, close_status_code, close_msg):
    """Called when the WebSocket connection is closed."""
    print(f"\nWebSocket Disconnected: Status={close_status_code}, Msg={close_msg}")

    # Final save of any remaining transcripts
    save_json_file(final=True)
    
    # Save recorded audio to WAV file
    save_wav_file()

    # Ensure audio resources are released
    global stream, audio
    stop_event.set()  # Signal audio thread and auto-save thread to stop

    if stream:
        if stream.is_active():
            stream.stop_stream()
        stream.close()
        stream = None
    if audio:
        audio.terminate()
        audio = None
    # Try to join the audio thread to ensure clean exit
    if audio_thread and audio_thread.is_alive():
        audio_thread.join(timeout=1.0)

def upload_to_supabase_realtime(transcript_entry):
    """Upload a single transcript to Supabase immediately."""
    if not supabase_client:
        return  # Skip if not configured
    
    try:
        data = {
            'session_id': transcript_entry['session_id'],
            'timestamp': transcript_entry['timestamp'],
            'transcript': transcript_entry['transcript'],
            'brands': json.dumps(transcript_entry['brands']),
            'entities': json.dumps(transcript_entry['entities']),
            'word_count': transcript_entry['word_count']
        }
        supabase_client.table(SUPABASE_TABLE).insert(data).execute()
    except Exception as e:
        # Silently log errors to avoid cluttering output
        pass  # You can uncomment below for debugging
        # print(f"\n⚠️  Supabase upload error: {e}")

def start_auto_save_thread():
    """Start background thread for periodic JSON saves."""
    global auto_save_thread
    
    def auto_save_loop():
        global last_save_time, transcript_count
        while not stop_event.is_set():
            time.sleep(5)  # Check every 5 seconds
            
            current_time = time.time()
            time_elapsed = current_time - last_save_time
            
            # Save if time interval reached OR transcript count reached
            should_save = (
                time_elapsed >= AUTO_SAVE_INTERVAL or 
                transcript_count >= AUTO_SAVE_TRANSCRIPT_COUNT
            )
            
            if should_save and transcript_data:
                save_json_file(final=False)
                last_save_time = current_time
    
    auto_save_thread = threading.Thread(target=auto_save_loop, daemon=True)
    auto_save_thread.start()

def save_json_file(final=False):
    """Save transcripts to a JSON file and clear memory."""
    global transcript_count
    
    if not transcript_data:
        return

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"transcripts_{timestamp}.json"

    try:
        with transcript_lock:
            output_data = {
                'metadata': {
                    'session_id': session_id,
                    'session_start': session_start_time.isoformat() if session_start_time else None,
                    'file_created': datetime.now().isoformat(),
                    'total_transcripts': len(transcript_data),
                    'is_final': final
                },
                'transcripts': transcript_data.copy()  # Copy current data
            }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        status = "FINAL" if final else "AUTO-SAVE"
        print(f"\n\n{'='*60}")
        print(f"💾 [{status}] JSON saved: {filename}")
        print(f"📊 Transcripts in file: {len(transcript_data)}")
        print(f"{'='*60}\n")

        # Clear memory after successful save (but not on final save)
        if not final:
            with transcript_lock:
                transcript_data.clear()
                transcript_count = 0

    except Exception as e:
        print(f"\n❌ Error saving JSON file: {e}")

def save_wav_file():
    """Save recorded audio frames to a WAV file."""
    if not recorded_frames:
        print("No audio data recorded.")
        return

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"recorded_audio_{timestamp}.wav"

    try:
        with wave.open(filename, 'wb') as wf:
            wf.setnchannels(CHANNELS)
            wf.setsampwidth(2)  # 16-bit = 2 bytes
            wf.setframerate(SAMPLE_RATE)

            # Write all recorded frames
            with recording_lock:
                wf.writeframes(b''.join(recorded_frames))

        print(f"🎵 Audio saved to: {filename}")
        print(f"Duration: {len(recorded_frames) * FRAMES_PER_BUFFER / SAMPLE_RATE:.2f} seconds")

    except Exception as e:
        print(f"Error saving WAV file: {e}")

# --- Main Execution ---
def run():
    global audio, stream, ws_app

    # Validate API Key
    if not API_KEY:
        print("ERROR: API key not found!")
        print("Please set the ASSEMBLY_AI_API_KEY environment variable:")
        print("  export ASSEMBLY_AI_API_KEY='your_api_key_here'")
        print("Or run with: ASSEMBLY_AI_API_KEY='your_api_key_here' python3 app.py")
        return

    # Initialize PyAudio
    audio = pyaudio.PyAudio()

    # Open microphone stream
    try:
        stream = audio.open(
            input=True,
            frames_per_buffer=FRAMES_PER_BUFFER,
            channels=CHANNELS,
            format=FORMAT,
            rate=SAMPLE_RATE,
        )
        print("Microphone stream opened successfully.")
        print("Speak into your microphone. Press Ctrl+C to stop.")
        print("Audio will be saved to a WAV file when the session ends.")
    except Exception as e:
        print(f"Error opening microphone stream: {e}")
        if audio:
            audio.terminate()
        return  # Exit if microphone cannot be opened

    # Create WebSocketApp
    ws_app = websocket.WebSocketApp(
        API_ENDPOINT,
        header={"Authorization": API_KEY},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Run WebSocketApp in a separate thread to allow main thread to catch KeyboardInterrupt
    ws_thread = threading.Thread(target=lambda: ws_app.run_forever(sslopt={"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()}))
    ws_thread.daemon = True
    ws_thread.start()

    try:
        # Keep main thread alive until interrupted
        while ws_thread.is_alive():
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nCtrl+C received. Stopping...")
        stop_event.set()  # Signal audio thread to stop

        # Send termination message to the server
        if ws_app and ws_app.sock and ws_app.sock.connected:
            try:
                terminate_message = {"type": "Terminate"}
                print(f"Sending termination message: {json.dumps(terminate_message)}")
                ws_app.send(json.dumps(terminate_message))
                # Give a moment for messages to process before forceful close
                time.sleep(5)
            except Exception as e:
                print(f"Error sending termination message: {e}")

        # Close the WebSocket connection (will trigger on_close)
        if ws_app:
            ws_app.close()

        # Wait for WebSocket thread to finish
        ws_thread.join(timeout=2.0)

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        stop_event.set()
        if ws_app:
            ws_app.close()
        ws_thread.join(timeout=2.0)

    finally:
        # Final cleanup (already handled in on_close, but good as a fallback)
        if stream and stream.is_active():
            stream.stop_stream()
        if stream:
            stream.close()
        if audio:
            audio.terminate()
        print("Cleanup complete. Exiting.")

if __name__ == "__main__":
    run()
