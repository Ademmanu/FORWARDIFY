from flask import Flask, request
import threading
import time
import os

# resource is available on Unix; wrap in try for portability
try:
    import resource
except Exception:
    resource = None

# optional fallback for system memory on non-Linux platforms
try:
    import psutil
except Exception:
    psutil = None

app = Flask(__name__)

START_TIME = time.time()

# Container limit (in MB). Default 512 MB as requested.
CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))
CONTAINER_TOTAL_BYTES = CONTAINER_MAX_RAM_MB * 1024 * 1024
CONTAINER_TOTAL_KB = CONTAINER_MAX_RAM_MB * 1024  # for use with kB units from /proc/meminfo

@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Bot Status</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                text-align: center;
                padding: 50px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }
            .status {
                background: rgba(255,255,255,0.1);
                padding: 30px;
                border-radius: 15px;
                max-width: 500px;
                margin: 0 auto;
            }
            h1 { font-size: 2.5em; margin: 0; }
            p { font-size: 1.2em; }
            .emoji { font-size: 3em; }
        </style>
    </head>
    <body>
        <div class="status">
            <div class="emoji">🤖</div>
            <h1>Bot is Running!</h1>
            <p>Telegram Forwarder Bot is online and active.</p>
            <p style="font-size: 0.9em; margin-top: 30px;">
                ✅ Keep pinging this URL to prevent the bot from sleeping.
            </p>
        </div>
    </body>
    </html>
    """

# Removed /ping as /health is now used for monitoring
@app.route('/health', methods=['GET'])
def health():
    """Health endpoint for monitoring systems. Returns basic status and uptime."""
    uptime = int(time.time() - START_TIME)
    return {'status': 'healthy', 'uptime_seconds': uptime}, 200

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    """Simple webhook endpoint for monitoring or external hooks."""
    now = int(time.time())
    if request.method == 'POST':
        data = request.get_json(silent=True)
        return {'status': 'ok', 'received': True, 'timestamp': now, 'data': data}, 200
    else:
        return {'status': 'ok', 'method': 'GET', 'timestamp': now}, 200

@app.route('/mem', methods=['GET'])
def mem():
    """
    Process memory usage (RSS).
    Uses resource.getrusage when available. Values returned in kilobytes and bytes.
    Also reports usage relative to CONTAINER_MAX_RAM_MB.
    """
    if resource is None:
        return {'status': 'unavailable', 'reason': 'resource module not available on this platform'}, 200

    try:
        r = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is in kilobytes on Linux; multiply to bytes for convenience
        rss_kb = int(getattr(r, 'ru_maxrss', 0))
        rss_bytes = rss_kb * 1024
        container_total_bytes = CONTAINER_TOTAL_BYTES
        rss_percent_of_container = round((rss_bytes / container_total_bytes) * 100, 2) if container_total_bytes else 0.0
        return {
            'status': 'ok',
            'rss_kb': rss_kb,
            'rss_bytes': rss_bytes,
            'container_total_mb': CONTAINER_MAX_RAM_MB,
            'rss_percent_of_container': rss_percent_of_container
        }, 200
    except Exception as e:
        return {'status': 'error', 'error': str(e)}, 500

@app.route('/sysmem', methods=['GET'])
def sysmem():
    """
    System memory statistics. Prefer /proc/meminfo on Linux; fallback to psutil if available.
    Returns total, available, used, and percent. Also includes container-limited totals (CONTAINER_MAX_RAM_MB).
    """
    # Try /proc/meminfo (Linux)
    meminfo_path = '/proc/meminfo'
    if os.path.exists(meminfo_path):
        try:
            meminfo = {}
            with open(meminfo_path, 'r') as f:
                for line in f:
                    parts = line.split(':')
                    if len(parts) < 2:
                        continue
                    key = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    meminfo[key] = int(val)  # values are in kB

            total_kb = meminfo.get('MemTotal', 0)
            avail_kb = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
            used_kb = total_kb - avail_kb
            percent = round((used_kb / total_kb) * 100, 2) if total_kb else 0.0

            # compute percent relative to container limit (container reported in kB)
            container_total_kb = CONTAINER_TOTAL_KB
            used_percent_of_container = round((used_kb / container_total_kb) * 100, 2) if container_total_kb else 0.0

            return {
                'status': 'ok',
                'total_kb': total_kb,
                'available_kb': avail_kb,
                'used_kb': used_kb,
                'used_percent': percent,
                'container_total_mb': CONTAINER_MAX_RAM_MB,
                'used_percent_of_container': used_percent_of_container
            }, 200
        except Exception as e:
            return {'status': 'error', 'error': f'failed to read /proc/meminfo: {e}'}, 500

    # Fallback to psutil if available
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            total = vm.total
            available = vm.available
            used = total - available
            percent = vm.percent
            container_total_bytes = CONTAINER_TOTAL_BYTES
            used_percent_of_container = round((used / container_total_bytes) * 100, 2) if container_total_bytes else 0.0
            return {
                'status': 'ok',
                'total_bytes': total,
                'available_bytes': available,
                'used_bytes': used,
                'used_percent': percent,
                'container_total_mb': CONTAINER_MAX_RAM_MB,
                'used_percent_of_container': used_percent_of_container
            }, 200
        except Exception as e:
            return {'status': 'error', 'error': str(e)}, 500

    return {'status': 'unavailable', 'reason': 'platform-specific meminfo not available and psutil not installed'}, 200

def run_server():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

def start_server_thread():
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    print("🌐 Web server started on port 5000")
