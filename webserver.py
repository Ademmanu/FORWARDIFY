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

# Container limit (in MB). Default 512 MB.
CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))
CONTAINER_TOTAL_BYTES = CONTAINER_MAX_RAM_MB * 1024 * 1024
CONTAINER_TOTAL_MB = float(CONTAINER_MAX_RAM_MB)


def _mb(n_bytes: int) -> float:
    """Convert bytes to MB (float)."""
    return round(n_bytes / (1024 * 1024), 2)


def _readable_mb(mb_value: float) -> str:
    """Return a human-readable MB string with two decimals."""
    return f"{mb_value:.2f} MB"


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
    Process memory usage (RSS) reported in MB and human-friendly strings.
    Uses resource.getrusage when available (common on Linux). Values returned:
      - rss_mb (float) and rss_readable (string)
      - container_total_mb (int)
      - rss_percent_of_container (float)
    """
    if resource is None:
        return {'status': 'unavailable', 'reason': 'resource module not available on this platform'}, 200

    try:
        r = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is in kilobytes on Linux; on some platforms it may be bytes.
        ru = getattr(r, 'ru_maxrss', 0)

        # Assume ru is KB (Linux), convert to bytes, then to MB.
        rss_bytes = int(ru) * 1024
        rss_mb = _mb(rss_bytes)

        # Safety: if rss_mb is implausibly large relative to container, but ru looked small,
        # we still present the computed value. (If needed, this heuristic can be expanded.)
        rss_percent_of_container = round((rss_mb / CONTAINER_TOTAL_MB) * 100, 2) if CONTAINER_TOTAL_MB else 0.0

        return {
            'status': 'ok',
            'rss_mb': rss_mb,
            'rss_readable': _readable_mb(rss_mb),
            'container_total_mb': CONTAINER_MAX_RAM_MB,
            'rss_percent_of_container': rss_percent_of_container
        }, 200
    except Exception as e:
        return {'status': 'error', 'error': str(e)}, 500


@app.route('/sysmem', methods=['GET'])
def sysmem():
    """
    System memory statistics in MB and human-friendly strings.
    Preferred source: /proc/meminfo (Linux). Fallback: psutil if available.
    Returned fields (examples):
      - total_mb, available_mb, used_mb (floats)
      - total_readable, available_readable, used_readable (strings)
      - used_percent (float)
      - container_total_mb, used_percent_of_container (float)
    """
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
                    meminfo[key] = int(val)  # values in kB

            total_kb = meminfo.get('MemTotal', 0)
            avail_kb = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
            used_kb = total_kb - avail_kb

            total_mb = round(total_kb / 1024, 2)
            available_mb = round(avail_kb / 1024, 2)
            used_mb = round(used_kb / 1024, 2)
            used_percent = round((used_mb / total_mb) * 100, 2) if total_mb else 0.0

            used_percent_of_container = round((used_mb / CONTAINER_TOTAL_MB) * 100, 2) if CONTAINER_TOTAL_MB else 0.0

            return {
                'status': 'ok',
                'total_mb': total_mb,
                'available_mb': available_mb,
                'used_mb': used_mb,
                'total_readable': _readable_mb(total_mb),
                'available_readable': _readable_mb(available_mb),
                'used_readable': _readable_mb(used_mb),
                'used_percent': used_percent,
                'container_total_mb': CONTAINER_MAX_RAM_MB,
                'used_percent_of_container': used_percent_of_container
            }, 200
        except Exception as e:
            return {'status': 'error', 'error': f'failed to read /proc/meminfo: {e}'}, 500

    # Fallback to psutil if available
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            total_mb = round(vm.total / (1024 * 1024), 2)
            available_mb = round(vm.available / (1024 * 1024), 2)
            used_mb = round((vm.total - vm.available) / (1024 * 1024), 2)
            used_percent = round(vm.percent, 2)
            used_percent_of_container = round((used_mb / CONTAINER_TOTAL_MB) * 100, 2) if CONTAINER_TOTAL_MB else 0.0

            return {
                'status': 'ok',
                'total_mb': total_mb,
                'available_mb': available_mb,
                'used_mb': used_mb,
                'total_readable': _readable_mb(total_mb),
                'available_readable': _readable_mb(available_mb),
                'used_readable': _readable_mb(used_mb),
                'used_percent': used_percent,
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
