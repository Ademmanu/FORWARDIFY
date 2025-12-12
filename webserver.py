from flask import Flask, request, jsonify
import threading
import time
import os
import logging

logger = logging.getLogger("webserver")
app = Flask(__name__)
START_TIME = time.time()

_cached_container_limit_mb = None
_monitor_callback = None

def register_monitoring(callback):
    global _monitor_callback
    _monitor_callback = callback

def _read_cgroup_memory_limit_bytes():
    candidates = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    
    for path in candidates:
        try:
            if os.path.exists(path):
                with open(path) as fh:
                    raw = fh.read().strip()
                if raw == "max":
                    return 0
                val = int(raw)
                if 0 < val < (1 << 50):
                    return val
        except Exception:
            continue
    
    return 0

def get_container_memory_limit_mb():
    global _cached_container_limit_mb
    if _cached_container_limit_mb is not None:
        return _cached_container_limit_mb
    
    bytes_limit = _read_cgroup_memory_limit_bytes()
    if bytes_limit > 0:
        _cached_container_limit_mb = round(bytes_limit / 1048576, 2)
    else:
        _cached_container_limit_mb = float(os.getenv("CONTAINER_MAX_RAM_MB", "512"))
    return _cached_container_limit_mb

@app.route("/")
def home():
    container_limit = get_container_memory_limit_mb()
    html = """<!DOCTYPE html><html><head><title>Bot Status</title><style>
    body{font-family:Arial,sans-serif;text-align:center;padding:50px;background:linear-gradient(135deg,#667eea 0%,#764ba2);color:white}
    .status{background:rgba(255,255,255,0.1);padding:30px;border-radius:15px;max-width:600px;margin:0 auto;text-align:left}
    h1{font-size:2.2em;margin:0;text-align:center}p{font-size:1.0em}
    .emoji{font-size:2.5em;text-align:center}.stats{font-family:monospace;margin-top:12px}</style></head>
    <body><div class="status"><div class="emoji">🤖</div><h1>Forwarder Bot Status</h1>
    <p>Bot is running. Use the monitoring endpoints:</p><ul>
    <li>/health — basic uptime</li><li>/webhook — simple webhook endpoint</li>
    <li>/metrics — forwarding subsystem metrics (if registered)</li></ul>
    <div class="stats"><strong>Container memory limit (detected):</strong> {} MB</div></div></body></html>"""
    return html.format(container_limit)

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "uptime_seconds": int(time.time() - START_TIME)})

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    now = int(time.time())
    if request.method == "POST":
        return jsonify({"status": "ok", "received": True, "timestamp": now, "data": request.get_json(silent=True)})
    return jsonify({"status": "ok", "method": "GET", "timestamp": now})

@app.route("/metrics")
def metrics():
    if _monitor_callback is None:
        return jsonify({"status": "unavailable", "reason": "no monitor registered"})
    
    try:
        data = _monitor_callback()
        return jsonify({"status": "ok", "metrics": data})
    except Exception as e:
        logger.exception("Monitoring callback failed")
        return jsonify({"status": "error", "error": str(e)}), 500

def run_server():
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)

def start_server_thread():
    threading.Thread(target=run_server, daemon=True).start()
    print("🌐 Web server started on port 5000")
