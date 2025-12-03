from flask import Flask, request, jsonify
import threading
import time
import os
import logging
import psutil
import gc
from datetime import datetime, timedelta

logger = logging.getLogger("webserver")

app = Flask(__name__)

START_TIME = time.time()
LAST_REQUEST_TIME = time.time()

# Memory management
DEFAULT_CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))
MEMORY_WARNING_THRESHOLD = 0.85  # 85% of container limit
MEMORY_CRITICAL_THRESHOLD = 0.95  # 95% of container limit

# Request rate limiting
REQUEST_COUNTS = {}
REQUEST_LIMIT_PER_MINUTE = 60
REQUEST_WINDOW_SECONDS = 60

# Cache container limit
_cached_container_limit_mb = None
_monitor_callback = None
_health_check_callback = None


def register_monitoring(callback):
    """Register callback for metrics collection"""
    global _monitor_callback
    _monitor_callback = callback
    logger.info("Monitoring callback registered")


def register_health_check(callback):
    """Register callback for health checks"""
    global _health_check_callback
    _health_check_callback = callback
    logger.info("Health check callback registered")


def _mb_from_bytes(n_bytes: int) -> float:
    return round(n_bytes / (1024 * 1024), 2)


def _read_cgroup_memory_limit_bytes() -> int:
    """Read container memory limit from cgroup"""
    candidates = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]

    for path in candidates:
        try:
            if not os.path.exists(path):
                continue
            with open(path, "r") as fh:
                raw = fh.read().strip()
            if raw == "max":
                return 0
            val = int(raw)
            if val <= 0:
                return 0
            if val > (1 << 50):
                return 0
            return val
        except Exception:
            continue

    try:
        with open("/proc/self/cgroup", "r") as fh:
            lines = fh.read().splitlines()
        for ln in lines:
            parts = ln.split(":")
            if len(parts) >= 3:
                controllers = parts[1]
                cpath = parts[2]
                if "memory" in controllers.split(","):
                    possible = f"/sys/fs/cgroup/memory{cpath}/memory.limit_in_bytes"
                    if os.path.exists(possible):
                        with open(possible, "r") as fh:
                            raw = fh.read().strip()
                        val = int(raw)
                        if val > 0 and val < (1 << 50):
                            return val
                    possible2 = f"/sys/fs/cgroup{cpath}/memory.max"
                    if os.path.exists(possible2):
                        with open(possible2, "r") as fh:
                            raw = fh.read().strip()
                        if raw != "max":
                            val = int(raw)
                            if val > 0 and val < (1 << 50):
                                return val
    except Exception:
        pass

    return 0


def get_container_memory_limit_mb() -> float:
    """Get container memory limit in MB"""
    global _cached_container_limit_mb
    if _cached_container_limit_mb is not None:
        return _cached_container_limit_mb

    bytes_limit = _read_cgroup_memory_limit_bytes()
    if bytes_limit and bytes_limit > 0:
        _cached_container_limit_mb = _mb_from_bytes(bytes_limit)
    else:
        _cached_container_limit_mb = float(os.getenv("CONTAINER_MAX_RAM_MB", str(DEFAULT_CONTAINER_MAX_RAM_MB)))
    return _cached_container_limit_mb


def get_memory_usage() -> dict:
    """Get current memory usage statistics"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        
        # Get system memory info
        system_memory = psutil.virtual_memory()
        
        # Get container limit
        container_limit_mb = get_container_memory_limit_mb()
        container_limit_bytes = container_limit_mb * 1024 * 1024
        
        # Calculate usage percentages
        process_percent = (memory_info.rss / container_limit_bytes) if container_limit_bytes > 0 else 0
        system_percent = system_memory.percent / 100
        
        return {
            "process_rss_mb": _mb_from_bytes(memory_info.rss),
            "process_vms_mb": _mb_from_bytes(memory_info.vms),
            "process_shared_mb": _mb_from_bytes(memory_info.shared),
            "process_percent": round(process_percent * 100, 2),
            "system_available_mb": _mb_from_bytes(system_memory.available),
            "system_percent": round(system_percent * 100, 2),
            "container_limit_mb": container_limit_mb,
            "status": "OK" if process_percent < MEMORY_WARNING_THRESHOLD else 
                     "WARNING" if process_percent < MEMORY_CRITICAL_THRESHOLD else 
                     "CRITICAL"
        }
    except Exception as e:
        logger.exception("Error getting memory usage")
        return {"error": str(e)}


def rate_limit_check(ip: str) -> bool:
    """Simple rate limiting by IP"""
    global REQUEST_COUNTS
    
    now = time.time()
    
    # Clean old entries
    cutoff = now - REQUEST_WINDOW_SECONDS
    REQUEST_COUNTS = {ip: ts for ip, ts in REQUEST_COUNTS.items() if ts > cutoff}
    
    # Count requests from this IP in the last window
    count = sum(1 for ts in REQUEST_COUNTS.values() if ts > cutoff and ip in REQUEST_COUNTS)
    
    if count >= REQUEST_LIMIT_PER_MINUTE:
        return False
    
    REQUEST_COUNTS[ip] = now
    return True


@app.before_request
def before_request():
    """Update last request time and check rate limits"""
    global LAST_REQUEST_TIME
    LAST_REQUEST_TIME = time.time()
    
    # Rate limiting for non-health endpoints
    if request.endpoint not in ['health', 'metrics', 'webhook']:
        if not rate_limit_check(request.remote_addr):
            return jsonify({"error": "Too many requests"}), 429


@app.after_request
def after_request(response):
    """Add security headers and update response"""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Cache-Control'] = 'no-store, max-age=0'
    return response


@app.route("/", methods=["GET"])
def home():
    """Main status page"""
    container_limit = get_container_memory_limit_mb()
    memory_usage = get_memory_usage()
    uptime = int(time.time() - START_TIME)
    
    status_color = "green" if memory_usage.get("status") == "OK" else "orange" if memory_usage.get("status") == "WARNING" else "red"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Forwarder Bot Status</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                min-height: 100vh;
            }}
            .container {{
                max-width: 800px;
                margin: 0 auto;
                background: rgba(255,255,255,0.1);
                backdrop-filter: blur(10px);
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.2);
            }}
            .header {{
                text-align: center;
                margin-bottom: 30px;
            }}
            .emoji {{
                font-size: 3em;
                margin: 0;
            }}
            h1 {{
                margin: 10px 0;
                font-size: 2.2em;
            }}
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin: 30px 0;
            }}
            .stat-card {{
                background: rgba(255,255,255,0.15);
                padding: 20px;
                border-radius: 10px;
                text-align: center;
            }}
            .stat-value {{
                font-size: 1.8em;
                font-weight: bold;
                margin: 10px 0;
            }}
            .endpoints {{
                margin-top: 30px;
                padding: 20px;
                background: rgba(255,255,255,0.1);
                border-radius: 10px;
            }}
            .endpoint-list {{
                list-style: none;
                padding: 0;
            }}
            .endpoint-list li {{
                padding: 10px;
                margin: 5px 0;
                background: rgba(255,255,255,0.1);
                border-radius: 5px;
            }}
            .status-badge {{
                display: inline-block;
                padding: 5px 15px;
                border-radius: 20px;
                background: {status_color};
                font-size: 0.9em;
                margin-left: 10px;
            }}
            .memory-bar {{
                height: 20px;
                background: rgba(255,255,255,0.2);
                border-radius: 10px;
                margin: 10px 0;
                overflow: hidden;
            }}
            .memory-fill {{
                height: 100%;
                background: {status_color};
                width: {min(memory_usage.get('process_percent', 0), 100)}%;
                transition: width 0.3s;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="emoji">🤖</div>
                <h1>Forwarder Bot Status</h1>
                <p>High-performance message forwarding system</p>
                <div class="status-badge">{memory_usage.get('status', 'UNKNOWN')}</div>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div>Uptime</div>
                    <div class="stat-value">{timedelta(seconds=uptime)}</div>
                </div>
                <div class="stat-card">
                    <div>Memory Usage</div>
                    <div class="stat-value">{memory_usage.get('process_rss_mb', 0)} / {container_limit} MB</div>
                    <div class="memory-bar">
                        <div class="memory-fill"></div>
                    </div>
                    <div>{memory_usage.get('process_percent', 0)}%</div>
                </div>
                <div class="stat-card">
                    <div>Container Limit</div>
                    <div class="stat-value">{container_limit} MB</div>
                </div>
            </div>
            
            <div class="endpoints">
                <h3>📡 Available Endpoints:</h3>
                <ul class="endpoint-list">
                    <li><strong>/health</strong> — System health check</li>
                    <li><strong>/metrics</strong> — Live performance metrics</li>
                    <li><strong>/webhook</strong> — Webhook test endpoint</li>
                    <li><strong>/memory</strong> — Memory usage details</li>
                    <li><strong>/gc</strong> — Manual garbage collection</li>
                </ul>
            </div>
            
            <div style="text-align: center; margin-top: 30px; font-size: 0.9em; opacity: 0.8;">
                Render Free Tier Optimized • Last request: {datetime.now().strftime('%H:%M:%S')}
            </div>
        </div>
    </body>
    </html>
    """
    return html


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint for Render"""
    uptime = int(time.time() - START_TIME)
    
    # Collect health information
    health_data = {
        "status": "healthy",
        "uptime_seconds": uptime,
        "timestamp": datetime.now().isoformat(),
        "service": "forwarder-bot",
        "version": "1.0.0"
    }
    
    # Add memory health
    memory = get_memory_usage()
    health_data["memory"] = memory
    
    # Check memory health
    if memory.get("status") == "CRITICAL":
        health_data["status"] = "degraded"
        health_data["warning"] = "High memory usage"
    
    # Run custom health check if registered
    if _health_check_callback:
        try:
            custom_health = _health_check_callback()
            health_data.update(custom_health)
        except Exception as e:
            health_data["custom_check_error"] = str(e)
    
    # Force garbage collection if memory is high
    if memory.get("process_percent", 0) > 70:
        gc.collect()
    
    return jsonify(health_data), 200 if health_data["status"] == "healthy" else 503


@app.route("/metrics", methods=["GET"])
def metrics():
    """Metrics endpoint for monitoring"""
    if _monitor_callback is None:
        return jsonify({"status": "unavailable", "reason": "no monitor registered"}), 200

    try:
        metrics_data = _monitor_callback()
        
        # Add system metrics
        metrics_data["system"] = {
            "timestamp": datetime.now().isoformat(),
            "uptime": int(time.time() - START_TIME),
            "python_version": os.sys.version,
            "platform": os.sys.platform
        }
        
        # Add memory metrics
        metrics_data["memory"] = get_memory_usage()
        
        # Add GC stats
        gc_stats = {
            "collected": gc.get_count()[0],
            "uncollectable": len(gc.garbage),
            "threshold": gc.get_threshold(),
            "enabled": gc.isenabled()
        }
        metrics_data["gc"] = gc_stats
        
        return jsonify({"status": "ok", "metrics": metrics_data}), 200
    except Exception as e:
        logger.exception("Monitoring callback failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/memory", methods=["GET"])
def memory():
    """Detailed memory usage information"""
    memory_data = get_memory_usage()
    
    # Add GC information
    memory_data["gc"] = {
        "collected": gc.get_count(),
        "threshold": gc.get_threshold(),
        "garbage": len(gc.garbage)
    }
    
    # Add process info
    try:
        process = psutil.Process()
        memory_data["process"] = {
            "cpu_percent": process.cpu_percent(interval=0.1),
            "num_threads": process.num_threads(),
            "create_time": process.create_time(),
            "connections": len(process.connections())
        }
    except Exception:
        pass
    
    return jsonify({"status": "ok", "memory": memory_data}), 200


@app.route("/gc", methods=["POST"])
def trigger_gc():
    """Trigger manual garbage collection"""
    try:
        collected = gc.collect()
        return jsonify({
            "status": "success",
            "collected": collected,
            "garbage": len(gc.garbage),
            "message": f"Garbage collection freed {collected} objects"
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    """Simple webhook endpoint for testing"""
    now = int(time.time())
    
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        return jsonify({
            "status": "ok",
            "received": True,
            "timestamp": now,
            "data_keys": list(data.keys()) if data else [],
            "method": "POST"
        }), 200
    else:
        return jsonify({
            "status": "ok",
            "method": "GET",
            "timestamp": now,
            "message": "Webhook endpoint is active"
        }), 200


def get_last_request_time():
    """Get last request time for inactivity check"""
    return LAST_REQUEST_TIME


def run_server():
    """Run Flask server with optimized settings for Render"""
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
        use_reloader=False,
        threaded=True
    )


def start_server_thread():
    """Start web server in a daemon thread"""
    server_thread = threading.Thread(
        target=run_server,
        daemon=True,
        name="WebServer"
    )
    server_thread.start()
    logger.info("🌐 Web server started on port %s", os.getenv("PORT", "5000"))
    return server_thread
