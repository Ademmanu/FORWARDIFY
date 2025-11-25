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

# Default container limit (in MB). Can be overridden with CONTAINER_MAX_RAM_MB env.
DEFAULT_CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))


def _mb_from_bytes(n_bytes: int) -> float:
    """Convert bytes to MB with two decimals."""
    return round(n_bytes / (1024 * 1024), 2)


def _readable_mb(mb_value: float) -> str:
    """Return a human-readable MB string with two decimals."""
    return f"{mb_value:.2f} MB"


def _read_cgroup_memory_limit_bytes() -> int:
    """
    Attempt to detect container memory limit from cgroup.
    Returns:
      - positive integer bytes if a limit is detected
      - 0 if no limit detected / unlimited / couldn't read
    Checks (best-effort):
      - cgroup v2: /sys/fs/cgroup/memory.max (value 'max' means unlimited)
      - cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
    """
    candidates = [
        "/sys/fs/cgroup/memory.max",                     # cgroup v2 (sometimes top-level)
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",   # cgroup v1 common path
        # some environments expose a per-slice file under /sys/fs/cgroup/<...>/memory.max
        # trying the top-level paths usually works in Render and Docker.
    ]

    for path in candidates:
        try:
            if not os.path.exists(path):
                continue
            with open(path, "r") as fh:
                raw = fh.read().strip()
            # cgroup v2 uses "max" for no limit
            if raw == "max":
                return 0
            # sometimes a very large value indicates "no limit" (e.g., > 1 PiB)
            val = int(raw)
            if val <= 0:
                return 0
            # treat extremely large values as "unlimited"
            if val > (1 << 50):
                return 0
            return val
        except Exception:
            continue

    # If none of the above paths worked, try reading from /proc/self/cgroup for a hint
    try:
        with open("/proc/self/cgroup", "r") as fh:
            lines = fh.read().splitlines()
        # look for a memory controller line referencing a path; attempt to build candidate path
        for ln in lines:
            # Format can be "0::/some/path" (v2) or "2:memory:/docker/..." (v1)
            parts = ln.split(":")
            if len(parts) >= 3:
                controllers = parts[1]
                cpath = parts[2]
                if "memory" in controllers.split(","):
                    # try v1-style memory.limit_in_bytes under the found path
                    possible = f"/sys/fs/cgroup/memory{cpath}/memory.limit_in_bytes"
                    if os.path.exists(possible):
                        with open(possible, "r") as fh:
                            raw = fh.read().strip()
                        val = int(raw)
                        if val > 0 and val < (1 << 50):
                            return val
                    # try v2-style memory.max under the found path
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
    """
    Return detected container memory limit in MB.
    Priority:
     1) cgroup-detected limit (if available)
     2) CONTAINER_MAX_RAM_MB env (DEFAULT_CONTAINER_MAX_RAM_MB fallback)
    """
    bytes_limit = _read_cgroup_memory_limit_bytes()
    if bytes_limit and bytes_limit > 0:
        return _mb_from_bytes(bytes_limit)
    return float(DEFAULT_CONTAINER_MAX_RAM_MB)


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
                max-width: 600px;
                margin: 0 auto;
                text-align: left;
            }
            h1 { font-size: 2.2em; margin: 0; text-align: center; }
            p { font-size: 1.0em; }
            .emoji { font-size: 2.5em; text-align: center; }
            .stats { font-family: monospace; margin-top: 12px; }
        </style>
    </head>
    <body>
        <div class="status">
            <div class="emoji">🤖</div>
            <h1>Forwarder Bot Status</h1>
            <p>Bot is running. Use the monitoring endpoints:</p>
            <ul>
              <li>/health — basic uptime</li>
              <li>/mem — process memory usage (MB)</li>
              <li>/sysmem — system & container memory (MB)</li>
              <li>/webhook — simple webhook endpoint</li>
            </ul>
            <div class="stats">
              <strong>Container memory limit (detected):</strong> {container_limit} MB
            </div>
        </div>
    </body>
    </html>
    """.format(container_limit=get_container_memory_limit_mb())


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
      - container_total_mb (float)
      - rss_percent_of_container (float)
    """
    container_total_mb = get_container_memory_limit_mb()

    if resource is None:
        return {'status': 'unavailable', 'reason': 'resource module not available on this platform'}, 200

    try:
        r = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is typically in kilobytes on Linux. Convert to bytes then MB.
        ru = getattr(r, 'ru_maxrss', 0)
        rss_bytes = int(ru) * 1024
        rss_mb = _mb_from_bytes(rss_bytes)

        rss_percent_of_container = round((rss_mb / container_total_mb) * 100, 2) if container_total_mb else 0.0

        return {
            'status': 'ok',
            'rss_mb': rss_mb,
            'rss_readable': _readable_mb(rss_mb),
            'container_total_mb': container_total_mb,
            'rss_percent_of_container': rss_percent_of_container
        }, 200
    except Exception as e:
        return {'status': 'error', 'error': str(e)}, 500


@app.route('/sysmem', methods=['GET'])
def sysmem():
    """
    System memory statistics in MB and human-friendly strings.
    Preferred source: /proc/meminfo (Linux). Fallback: psutil if available.
    Also returns the detected container memory limit (MB).
    """
    container_total_mb = get_container_memory_limit_mb()
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

            used_percent_of_container = round((used_mb / container_total_mb) * 100, 2) if container_total_mb else 0.0

            return {
                'status': 'ok',
                'total_mb': total_mb,
                'available_mb': available_mb,
                'used_mb': used_mb,
                'total_readable': _readable_mb(total_mb),
                'available_readable': _readable_mb(available_mb),
                'used_readable': _readable_mb(used_mb),
                'used_percent': used_percent,
                'container_total_mb': container_total_mb,
                'used_percent_of_container': used_percent_of_container
            }, 200
        except Exception as e:
            return {'status': 'error', 'error': f'failed to read /proc/meminfo: {e}'}, 500

    # Fallback to psutil if available (cross-platform)
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            total_mb = round(vm.total / (1024 * 1024), 2)
            available_mb = round(vm.available / (1024 * 1024), 2)
            used_mb = round((vm.total - vm.available) / (1024 * 1024), 2)
            used_percent = round(vm.percent, 2)
            used_percent_of_container = round((used_mb / container_total_mb) * 100, 2) if container_total_mb else 0.0

            return {
                'status': 'ok',
                'total_mb': total_mb,
                'available_mb': available_mb,
                'used_mb': used_mb,
                'total_readable': _readable_mb(total_mb),
                'available_readable': _readable_mb(available_mb),
                'used_readable': _readable_mb(used_mb),
                'used_percent': used_percent,
                'container_total_mb': container_total_mb,
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
