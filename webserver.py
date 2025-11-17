from flask import Flask
import threading

app = Flask(__name__)

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

@app.route('/ping')
def ping():
    return {'status': 'ok', 'message': 'Bot is alive!'}, 200

@app.route('/health')
def health():
    return {'status': 'healthy'}, 200

def run_server():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

def start_server_thread():
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    print("🌐 Web server started on port 5000")
