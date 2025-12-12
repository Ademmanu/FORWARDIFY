import sqlite3
import json
import threading
import os
import logging
from datetime import datetime

logger = logging.getLogger("database")

_conn_init_lock = threading.Lock()
_thread_local = threading.local()

class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        self.init_db()
    
    def _apply_pragmas(self, conn: sqlite3.Connection):
        try:
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA temp_store=MEMORY;
                PRAGMA cache_size=-2000;
                PRAGMA mmap_size=134217728;
            """)
        except Exception:
            pass
    
    def _create_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn
    
    def get_connection(self):
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _thread_local.conn = None
        
        try:
            _thread_local.conn = self._create_connection()
            return _thread_local.conn
        except Exception as e:
            logger.exception("Failed to create DB connection: %s", e)
            raise
    
    def close_connection(self):
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            _thread_local.conn = None
    
    def init_db(self):
        with _conn_init_lock:
            conn = self.get_connection()
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS forwarding_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    label TEXT,
                    source_ids TEXT,
                    target_ids TEXT,
                    filters TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, label)
                );
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at TEXT DEFAULT (datetime('now'))
                );
            """)
            conn.commit()
    
    def get_user(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    def save_user(self, user_id: int, phone=None, name=None, session_data=None, is_logged_in=False):
        conn = self.get_connection()
        cur = conn.cursor()
        existing = self.get_user(user_id)
        
        if existing:
            updates = []
            params = []
            if phone is not None:
                updates.append("phone = ?")
                params.append(phone)
            if name is not None:
                updates.append("name = ?")
                params.append(name)
            if session_data is not None:
                updates.append("session_data = ?")
                params.append(session_data)
            
            updates.append("is_logged_in = ?, updated_at = ?")
            params.extend([1 if is_logged_in else 0, datetime.now().isoformat(), user_id])
            query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            cur.execute(query, params)
        else:
            cur.execute(
                "INSERT INTO users (user_id, phone, name, session_data, is_logged_in) VALUES (?, ?, ?, ?, ?)",
                (user_id, phone, name, session_data, 1 if is_logged_in else 0)
            )
        conn.commit()
    
    def add_forwarding_task(self, user_id: int, label: str, source_ids: list, target_ids: list, filters=None):
        conn = self.get_connection()
        cur = conn.cursor()
        
        if filters is None:
            filters = {
                "filters": {"raw_text": False, "numbers_only": False, "alphabets_only": False,
                           "removed_alphabetic": False, "removed_numeric": False, "prefix": "", "suffix": ""},
                "outgoing": True, "forward_tag": False, "control": True
            }
        
        try:
            cur.execute(
                "INSERT INTO forwarding_tasks (user_id, label, source_ids, target_ids, filters) VALUES (?, ?, ?, ?, ?)",
                (user_id, label, json.dumps(source_ids), json.dumps(target_ids), json.dumps(filters))
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def update_task_filters(self, user_id: int, label: str, filters: dict):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE forwarding_tasks SET filters = ?, updated_at = ? WHERE user_id = ? AND label = ?",
            (json.dumps(filters), datetime.now().isoformat(), user_id, label)
        )
        conn.commit()
        return cur.rowcount > 0
    
    def remove_forwarding_task(self, user_id: int, label: str):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM forwarding_tasks WHERE user_id = ? AND label = ?", (user_id, label))
        conn.commit()
        return cur.rowcount > 0
    
    def get_user_tasks(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, label, source_ids, target_ids, filters, is_active, created_at FROM forwarding_tasks WHERE user_id = ? AND is_active = 1 ORDER BY created_at DESC",
            (user_id,)
        )
        tasks = []
        for row in cur.fetchall():
            try:
                filters_data = json.loads(row["filters"]) if row["filters"] else {}
            except Exception:
                filters_data = {}
            tasks.append({
                "id": row["id"], "label": row["label"], "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [], "filters": filters_data,
                "is_active": row["is_active"], "created_at": row["created_at"]
            })
        return tasks
    
    def get_all_active_tasks(self):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, label, source_ids, target_ids, filters FROM forwarding_tasks WHERE is_active = 1")
        tasks = []
        for row in cur.fetchall():
            try:
                filters_data = json.loads(row["filters"]) if row["filters"] else {}
            except Exception:
                filters_data = {}
            tasks.append({
                "user_id": row["user_id"], "label": row["label"], "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [], "filters": filters_data
            })
        return tasks
    
    def is_user_allowed(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None
    
    def is_user_admin(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row is not None and bool(row["is_admin"])
    
    def add_allowed_user(self, user_id: int, username=None, is_admin=False, added_by=None):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO allowed_users (user_id, username, is_admin, added_by) VALUES (?, ?, ?, ?)",
                (user_id, username, 1 if is_admin else 0, added_by)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def remove_allowed_user(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
        conn.commit()
        return cur.rowcount > 0
    
    def get_all_allowed_users(self):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, username, is_admin, added_by, created_at FROM allowed_users ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]
    
    def get_logged_in_users(self, limit=None):
        conn = self.get_connection()
        cur = conn.cursor()
        if limit and limit > 0:
            cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC LIMIT ?", (limit,))
        else:
            cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC")
        return [{"user_id": row["user_id"], "session_data": row["session_data"]} for row in cur.fetchall()]
    
    def get_user_phone_status(self, user_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT phone, is_logged_in FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return {"has_phone": False, "is_logged_in": False}
        return {"has_phone": bool(row["phone"]), "is_logged_in": bool(row["is_logged_in"])}
    
    def get_db_status(self):
        status = {"path": self.db_path, "exists": False, "size_bytes": None, "user_version": None, "counts": {}}
        try:
            status["exists"] = os.path.exists(self.db_path)
            if status["exists"]:
                status["size_bytes"] = os.path.getsize(self.db_path)
        except Exception:
            pass
        
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("PRAGMA user_version")
            row = cur.fetchone()
            if row:
                status["user_version"] = int(row[0])
            
            for table in ("users", "forwarding_tasks", "allowed_users"):
                try:
                    cur.execute(f"SELECT COUNT(1) as c FROM {table}")
                    row = cur.fetchone()
                    status["counts"][table] = int(row[0]) if row else 0
                except Exception:
                    status["counts"][table] = None
            self.close_connection()
        except Exception:
            pass
        return status
    
    def __del__(self):
        self.close_connection()
