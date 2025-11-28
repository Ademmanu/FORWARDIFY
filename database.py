import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional
import os
import logging

logger = logging.getLogger("database")

"""
Enhanced SQLite helper for FORWARDIFY with optimized methods for 20 users
"""

_conn_init_lock = threading.Lock()
_thread_local = threading.local()


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        # initialize DB schema
        try:
            self.init_db()
        except Exception:
            logger.exception("Failed initializing DB")

    def _apply_pragmas(self, conn: sqlite3.Connection):
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-20000;")
        except Exception:
            # best-effort - don't fail if environment doesn't allow these pragmas
            pass

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        # use sqlite3.Row for named column access; conversion of datetime kept off
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn

    def get_connection(self) -> sqlite3.Connection:
        """
        Return a thread-local connection (create if missing).
        Reuses connection across calls from the same thread to reduce overhead.
        If the connection becomes invalid it will be recreated.
        """
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                # cheap check to ensure connection is alive
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
                logger.exception("Failed to close DB connection")
            _thread_local.conn = None

    def init_db(self):
        with _conn_init_lock:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forwarding_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    label TEXT,
                    source_ids TEXT,
                    target_ids TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, label)
                )
            """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            conn.commit()

    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        # sqlite3.Row -> dict
        return {
            "user_id": row["user_id"],
            "phone": row["phone"],
            "name": row["name"],
            "session_data": row["session_data"],
            "is_logged_in": row["is_logged_in"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def save_user(
        self,
        user_id: int,
        phone: Optional[str] = None,
        name: Optional[str] = None,
        session_data: Optional[str] = None,
        is_logged_in: bool = False,
    ):
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

            updates.append("is_logged_in = ?")
            params.append(1 if is_logged_in else 0)

            updates.append("updated_at = ?")
            params.append(datetime.now().isoformat())

            params.append(user_id)
            query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            cur.execute(query, params)
        else:
            cur.execute(
                """
                INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                VALUES (?, ?, ?, ?, ?)
            """,
                (user_id, phone, name, session_data, 1 if is_logged_in else 0),
            )

        conn.commit()

    def add_forwarding_task(self, user_id: int, label: str, source_ids: List[int], target_ids: List[int]) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO forwarding_tasks (user_id, label, source_ids, target_ids)
                VALUES (?, ?, ?, ?)
            """,
                (user_id, label, json.dumps(source_ids), json.dumps(target_ids)),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_forwarding_task(self, user_id: int, label: str) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM forwarding_tasks WHERE user_id = ? AND label = ?", (user_id, label))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, label, source_ids, target_ids, is_active, created_at
            FROM forwarding_tasks
            WHERE user_id = ? AND is_active = 1
            ORDER BY created_at DESC
        """,
            (user_id,),
        )

        tasks = []
        for row in cur.fetchall():
            tasks.append(
                {
                    "id": row["id"],
                    "label": row["label"],
                    "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                    "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [],
                    "is_active": row["is_active"],
                    "created_at": row["created_at"],
                }
            )

        return tasks

    # ENHANCED METHOD: Add this for optimized task loading
    def get_user_tasks_limited(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Get limited number of tasks for a user (max 10)"""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, label, source_ids, target_ids, is_active, created_at
            FROM forwarding_tasks
            WHERE user_id = ? AND is_active = 1
            ORDER BY created_at DESC
            LIMIT ?
        """,
            (user_id, limit),
        )

        tasks = []
        for row in cur.fetchall():
            tasks.append(
                {
                    "id": row["id"],
                    "label": row["label"],
                    "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                    "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [],
                    "is_active": row["is_active"],
                    "created_at": row["created_at"],
                }
            )
        return tasks

    def get_all_active_tasks(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, id, label, source_ids, target_ids
            FROM forwarding_tasks
            WHERE is_active = 1
        """
        )
        tasks = []
        for row in cur.fetchall():
            tasks.append(
                {
                    "user_id": row["user_id"],
                    "id": row["id"],
                    "label": row["label"],
                    "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                    "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [],
                }
            )
        return tasks

    def is_user_allowed(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None

    def is_user_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row is not None and int(row["is_admin"]) == 1

    def add_allowed_user(self, user_id: int, username: Optional[str] = None, is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO allowed_users (user_id, username, is_admin, added_by)
                VALUES (?, ?, ?, ?)
            """,
                (user_id, username, 1 if is_admin else 0, added_by),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_allowed_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted

    def get_all_allowed_users(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, username, is_admin, added_by, created_at
            FROM allowed_users
            ORDER BY created_at DESC
        """
        )
        users = []
        for row in cur.fetchall():
            users.append(
                {
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "is_admin": row["is_admin"],
                    "added_by": row["added_by"],
                    "created_at": row["created_at"],
                }
            )
        return users

    # ENHANCED METHOD: Add this for user monitoring
    def get_active_users_count(self) -> int:
        """Get count of active users for monitoring"""
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM users WHERE is_logged_in = 1")
        row = cur.fetchone()
        return row[0] if row else 0

    def get_db_status(self) -> Dict:
        """
        Return a small health/status snapshot for the DB:
        - path
        - exists
        - size_bytes
        - user_version (PRAGMA)
        - row counts for key tables
        """
        status = {"path": self.db_path, "exists": False, "size_bytes": None, "user_version": None, "counts": {}}
        try:
            status["exists"] = os.path.exists(self.db_path)
            if status["exists"]:
                status["size_bytes"] = os.path.getsize(self.db_path)
        except Exception:
            logger.exception("Error reading DB file info")

        try:
            conn = self.get_connection()
            cur = conn.cursor()
            try:
                cur.execute("PRAGMA user_version;")
                row = cur.fetchone()
                # row can be a tuple or sqlite3.Row depending on driver
                if row:
                    try:
                        status["user_version"] = int(row[0])
                    except Exception:
                        try:
                            # fallback if row is a mapping
                            status["user_version"] = int(row["user_version"])
                        except Exception:
                            status["user_version"] = None
            except Exception:
                # ignore if PRAGMA unsupported
                status["user_version"] = None

            for table in ("users", "forwarding_tasks", "allowed_users"):
                try:
                    cur.execute(f"SELECT COUNT(1) as c FROM {table}")
                    crow = cur.fetchone()
                    if crow:
                        try:
                            cnt = crow["c"]
                        except Exception:
                            cnt = crow[0]
                        status["counts"][table] = int(cnt)
                    else:
                        status["counts"][table] = 0
                except Exception:
                    status["counts"][table] = None
        except Exception:
            logger.exception("Error querying DB status")

        return status

    def __del__(self):
        try:
            self.close_connection()
        except Exception:
            pass
