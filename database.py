import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import logging

logger = logging.getLogger("database")

"""
Optimized SQLite helper for FORWARDIFY - Fixed connection management
"""

_conn_init_lock = threading.Lock()
_thread_local = threading.local()


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        try:
            self.init_db()
        except Exception: 
            logger.exception("Failed initializing DB")

    def _apply_pragmas(self, conn: sqlite3.Connection):
        """Apply SQLite pragmas for optimization"""
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-512;")  # REDUCED from -1000 to save ~500KB
            conn.execute("PRAGMA mmap_size=134217728;")  # REDUCED from 256MB to 128MB
            conn.execute("PRAGMA query_only=FALSE;")
        except Exception: 
            pass

    def _create_connection(self) -> sqlite3.Connection:
        """Create optimized database connection"""
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn

    def get_connection(self) -> sqlite3.Connection:
        """Return thread-local connection with health check"""
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
            logger.exception("Failed to create DB connection:  %s", e)
            raise

    def close_connection(self):
        """Close thread-local connection"""
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                logger.exception("Failed to close DB connection")
            _thread_local. conn = None

    def init_db(self):
        """Initialize database schema with indexes"""
        with _conn_init_lock:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # Create tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT UNIQUE,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS forwarding_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    source_ids TEXT NOT NULL,
                    target_ids TEXT NOT NULL,
                    filters TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                    UNIQUE(user_id, label)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)

            # Create indexes for faster queries
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON forwarding_tasks(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON forwarding_tasks(is_active)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_logged_in ON users(is_logged_in)")

            conn.commit()

    def get_user(self, user_id: int) -> Optional[Dict]: 
        """Fetch user with minimal data conversion"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return dict(row)
        except Exception as e:
            logger.exception("Error in get_user for %s: %s", user_id, e)
            raise

    def save_user(
        self,
        user_id: int,
        phone: Optional[str] = None,
        name: Optional[str] = None,
        session_data: Optional[str] = None,
        is_logged_in: bool = False,
    ):
        """Optimized user save with conditional updates"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            existing = self.get_user(user_id)

            if existing:
                updates = []
                params = []

                if phone is not None:
                    updates. append("phone = ?")
                    params.append(phone)
                if name is not None: 
                    updates.append("name = ?")
                    params.append(name)
                if session_data is not None:
                    updates.append("session_data = ?")
                    params.append(session_data)

                updates.append("is_logged_in = ?")
                params.append(1 if is_logged_in else 0)
                updates.append("updated_at = ? ")
                params.append(datetime.now().isoformat())

                params.append(user_id)
                query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
                cur. execute(query, params)
            else:
                cur.execute(
                    "INSERT INTO users (user_id, phone, name, session_data, is_logged_in) VALUES (?, ?, ?, ?, ?)",
                    (user_id, phone, name, session_data, 1 if is_logged_in else 0),
                )

            conn.commit()
        except Exception as e:
            logger.exception("Error in save_user for %s: %s", user_id, e)
            raise

    def add_forwarding_task(self, user_id: int, label: str, source_ids: List[int], target_ids: List[int], filters: Optional[Dict[str, Any]] = None) -> bool:
        """Add forwarding task with default filters"""
        conn = self. get_connection()
        try:
            cur = conn.cursor()
            if filters is None:
                filters = {
                    "filters": {
                        "raw_text": False,
                        "numbers_only": False,
                        "alphabets_only": False,
                        "removed_alphabetic": False,
                        "removed_numeric": False,
                        "prefix":  "",
                        "suffix": ""
                    },
                    "outgoing": True,
                    "forward_tag": False,
                    "control":  True
                }
            
            cur.execute(
                "INSERT INTO forwarding_tasks (user_id, label, source_ids, target_ids, filters) VALUES (?, ?, ?, ?, ?)",
                (user_id, label, json.dumps(source_ids), json.dumps(target_ids), json.dumps(filters)),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e: 
            logger.exception("Error in add_forwarding_task for %s: %s", user_id, e)
            raise

    def update_task_filters(self, user_id: int, label: str, filters: Dict[str, Any]) -> bool:
        """Update task filters only"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE forwarding_tasks SET filters = ? WHERE user_id = ?  AND label = ?",
                (json.dumps(filters), user_id, label),
            )
            updated = cur.rowcount > 0
            conn.commit()
            return updated
        except Exception as e:
            logger.exception("Error in update_task_filters for %s, task %s: %s", user_id, label, e)
            raise

    def remove_forwarding_task(self, user_id: int, label:  str) -> bool:
        """Delete forwarding task"""
        conn = self.get_connection()
        try:
            cur = conn. cursor()
            cur.execute("DELETE FROM forwarding_tasks WHERE user_id = ? AND label = ? ", (user_id, label))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception as e:
            logger.exception("Error in remove_forwarding_task for %s:  %s", user_id, e)
            raise

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        """Get active tasks with optimized queries"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, label, source_ids, target_ids, filters, is_active, created_at FROM forwarding_tasks WHERE user_id = ? AND is_active = 1 ORDER BY created_at DESC",
                (user_id,),
            )

            tasks = []
            for row in cur.fetchall():
                try:
                    filters_data = json.loads(row["filters"]) if row["filters"] else {}
                except (json.JSONDecodeError, TypeError):
                    filters_data = {}
                    
                tasks.append({
                    "id": row["id"],
                    "label": row["label"],
                    "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                    "target_ids": json. loads(row["target_ids"]) if row["target_ids"] else [],
                    "filters":  filters_data,
                    "is_active": row["is_active"],
                    "created_at": row["created_at"],
                })

            return tasks
        except Exception as e:
            logger.exception("Error in get_user_tasks for %s: %s", user_id, e)
            raise

    def get_all_active_tasks(self) -> List[Dict]:
        """Get all active tasks with minimal memory usage"""
        conn = self. get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id, id, label, source_ids, target_ids, filters FROM forwarding_tasks WHERE is_active = 1"
            )
            tasks = []
            for row in cur.fetchall():
                try:
                    filters_data = json.loads(row["filters"]) if row["filters"] else {}
                except (json.JSONDecodeError, TypeError):
                    filters_data = {}
                    
                tasks.append({
                    "user_id": row["user_id"],
                    "id": row["id"],
                    "label": row["label"],
                    "source_ids": json.loads(row["source_ids"]) if row["source_ids"] else [],
                    "target_ids": json.loads(row["target_ids"]) if row["target_ids"] else [],
                    "filters": filters_data,
                })
            return tasks
        except Exception as e:
            logger.exception("Error in get_all_active_tasks:  %s", e)
            raise

    def is_user_allowed(self, user_id: int) -> bool:
        """Check if user is in allowed list"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM allowed_users WHERE user_id = ?  LIMIT 1", (user_id,))
            return cur.fetchone() is not None
        except Exception as e:
            logger. exception("Error in is_user_allowed for %s: %s", user_id, e)
            raise

    def is_user_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?  LIMIT 1", (user_id,))
            row = cur.fetchone()
            return row is not None and int(row["is_admin"]) == 1
        except Exception as e:
            logger.exception("Error in is_user_admin for %s: %s", user_id, e)
            raise

    def add_allowed_user(self, user_id: int, username: Optional[str] = None, is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        """Add allowed user"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO allowed_users (user_id, username, is_admin, added_by) VALUES (?, ?, ?, ?)",
                (user_id, username, 1 if is_admin else 0, added_by),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logger.exception("Error in add_allowed_user for %s: %s", user_id, e)
            raise

    def remove_allowed_user(self, user_id: int) -> bool:
        """Remove allowed user"""
        conn = self. get_connection()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM allowed_users WHERE user_id = ? ", (user_id,))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception as e:
            logger.exception("Error in remove_allowed_user for %s: %s", user_id, e)
            raise

    def get_all_allowed_users(self) -> List[Dict]:
        """Get all allowed users"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT user_id, username, is_admin, added_by, created_at FROM allowed_users ORDER BY created_at DESC"
            )
            users = []
            for row in cur.fetchall():
                users.append({
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "is_admin": row["is_admin"],
                    "added_by": row["added_by"],
                    "created_at": row["created_at"],
                })
            return users
        except Exception as e:
            logger.exception("Error in get_all_allowed_users: %s", e)
            raise

    def get_logged_in_users(self, limit: Optional[int] = None) -> List[Dict]:
        """Get logged-in users with limit support"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            if limit and int(limit) > 0:
                cur.execute(
                    "SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC LIMIT ?",
                    (int(limit),),
                )
            else:
                cur.execute(
                    "SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC"
                )
            rows = cur.fetchall()
            result = []
            for r in rows:
                result.append({"user_id": r["user_id"], "session_data": r["session_data"]})
            return result
        except Exception as e:
            logger.exception("Error fetching logged-in users: %s", e)
            raise

    def get_user_phone_status(self, user_id: int) -> Dict:
        """Get user phone status"""
        conn = self.get_connection()
        try:
            cur = conn. cursor()
            cur.execute("SELECT phone, is_logged_in FROM users WHERE user_id = ? ", (user_id,))
            row = cur.fetchone()
            if not row:
                return {"has_phone": False, "is_logged_in": False}
            
            has_phone = row["phone"] is not None and row["phone"] != ""
            return {"has_phone": has_phone, "is_logged_in":  bool(row["is_logged_in"])}
        except Exception as e:
            logger.exception("Error in get_user_phone_status for %s: %s", user_id, e)
            raise

    def get_db_status(self) -> Dict:
        """Get database status information"""
        status = {"path": self.db_path, "exists": False, "size_bytes": None, "counts": {}}
        try:
            status["exists"] = os.path. exists(self.db_path)
            if status["exists"]: 
                status["size_bytes"] = os.path.getsize(self.db_path)
        except Exception: 
            logger.exception("Error reading DB file info")

        try:
            conn = self.get_connection()
            cur = conn.cursor()
            for table in ("users", "forwarding_tasks", "allowed_users"):
                try:
                    cur.execute(f"SELECT COUNT(*) as c FROM {table}")
                    crow = cur.fetchone()
                    status["counts"][table] = int(crow["c"]) if crow else 0
                except Exception: 
                    status["counts"][table] = None
        except Exception:
            logger.exception("Error querying DB status")

        return status

    def vacuum_database(self):
        """Optimize database size by removing unused space"""
        conn = self.get_connection()
        try:
            conn.execute("VACUUM;")
            conn.commit()
            logger.info("Database vacuumed successfully")
        except Exception as e:
            logger.exception("Error vacuuming database: %s", e)

    def __del__(self):
        try:
            self.close_connection()
        except Exception:
            pass
