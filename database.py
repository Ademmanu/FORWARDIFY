import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import logging
from queue import Queue
import time

logger = logging.getLogger("database")

"""
Optimized SQLite helper for FORWARDIFY with connection pooling for Render free tier
"""

class ConnectionPool:
    """SQLite connection pool for memory efficiency on Render"""
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.max_connections = max_connections
        self.pool = Queue(maxsize=max_connections)
        self.active_connections = 0
        self.lock = threading.Lock()
        
        # Initialize connections
        for _ in range(2):  # Start with 2 connections
            conn = self._create_connection()
            self.pool.put(conn)
            self.active_connections += 1
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path, 
            timeout=30, 
            check_same_thread=False,
            isolation_level=None  # Auto-commit mode for better performance
        )
        conn.row_factory = sqlite3.Row
        
        # Optimized pragmas for Render free tier (512MB RAM)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-2000;")  # ~2MB cache
            conn.execute("PRAGMA mmap_size=134217728;")  # 128MB mmap
            conn.execute("PRAGMA page_size=4096;")
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL;")
        except Exception:
            pass
            
        return conn
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from pool or create new one if under limit"""
        try:
            # Try to get from pool with timeout
            conn = self.pool.get(timeout=5)
            return conn
        except Exception:
            with self.lock:
                if self.active_connections < self.max_connections:
                    conn = self._create_connection()
                    self.active_connections += 1
                    return conn
                else:
                    # Wait and retry
                    time.sleep(0.1)
                    return self.pool.get(timeout=10)
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool"""
        try:
            self.pool.put(conn, timeout=5)
        except Exception:
            try:
                conn.close()
            except:
                pass
            with self.lock:
                self.active_connections -= 1
    
    def close_all(self):
        """Close all connections in pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except Exception:
                pass
        self.active_connections = 0


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections=5)
        self.init_lock = threading.Lock()
        self._init_db()
        
    def _execute_with_conn(self, func):
        """Execute function with connection from pool"""
        conn = None
        try:
            conn = self.pool.get_connection()
            result = func(conn)
            self.pool.return_connection(conn)
            return result
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
                with self.pool.lock:
                    self.pool.active_connections -= 1
            raise e
    
    def _init_db(self):
        """Initialize database schema"""
        def init_tables(conn):
            cur = conn.cursor()
            
            # Users table with optimized indexes
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """)
            
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_logged_in ON users(is_logged_in)")
            
            # Forwarding tasks with optimized structure
            cur.execute("""
                CREATE TABLE IF NOT EXISTS forwarding_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    label TEXT,
                    source_ids TEXT,
                    target_ids TEXT,
                    filters TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    last_used_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, label)
                )
            """)
            
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_active ON forwarding_tasks(user_id, is_active)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON forwarding_tasks(is_active)")
            
            # Allowed users
            cur.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            
            # Message queue for better memory management
            cur.execute("""
                CREATE TABLE IF NOT EXISTS message_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    target_id INTEGER,
                    message_text TEXT,
                    task_filters TEXT,
                    forward_tag INTEGER DEFAULT 0,
                    source_chat_id INTEGER,
                    message_id INTEGER,
                    status INTEGER DEFAULT 0, -- 0=pending, 1=processing, 2=completed, 3=failed
                    attempts INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
            
            cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_pending ON message_queue(status, created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_user ON message_queue(user_id, status)")
            
            conn.commit()
        
        with self.init_lock:
            self._execute_with_conn(init_tables)
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        def query(conn):
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return dict(row)
        
        return self._execute_with_conn(query)
    
    def save_user(self, user_id: int, phone: Optional[str] = None, 
                 name: Optional[str] = None, session_data: Optional[str] = None, 
                 is_logged_in: bool = False):
        def update(conn):
            cur = conn.cursor()
            cur.execute(
                """
                INSERT OR REPLACE INTO users 
                (user_id, phone, name, session_data, is_logged_in, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, phone, name, session_data, 1 if is_logged_in else 0, 
                 datetime.now().isoformat())
            )
            conn.commit()
        
        self._execute_with_conn(update)
    
    def add_forwarding_task(self, user_id: int, label: str, 
                           source_ids: List[int], target_ids: List[int], 
                           filters: Optional[Dict[str, Any]] = None) -> bool:
        def insert(conn):
            cur = conn.cursor()
            if filters is None:
                filters = {
                    "filters": {
                        "raw_text": False,
                        "numbers_only": False,
                        "alphabets_only": False,
                        "removed_alphabetic": False,
                        "removed_numeric": False,
                        "prefix": "",
                        "suffix": ""
                    },
                    "outgoing": True,
                    "forward_tag": False,
                    "control": True
                }
            
            try:
                cur.execute(
                    """
                    INSERT INTO forwarding_tasks 
                    (user_id, label, source_ids, target_ids, filters, last_used_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, label, json.dumps(source_ids), json.dumps(target_ids), 
                     json.dumps(filters), datetime.now().isoformat())
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
        
        return self._execute_with_conn(insert)
    
    def update_task_filters(self, user_id: int, label: str, filters: Dict[str, Any]) -> bool:
        def update(conn):
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE forwarding_tasks 
                SET filters = ?, updated_at = ?, last_used_at = ?
                WHERE user_id = ? AND label = ?
                """,
                (json.dumps(filters), datetime.now().isoformat(), 
                 datetime.now().isoformat(), user_id, label)
            )
            updated = cur.rowcount > 0
            conn.commit()
            return updated
        
        return self._execute_with_conn(update)
    
    def remove_forwarding_task(self, user_id: int, label: str) -> bool:
        def delete(conn):
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM forwarding_tasks WHERE user_id = ? AND label = ?", 
                (user_id, label)
            )
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        
        return self._execute_with_conn(delete)
    
    def get_user_tasks(self, user_id: int) -> List[Dict]:
        def query(conn):
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, label, source_ids, target_ids, filters, is_active, created_at
                FROM forwarding_tasks
                WHERE user_id = ? AND is_active = 1
                ORDER BY last_used_at DESC
                LIMIT 100
                """,
                (user_id,)
            )
            
            tasks = []
            for row in cur.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["source_ids"] = json.loads(row_dict["source_ids"]) if row_dict["source_ids"] else []
                    row_dict["target_ids"] = json.loads(row_dict["target_ids"]) if row_dict["target_ids"] else []
                    row_dict["filters"] = json.loads(row_dict["filters"]) if row_dict["filters"] else {}
                except (json.JSONDecodeError, TypeError):
                    row_dict["source_ids"] = []
                    row_dict["target_ids"] = []
                    row_dict["filters"] = {}
                
                tasks.append(row_dict)
            return tasks
        
        return self._execute_with_conn(query)
    
    def get_all_active_tasks(self) -> List[Dict]:
        def query(conn):
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, id, label, source_ids, target_ids, filters
                FROM forwarding_tasks
                WHERE is_active = 1
                ORDER BY last_used_at DESC
                LIMIT 200
                """
            )
            
            tasks = []
            for row in cur.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["source_ids"] = json.loads(row_dict["source_ids"]) if row_dict["source_ids"] else []
                    row_dict["target_ids"] = json.loads(row_dict["target_ids"]) if row_dict["target_ids"] else []
                    row_dict["filters"] = json.loads(row_dict["filters"]) if row_dict["filters"] else {}
                except (json.JSONDecodeError, TypeError):
                    row_dict["source_ids"] = []
                    row_dict["target_ids"] = []
                    row_dict["filters"] = {}
                
                tasks.append(row_dict)
            return tasks
        
        return self._execute_with_conn(query)
    
    def is_user_allowed(self, user_id: int) -> bool:
        def query(conn):
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
            return cur.fetchone() is not None
        
        return self._execute_with_conn(query)
    
    def is_user_admin(self, user_id: int) -> bool:
        def query(conn):
            cur = conn.cursor()
            cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            return row is not None and int(row["is_admin"]) == 1
        
        return self._execute_with_conn(query)
    
    def add_allowed_user(self, user_id: int, username: Optional[str] = None, 
                        is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        def insert(conn):
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO allowed_users 
                    (user_id, username, is_admin, added_by)
                    VALUES (?, ?, ?, ?)
                    """,
                    (user_id, username, 1 if is_admin else 0, added_by)
                )
                conn.commit()
                return cur.rowcount > 0
            except sqlite3.IntegrityError:
                return False
        
        return self._execute_with_conn(insert)
    
    def remove_allowed_user(self, user_id: int) -> bool:
        def delete(conn):
            cur = conn.cursor()
            cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        
        return self._execute_with_conn(delete)
    
    def get_all_allowed_users(self) -> List[Dict]:
        def query(conn):
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, username, is_admin, added_by, created_at
                FROM allowed_users
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
            return [dict(row) for row in cur.fetchall()]
        
        return self._execute_with_conn(query)
    
    def add_to_message_queue(self, user_id: int, target_id: int, message_text: str,
                            task_filters: Dict, forward_tag: bool = False,
                            source_chat_id: Optional[int] = None,
                            message_id: Optional[int] = None) -> int:
        """Add message to persistent queue (fallback when memory queue is full)"""
        def insert(conn):
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO message_queue 
                (user_id, target_id, message_text, task_filters, forward_tag, 
                 source_chat_id, message_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, target_id, message_text, json.dumps(task_filters),
                 1 if forward_tag else 0, source_chat_id, message_id,
                 datetime.now().isoformat())
            )
            conn.commit()
            return cur.lastrowid
        
        return self._execute_with_conn(insert)
    
    def get_pending_messages(self, limit: int = 50) -> List[Dict]:
        """Get pending messages from queue for processing"""
        def query(conn):
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_id, target_id, message_text, task_filters, 
                       forward_tag, source_chat_id, message_id
                FROM message_queue
                WHERE status = 0 AND attempts < 3
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,)
            )
            
            messages = []
            for row in cur.fetchall():
                row_dict = dict(row)
                try:
                    row_dict["task_filters"] = json.loads(row_dict["task_filters"]) if row_dict["task_filters"] else {}
                except (json.JSONDecodeError, TypeError):
                    row_dict["task_filters"] = {}
                messages.append(row_dict)
            return messages
        
        return self._execute_with_conn(query)
    
    def update_message_status(self, message_id: int, status: int, increment_attempts: bool = False):
        """Update message status in queue"""
        def update(conn):
            cur = conn.cursor()
            if increment_attempts:
                cur.execute(
                    """
                    UPDATE message_queue 
                    SET status = ?, attempts = attempts + 1
                    WHERE id = ?
                    """,
                    (status, message_id)
                )
            else:
                cur.execute(
                    "UPDATE message_queue SET status = ? WHERE id = ?",
                    (status, message_id)
                )
            conn.commit()
        
        self._execute_with_conn(update)
    
    def cleanup_old_messages(self, days: int = 7):
        """Clean up old completed/failed messages"""
        def cleanup(conn):
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM message_queue 
                WHERE created_at < datetime('now', ?) 
                AND status IN (2, 3)
                """,
                (f'-{days} days',)
            )
            conn.commit()
            return cur.rowcount
        
        return self._execute_with_conn(cleanup)
    
    def get_db_status(self) -> Dict:
        """Get database status and statistics"""
        def query(conn):
            cur = conn.cursor()
            
            status = {
                "path": self.db_path,
                "exists": os.path.exists(self.db_path),
                "pool_active": self.pool.active_connections,
                "pool_queue": self.pool.pool.qsize() if hasattr(self.pool.pool, 'qsize') else 0
            }
            
            if status["exists"]:
                try:
                    status["size_bytes"] = os.path.getsize(self.db_path)
                except Exception:
                    status["size_bytes"] = None
            
            try:
                cur.execute("PRAGMA user_version;")
                row = cur.fetchone()
                status["user_version"] = row[0] if row else None
                
                # Get table counts
                tables = ["users", "forwarding_tasks", "allowed_users", "message_queue"]
                status["counts"] = {}
                for table in tables:
                    try:
                        cur.execute(f"SELECT COUNT(1) as c FROM {table}")
                        row = cur.fetchone()
                        status["counts"][table] = row[0] if row else 0
                    except Exception:
                        status["counts"][table] = None
                
                # Get database stats
                cur.execute("PRAGMA cache_size;")
                status["cache_size"] = cur.fetchone()[0]
                
                cur.execute("PRAGMA page_count;")
                status["page_count"] = cur.fetchone()[0]
                
                cur.execute("PRAGMA page_size;")
                status["page_size"] = cur.fetchone()[0]
                
            except Exception:
                pass
            
            return status
        
        try:
            return self._execute_with_conn(query)
        except Exception:
            return {"error": "Failed to get DB status"}
    
    def vacuum_if_needed(self):
        """Run VACUUM if database is fragmented (run sparingly)"""
        def vacuum(conn):
            cur = conn.cursor()
            # Check fragmentation
            cur.execute("PRAGMA freelist_count;")
            freelist = cur.fetchone()[0]
            cur.execute("PRAGMA page_count;")
            total_pages = cur.fetchone()[0]
            
            if total_pages > 0 and freelist / total_pages > 0.3:  # 30% fragmentation
                logger.info(f"Running VACUUM: {freelist}/{total_pages} pages free")
                cur.execute("VACUUM;")
                conn.commit()
                return True
            return False
        
        try:
            return self._execute_with_conn(vacuum)
        except Exception:
            return False
    
    def close(self):
        """Close all database connections"""
        self.pool.close_all()
    
    def __del__(self):
        self.close()


# Global database instance
db = Database()
