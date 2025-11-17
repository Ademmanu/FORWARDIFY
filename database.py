import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional

class Database:
    def __init__(self, db_path='bot_data.db'):
        self.db_path = db_path
        self.init_db()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                phone TEXT,
                name TEXT,
                session_data TEXT,
                is_logged_in INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarding_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                label TEXT,
                source_ids TEXT,
                target_ids TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(user_id, label)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                is_admin INTEGER DEFAULT 0,
                added_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'user_id': row[0],
                'phone': row[1],
                'name': row[2],
                'session_data': row[3],
                'is_logged_in': row[4],
                'created_at': row[5],
                'updated_at': row[6]
            }
        return None
    
    def save_user(self, user_id: int, phone: Optional[str] = None, name: Optional[str] = None, 
                  session_data: Optional[str] = None, is_logged_in: bool = False):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        existing_user = self.get_user(user_id)
        
        if existing_user:
            updates = []
            params = []
            
            if phone is not None:
                updates.append('phone = ?')
                params.append(phone)
            if name is not None:
                updates.append('name = ?')
                params.append(name)
            if session_data is not None:
                updates.append('session_data = ?')
                params.append(session_data)
            
            updates.append('is_logged_in = ?')
            params.append(1 if is_logged_in else 0)
            
            updates.append('updated_at = ?')
            params.append(datetime.now().isoformat())
            
            params.append(user_id)
            
            query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            cursor.execute(query, params)
        else:
            cursor.execute('''
                INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, phone, name, session_data, 1 if is_logged_in else 0))
        
        conn.commit()
        conn.close()
    
    def add_forwarding_task(self, user_id: int, label: str, 
                           source_ids: List[int], target_ids: List[int]) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO forwarding_tasks (user_id, label, source_ids, target_ids)
                VALUES (?, ?, ?, ?)
            ''', (user_id, label, json.dumps(source_ids), json.dumps(target_ids)))
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False
    
    def remove_forwarding_task(self, user_id: int, label: str) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM forwarding_tasks 
            WHERE user_id = ? AND label = ?
        ''', (user_id, label))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    
    def get_user_tasks(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, label, source_ids, target_ids, is_active, created_at
            FROM forwarding_tasks 
            WHERE user_id = ? AND is_active = 1
            ORDER BY created_at DESC
        ''', (user_id,))
        
        tasks = []
        for row in cursor.fetchall():
            tasks.append({
                'id': row[0],
                'label': row[1],
                'source_ids': json.loads(row[2]),
                'target_ids': json.loads(row[3]),
                'is_active': row[4],
                'created_at': row[5]
            })
        
        conn.close()
        return tasks
    
    def get_all_active_tasks(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, id, label, source_ids, target_ids
            FROM forwarding_tasks 
            WHERE is_active = 1
        ''')
        
        tasks = []
        for row in cursor.fetchall():
            tasks.append({
                'user_id': row[0],
                'id': row[1],
                'label': row[2],
                'source_ids': json.loads(row[3]),
                'target_ids': json.loads(row[4])
            })
        
        conn.close()
        return tasks
    
    def is_user_allowed(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM allowed_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def is_user_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT is_admin FROM allowed_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None and result[0] == 1
    
    def add_allowed_user(self, user_id: int, username: Optional[str] = None, is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO allowed_users (user_id, username, is_admin, added_by)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, 1 if is_admin else 0, added_by))
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            conn.close()
            return False
    
    def remove_allowed_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM allowed_users WHERE user_id = ?', (user_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    
    def get_all_allowed_users(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT user_id, username, is_admin, added_by, created_at
            FROM allowed_users
            ORDER BY created_at DESC
        ''')
        
        users = []
        for row in cursor.fetchall():
            users.append({
                'user_id': row[0],
                'username': row[1],
                'is_admin': row[2],
                'added_by': row[3],
                'created_at': row[4]
            })
        
        conn.close()
        return users
