"""Memory manager for storing conversion history and context."""

import sqlite3
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


class MemoryManager:
    """SQLite-based memory manager for storing conversion history and context."""
    
    def __init__(self, db_path: str = "/home/dev/.cache/mcp/memory.sqlite"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sql_hash TEXT UNIQUE,
                    sql_query TEXT,
                    pyspark_code TEXT,
                    optimization_notes TEXT,
                    review_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS context (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE,
                    value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sql_hash ON conversions(sql_hash)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_context_key ON context(key)
            """)
    
    def _hash_sql(self, sql: str) -> str:
        """Generate a hash for SQL query for deduplication."""
        return hashlib.md5(sql.strip().lower().encode()).hexdigest()
    
    def store_conversion(
        self, 
        sql_query: str, 
        pyspark_code: str, 
        optimization_notes: str = "", 
        review_notes: str = ""
    ) -> str:
        """Store a SQL to PySpark conversion."""
        sql_hash = self._hash_sql(sql_query)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO conversions 
                (sql_hash, sql_query, pyspark_code, optimization_notes, review_notes, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (sql_hash, sql_query, pyspark_code, optimization_notes, review_notes))
        
        return sql_hash
    
    def get_conversion(self, sql_query: str) -> Optional[Dict[str, Any]]:
        """Retrieve a stored conversion by SQL query."""
        sql_hash = self._hash_sql(sql_query)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM conversions WHERE sql_hash = ?
            """, (sql_hash,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
        
        return None
    
    def get_recent_conversions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent conversions for context."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM conversions 
                ORDER BY updated_at DESC 
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def store_context(self, key: str, value: Any):
        """Store context information."""
        value_str = json.dumps(value) if not isinstance(value, str) else value
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO context 
                (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (key, value_str))
    
    def get_context(self, key: str) -> Optional[Any]:
        """Retrieve context information."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT value FROM context WHERE key = ?
            """, (key,))
            
            row = cursor.fetchone()
            if row:
                try:
                    return json.loads(row[0])
                except json.JSONDecodeError:
                    return row[0]
        
        return None
    
    def search_conversions(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search conversions by SQL content."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM conversions 
                WHERE sql_query LIKE ? OR pyspark_code LIKE ?
                ORDER BY updated_at DESC 
                LIMIT ?
            """, (f"%{query}%", f"%{query}%", limit))
            
            return [dict(row) for row in cursor.fetchall()]