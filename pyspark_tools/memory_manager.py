"""Memory manager for storing conversion history and context."""

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class BatchJob:
    """Represents a batch processing job."""

    id: Optional[int]
    job_name: str
    input_files: str  # JSON string of file paths
    output_directory: str
    status: str
    created_at: Optional[str] = None


@dataclass
class DuplicatePattern:
    """Represents a duplicate code pattern."""

    id: Optional[int]
    pattern_hash: str
    pattern_description: str
    code_template: str
    usage_count: int
    created_at: Optional[str] = None


@dataclass
class PerformanceMetric:
    """Represents a performance metric."""

    id: Optional[int]
    conversion_id: Optional[int]
    metric_type: str
    metric_value: float
    optimization_applied: str
    created_at: Optional[str] = None


@dataclass
class DocumentationCache:
    """Represents cached documentation."""

    id: Optional[int]
    doc_type: str
    doc_key: str
    content: str
    cached_at: Optional[str] = None
    expires_at: Optional[str] = None


class MemoryManager:
    """SQLite-based memory manager for storing conversion history and context."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.getenv('PYSPARK_TOOLS_DB_PATH', 
                               os.path.expanduser('~/.cache/mcp/memory.sqlite'))
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        # Ensure database is migrated to latest schema
        self.migrate_database()

    def _init_db(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            # Original tables
            conn.execute(
                """
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
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS context (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE,
                    value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # New enhanced tables
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT,
                    input_files TEXT,
                    output_directory TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS duplicate_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_hash TEXT UNIQUE,
                    pattern_description TEXT,
                    code_template TEXT,
                    usage_count INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversion_id INTEGER,
                    metric_type TEXT,
                    metric_value REAL,
                    optimization_applied TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (conversion_id) REFERENCES conversions (id)
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documentation_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_type TEXT,
                    doc_key TEXT,
                    content TEXT,
                    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """
            )

            # Original indexes
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sql_hash ON conversions(sql_hash)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_context_key ON context(key)
            """
            )

            # New indexes for enhanced functionality
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_jobs(status)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pattern_hash ON duplicate_patterns(pattern_hash)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metric_type ON performance_metrics(metric_type)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_doc_cache_key ON documentation_cache(doc_type, doc_key)
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_doc_expires ON documentation_cache(expires_at)
            """
            )

    def migrate_database(self) -> bool:
        """Migrate existing database to new schema version."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if migration is needed by looking for new tables
                cursor = conn.execute(
                    """
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name IN ('batch_jobs', 'duplicate_patterns', 'performance_metrics', 'documentation_cache')
                """
                )
                existing_tables = [row[0] for row in cursor.fetchall()]

                # Check if dialect column exists in conversions table
                cursor = conn.execute("PRAGMA table_info(conversions)")
                columns = [row[1] for row in cursor.fetchall()]
                has_dialect_column = "dialect" in columns

                # Add dialect column if missing
                if not has_dialect_column:
                    conn.execute(
                        "ALTER TABLE conversions ADD COLUMN dialect TEXT DEFAULT 'spark'"
                    )

                if len(existing_tables) == 4 and has_dialect_column:
                    return True  # Already migrated

                # Create missing tables
                if "batch_jobs" not in existing_tables:
                    conn.execute(
                        """
                        CREATE TABLE batch_jobs (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            job_name TEXT,
                            input_files TEXT,
                            output_directory TEXT,
                            status TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    )
                    conn.execute("CREATE INDEX idx_batch_status ON batch_jobs(status)")

                if "duplicate_patterns" not in existing_tables:
                    conn.execute(
                        """
                        CREATE TABLE duplicate_patterns (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            pattern_hash TEXT UNIQUE,
                            pattern_description TEXT,
                            code_template TEXT,
                            usage_count INTEGER DEFAULT 1,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    )
                    conn.execute(
                        "CREATE INDEX idx_pattern_hash ON duplicate_patterns(pattern_hash)"
                    )

                if "performance_metrics" not in existing_tables:
                    conn.execute(
                        """
                        CREATE TABLE performance_metrics (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            conversion_id INTEGER,
                            metric_type TEXT,
                            metric_value REAL,
                            optimization_applied TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (conversion_id) REFERENCES conversions (id)
                        )
                    """
                    )
                    conn.execute(
                        "CREATE INDEX idx_metric_type ON performance_metrics(metric_type)"
                    )

                if "documentation_cache" not in existing_tables:
                    conn.execute(
                        """
                        CREATE TABLE documentation_cache (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            doc_type TEXT,
                            doc_key TEXT,
                            content TEXT,
                            cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            expires_at TIMESTAMP
                        )
                    """
                    )
                    conn.execute(
                        "CREATE INDEX idx_doc_cache_key ON documentation_cache(doc_type, doc_key)"
                    )
                    conn.execute(
                        "CREATE INDEX idx_doc_expires ON documentation_cache(expires_at)"
                    )

                return True
        except Exception as e:
            print(f"Database migration failed: {e}")
            return False

    def _hash_sql(self, sql: str) -> str:
        """Generate a hash for SQL query for deduplication."""
        return hashlib.md5(sql.strip().lower().encode()).hexdigest()

    def store_conversion(
        self,
        sql_query: str,
        pyspark_code: str,
        optimization_notes: str = "",
        review_notes: str = "",
        dialect: str = "spark",
    ) -> str:
        """Store a SQL to PySpark conversion."""
        sql_hash = self._hash_sql(sql_query)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO conversions 
                (sql_hash, sql_query, pyspark_code, optimization_notes, review_notes, dialect, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (
                    sql_hash,
                    sql_query,
                    pyspark_code,
                    optimization_notes,
                    review_notes,
                    dialect,
                ),
            )

        return sql_hash

    def get_conversion(self, sql_query: str) -> Optional[Dict[str, Any]]:
        """Retrieve a stored conversion by SQL query."""
        sql_hash = self._hash_sql(sql_query)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM conversions WHERE sql_hash = ?
            """,
                (sql_hash,),
            )

            row = cursor.fetchone()
            if row:
                return dict(row)

        return None

    def get_recent_conversions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent conversions for context."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM conversions 
                ORDER BY updated_at DESC 
                LIMIT ?
            """,
                (limit,),
            )

            return [dict(row) for row in cursor.fetchall()]

    def store_context(self, key: str, value: Any):
        """Store context information."""
        value_str = json.dumps(value) if not isinstance(value, str) else value

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO context 
                (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
                (key, value_str),
            )

    def get_context(self, key: str) -> Optional[Any]:
        """Retrieve context information."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT value FROM context WHERE key = ?
            """,
                (key,),
            )

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
            cursor = conn.execute(
                """
                SELECT * FROM conversions 
                WHERE sql_query LIKE ? OR pyspark_code LIKE ?
                ORDER BY updated_at DESC 
                LIMIT ?
            """,
                (f"%{query}%", f"%{query}%", limit),
            )

            return [dict(row) for row in cursor.fetchall()]

    # Batch Job Management Methods
    def create_batch_job(
        self, job_name: str, input_files: List[str], output_directory: str
    ) -> int:
        """Create a new batch processing job."""
        input_files_json = json.dumps(input_files)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO batch_jobs (job_name, input_files, output_directory, status)
                VALUES (?, ?, ?, 'pending')
            """,
                (job_name, input_files_json, output_directory),
            )

            return cursor.lastrowid

    def update_batch_job_status(self, job_id: int, status: str) -> bool:
        """Update the status of a batch job."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE batch_jobs SET status = ? WHERE id = ?
            """,
                (status, job_id),
            )

            return cursor.rowcount > 0

    def get_batch_job(self, job_id: int) -> Optional[BatchJob]:
        """Retrieve a batch job by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM batch_jobs WHERE id = ?
            """,
                (job_id,),
            )

            row = cursor.fetchone()
            if row:
                return BatchJob(
                    id=row["id"],
                    job_name=row["job_name"],
                    input_files=row["input_files"],
                    output_directory=row["output_directory"],
                    status=row["status"],
                    created_at=row["created_at"],
                )

        return None

    def get_batch_jobs_by_status(self, status: str) -> List[BatchJob]:
        """Get all batch jobs with a specific status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM batch_jobs WHERE status = ? ORDER BY created_at DESC
            """,
                (status,),
            )

            return [
                BatchJob(
                    id=row["id"],
                    job_name=row["job_name"],
                    input_files=row["input_files"],
                    output_directory=row["output_directory"],
                    status=row["status"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    def get_recent_batch_jobs(self, limit: int = 10) -> List[BatchJob]:
        """Get recent batch jobs."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM batch_jobs ORDER BY created_at DESC LIMIT ?
            """,
                (limit,),
            )

            return [
                BatchJob(
                    id=row["id"],
                    job_name=row["job_name"],
                    input_files=row["input_files"],
                    output_directory=row["output_directory"],
                    status=row["status"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    # Duplicate Pattern Management Methods
    def store_duplicate_pattern(
        self, pattern_hash: str, pattern_description: str, code_template: str
    ) -> int:
        """Store or update a duplicate code pattern."""
        with sqlite3.connect(self.db_path) as conn:
            # Try to update existing pattern first
            cursor = conn.execute(
                """
                UPDATE duplicate_patterns 
                SET usage_count = usage_count + 1, pattern_description = ?, code_template = ?
                WHERE pattern_hash = ?
            """,
                (pattern_description, code_template, pattern_hash),
            )

            if cursor.rowcount == 0:
                # Insert new pattern if it doesn't exist
                cursor = conn.execute(
                    """
                    INSERT INTO duplicate_patterns (pattern_hash, pattern_description, code_template, usage_count)
                    VALUES (?, ?, ?, 1)
                """,
                    (pattern_hash, pattern_description, code_template),
                )

                return cursor.lastrowid
            else:
                # Return the ID of the updated pattern
                cursor = conn.execute(
                    """
                    SELECT id FROM duplicate_patterns WHERE pattern_hash = ?
                """,
                    (pattern_hash,),
                )
                return cursor.fetchone()[0]

    def get_duplicate_pattern(self, pattern_hash: str) -> Optional[DuplicatePattern]:
        """Retrieve a duplicate pattern by hash."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM duplicate_patterns WHERE pattern_hash = ?
            """,
                (pattern_hash,),
            )

            row = cursor.fetchone()
            if row:
                return DuplicatePattern(
                    id=row["id"],
                    pattern_hash=row["pattern_hash"],
                    pattern_description=row["pattern_description"],
                    code_template=row["code_template"],
                    usage_count=row["usage_count"],
                    created_at=row["created_at"],
                )

        return None

    def get_common_patterns(
        self, min_usage: int = 2, limit: int = 20
    ) -> List[DuplicatePattern]:
        """Get commonly used patterns above a usage threshold."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM duplicate_patterns 
                WHERE usage_count >= ? 
                ORDER BY usage_count DESC, created_at DESC 
                LIMIT ?
            """,
                (min_usage, limit),
            )

            return [
                DuplicatePattern(
                    id=row["id"],
                    pattern_hash=row["pattern_hash"],
                    pattern_description=row["pattern_description"],
                    code_template=row["code_template"],
                    usage_count=row["usage_count"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    def search_patterns(self, query: str, limit: int = 10) -> List[DuplicatePattern]:
        """Search patterns by description or code template."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM duplicate_patterns 
                WHERE pattern_description LIKE ? OR code_template LIKE ?
                ORDER BY usage_count DESC, created_at DESC 
                LIMIT ?
            """,
                (f"%{query}%", f"%{query}%", limit),
            )

            return [
                DuplicatePattern(
                    id=row["id"],
                    pattern_hash=row["pattern_hash"],
                    pattern_description=row["pattern_description"],
                    code_template=row["code_template"],
                    usage_count=row["usage_count"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    # Performance Metrics Management Methods
    def store_performance_metric(
        self,
        conversion_id: Optional[int],
        metric_type: str,
        metric_value: float,
        optimization_applied: str,
    ) -> int:
        """Store a performance metric."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO performance_metrics (conversion_id, metric_type, metric_value, optimization_applied)
                VALUES (?, ?, ?, ?)
            """,
                (conversion_id, metric_type, metric_value, optimization_applied),
            )

            return cursor.lastrowid

    def get_performance_metrics(self, conversion_id: int) -> List[PerformanceMetric]:
        """Get all performance metrics for a specific conversion."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM performance_metrics 
                WHERE conversion_id = ? 
                ORDER BY created_at DESC
            """,
                (conversion_id,),
            )

            return [
                PerformanceMetric(
                    id=row["id"],
                    conversion_id=row["conversion_id"],
                    metric_type=row["metric_type"],
                    metric_value=row["metric_value"],
                    optimization_applied=row["optimization_applied"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    def get_metrics_by_type(
        self, metric_type: str, limit: int = 50
    ) -> List[PerformanceMetric]:
        """Get performance metrics by type."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM performance_metrics 
                WHERE metric_type = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            """,
                (metric_type, limit),
            )

            return [
                PerformanceMetric(
                    id=row["id"],
                    conversion_id=row["conversion_id"],
                    metric_type=row["metric_type"],
                    metric_value=row["metric_value"],
                    optimization_applied=row["optimization_applied"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    def get_optimization_effectiveness(
        self, optimization_type: str
    ) -> Dict[str, float]:
        """Get effectiveness statistics for a specific optimization type."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT 
                    AVG(metric_value) as avg_improvement,
                    MIN(metric_value) as min_improvement,
                    MAX(metric_value) as max_improvement,
                    COUNT(*) as usage_count
                FROM performance_metrics 
                WHERE optimization_applied LIKE ?
            """,
                (f"%{optimization_type}%",),
            )

            row = cursor.fetchone()
            if row and row[0] is not None:
                return {
                    "avg_improvement": row[0],
                    "min_improvement": row[1],
                    "max_improvement": row[2],
                    "usage_count": row[3],
                }

            return {
                "avg_improvement": 0.0,
                "min_improvement": 0.0,
                "max_improvement": 0.0,
                "usage_count": 0,
            }

    def get_recent_metrics(self, limit: int = 20) -> List[PerformanceMetric]:
        """Get recent performance metrics across all conversions."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM performance_metrics 
                ORDER BY created_at DESC 
                LIMIT ?
            """,
                (limit,),
            )

            return [
                PerformanceMetric(
                    id=row["id"],
                    conversion_id=row["conversion_id"],
                    metric_type=row["metric_type"],
                    metric_value=row["metric_value"],
                    optimization_applied=row["optimization_applied"],
                    created_at=row["created_at"],
                )
                for row in cursor.fetchall()
            ]

    # Documentation Cache Management Methods
    def cache_documentation(
        self, doc_type: str, doc_key: str, content: str, ttl_hours: int = 24
    ) -> int:
        """Cache documentation with TTL."""
        expires_at = datetime.now() + timedelta(hours=ttl_hours)

        with sqlite3.connect(self.db_path) as conn:
            # Remove existing cache entry if it exists
            conn.execute(
                """
                DELETE FROM documentation_cache WHERE doc_type = ? AND doc_key = ?
            """,
                (doc_type, doc_key),
            )

            # Insert new cache entry
            cursor = conn.execute(
                """
                INSERT INTO documentation_cache (doc_type, doc_key, content, expires_at)
                VALUES (?, ?, ?, ?)
            """,
                (doc_type, doc_key, content, expires_at.isoformat()),
            )

            return cursor.lastrowid

    def get_cached_documentation(
        self, doc_type: str, doc_key: str
    ) -> Optional[DocumentationCache]:
        """Retrieve cached documentation if not expired."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM documentation_cache 
                WHERE doc_type = ? AND doc_key = ? AND expires_at > datetime('now')
            """,
                (doc_type, doc_key),
            )

            row = cursor.fetchone()
            if row:
                return DocumentationCache(
                    id=row["id"],
                    doc_type=row["doc_type"],
                    doc_key=row["doc_key"],
                    content=row["content"],
                    cached_at=row["cached_at"],
                    expires_at=row["expires_at"],
                )

        return None

    def cleanup_expired_documentation(self) -> int:
        """Remove expired documentation cache entries."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM documentation_cache WHERE expires_at <= datetime('now')
            """
            )

            return cursor.rowcount

    def get_cached_documentation_by_type(
        self, doc_type: str
    ) -> List[DocumentationCache]:
        """Get all cached documentation of a specific type (including expired)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM documentation_cache 
                WHERE doc_type = ? 
                ORDER BY cached_at DESC
            """,
                (doc_type,),
            )

            return [
                DocumentationCache(
                    id=row["id"],
                    doc_type=row["doc_type"],
                    doc_key=row["doc_key"],
                    content=row["content"],
                    cached_at=row["cached_at"],
                    expires_at=row["expires_at"],
                )
                for row in cursor.fetchall()
            ]

    def update_documentation_cache_ttl(
        self, doc_type: str, doc_key: str, ttl_hours: int
    ) -> bool:
        """Update the TTL for a cached documentation entry."""
        expires_at = datetime.now() + timedelta(hours=ttl_hours)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE documentation_cache 
                SET expires_at = ? 
                WHERE doc_type = ? AND doc_key = ?
            """,
                (expires_at.isoformat(), doc_type, doc_key),
            )

            return cursor.rowcount > 0

    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get statistics about the documentation cache."""
        with sqlite3.connect(self.db_path) as conn:
            # Total cache entries
            cursor = conn.execute("SELECT COUNT(*) FROM documentation_cache")
            total_entries = cursor.fetchone()[0]

            # Active (non-expired) entries
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM documentation_cache 
                WHERE expires_at > datetime('now')
            """
            )
            active_entries = cursor.fetchone()[0]

            # Entries by type
            cursor = conn.execute(
                """
                SELECT doc_type, COUNT(*) as count 
                FROM documentation_cache 
                GROUP BY doc_type
            """
            )
            entries_by_type = dict(cursor.fetchall())

            return {
                "total_entries": total_entries,
                "active_entries": active_entries,
                "expired_entries": total_entries - active_entries,
                "entries_by_type": entries_by_type,
            }
