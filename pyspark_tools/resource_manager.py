"""
Resource management infrastructure for tracking and cleaning up resources.

This module provides centralized resource management to prevent leaks in tests
and production code, particularly for database connections, file handles, and processes.
"""

import atexit
import logging
import os
import sqlite3
import subprocess
import tempfile
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


@dataclass
class ResourceInfo:
    """Information about a tracked resource."""

    resource_id: str
    resource_type: str
    description: str
    created_at: datetime = field(default_factory=datetime.now)
    cleanup_func: Optional[callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ResourceManager:
    """
    Centralized resource manager for tracking and cleaning up resources.

    Tracks database connections, file handles, processes, and temporary files
    to ensure proper cleanup and prevent resource leaks.
    """

    _instance = None
    _class_lock = threading.Lock()

    def __new__(cls):
        """Singleton pattern to ensure one resource manager per process."""
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the resource manager."""
        if hasattr(self, "_initialized"):
            return

        self._initialized = True
        self._resources: Dict[str, ResourceInfo] = {}
        self._connections: Set[sqlite3.Connection] = set()
        self._temp_files: Set[Path] = set()
        self._processes: Set[subprocess.Popen] = set()
        self._file_handles: Set[Any] = set()
        self._instance_lock = threading.Lock()

        # Register cleanup on exit
        atexit.register(self.cleanup_all)

        logger.info("ResourceManager initialized")

    def register_connection(
        self, conn: sqlite3.Connection, description: str = ""
    ) -> str:
        """
        Register a database connection for tracking and cleanup.

        Args:
            conn: SQLite connection to track
            description: Optional description of the connection

        Returns:
            Resource ID for tracking
        """
        resource_id = f"conn_{id(conn)}"

        with self._instance_lock:
            self._connections.add(conn)

            # Store connection directly since SQLite connections don't support weak references
            # Store the thread ID where the connection was created
            creation_thread_id = threading.get_ident()

            def cleanup_connection():
                try:
                    if conn and hasattr(conn, "close"):
                        current_thread_id = threading.get_ident()
                        if current_thread_id == creation_thread_id:
                            # Safe to close in the same thread
                            conn.close()
                            logger.debug(f"Closed database connection: {resource_id}")
                        else:
                            # Different thread - just log and remove from tracking
                            logger.debug(
                                f"Skipping cross-thread connection close: {resource_id}"
                            )
                except Exception as e:
                    logger.warning(f"Error closing connection {resource_id}: {e}")
                finally:
                    # Remove from tracking set
                    self._connections.discard(conn)

            resource_info = ResourceInfo(
                resource_id=resource_id,
                resource_type="database_connection",
                description=description or f"SQLite connection {resource_id}",
                cleanup_func=cleanup_connection,
                metadata={"connection_id": id(conn)},
            )

            self._resources[resource_id] = resource_info

        logger.debug(f"Registered database connection: {resource_id}")
        return resource_id

    def register_temp_file(
        self, file_path: Union[str, Path], description: str = ""
    ) -> str:
        """
        Register a temporary file for cleanup.

        Args:
            file_path: Path to temporary file
            description: Optional description of the file

        Returns:
            Resource ID for tracking
        """
        path = Path(file_path)
        resource_id = f"temp_{abs(hash(str(path)))}"

        with self._instance_lock:
            self._temp_files.add(path)

            def cleanup_temp_file():
                try:
                    if path.exists():
                        if path.is_file():
                            path.unlink()
                        elif path.is_dir():
                            import shutil

                            shutil.rmtree(path, ignore_errors=True)
                        logger.debug(f"Removed temporary file/directory: {path}")
                except Exception as e:
                    logger.warning(f"Error removing temp file {path}: {e}")

            resource_info = ResourceInfo(
                resource_id=resource_id,
                resource_type="temp_file",
                description=description or f"Temporary file {path}",
                cleanup_func=cleanup_temp_file,
                metadata={"path": str(path)},
            )

            self._resources[resource_id] = resource_info

        logger.debug(f"Registered temporary file: {path}")
        return resource_id

    def register_process(self, process: subprocess.Popen, description: str = "") -> str:
        """
        Register a subprocess for tracking and cleanup.

        Args:
            process: Subprocess to track
            description: Optional description of the process

        Returns:
            Resource ID for tracking
        """
        resource_id = f"proc_{process.pid}"

        with self._instance_lock:
            self._processes.add(process)

            # Store process directly
            def cleanup_process():
                try:
                    if process and hasattr(process, "poll") and process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=2)  # Reduced timeout
                        except subprocess.TimeoutExpired:
                            process.kill()
                            try:
                                process.wait(timeout=1)  # Final timeout
                            except subprocess.TimeoutExpired:
                                logger.warning(
                                    f"Process {resource_id} could not be killed"
                                )
                        logger.debug(f"Terminated process: {resource_id}")
                except Exception as e:
                    logger.warning(f"Error terminating process {resource_id}: {e}")
                finally:
                    # Remove from tracking set
                    self._processes.discard(process)

            resource_info = ResourceInfo(
                resource_id=resource_id,
                resource_type="process",
                description=description or f"Process {process.pid}",
                cleanup_func=cleanup_process,
                metadata={"pid": process.pid},
            )

            self._resources[resource_id] = resource_info

        logger.debug(f"Registered process: {process.pid}")
        return resource_id

    def register_file_handle(self, file_handle: Any, description: str = "") -> str:
        """
        Register a file handle for tracking and cleanup.

        Args:
            file_handle: File handle to track
            description: Optional description of the file handle

        Returns:
            Resource ID for tracking
        """
        resource_id = f"file_{id(file_handle)}"

        with self._instance_lock:
            self._file_handles.add(file_handle)

            # Store file handle directly (some file objects don't support weak references)
            def cleanup_file_handle():
                try:
                    if (
                        file_handle
                        and hasattr(file_handle, "close")
                        and hasattr(file_handle, "closed")
                    ):
                        if not file_handle.closed:
                            file_handle.close()
                            logger.debug(f"Closed file handle: {resource_id}")
                except Exception as e:
                    logger.warning(f"Error closing file handle {resource_id}: {e}")
                finally:
                    # Remove from tracking set
                    self._file_handles.discard(file_handle)

            resource_info = ResourceInfo(
                resource_id=resource_id,
                resource_type="file_handle",
                description=description or f"File handle {resource_id}",
                cleanup_func=cleanup_file_handle,
                metadata={"handle_id": id(file_handle)},
            )

            self._resources[resource_id] = resource_info

        logger.debug(f"Registered file handle: {resource_id}")
        return resource_id

    def unregister_resource(self, resource_id: str) -> bool:
        """
        Unregister a resource (usually called after manual cleanup).

        Args:
            resource_id: ID of resource to unregister

        Returns:
            True if resource was found and unregistered
        """
        with self._instance_lock:
            if resource_id in self._resources:
                resource_info = self._resources.pop(resource_id)

                # Remove from specific collections
                if resource_info.resource_type == "database_connection":
                    # Connection might already be closed, so we can't remove by reference
                    pass
                elif resource_info.resource_type == "temp_file":
                    path = Path(resource_info.metadata["path"])
                    self._temp_files.discard(path)
                elif resource_info.resource_type == "process":
                    # Process might already be terminated
                    pass
                elif resource_info.resource_type == "file_handle":
                    # Handle might already be closed
                    pass

                logger.debug(f"Unregistered resource: {resource_id}")
                return True

        return False

    def cleanup_resource(self, resource_id: str) -> bool:
        """
        Clean up a specific resource.

        Args:
            resource_id: ID of resource to clean up

        Returns:
            True if resource was found and cleaned up
        """
        resource_info = None

        # Get resource info while holding lock
        with self._instance_lock:
            if resource_id in self._resources:
                resource_info = self._resources[resource_id]
            else:
                return False

        # Execute cleanup function outside the lock
        if resource_info and resource_info.cleanup_func:
            try:
                resource_info.cleanup_func()
                logger.debug(f"Cleaned up resource: {resource_id}")
            except Exception as e:
                logger.error(f"Error cleaning up resource {resource_id}: {e}")

        # Remove from tracking (this will acquire the lock again, but safely)
        self.unregister_resource(resource_id)
        return True

    def cleanup_all(self) -> Dict[str, bool]:
        """
        Clean up all tracked resources.

        Returns:
            Dictionary mapping resource IDs to cleanup success status
        """
        results = {}

        with self._instance_lock:
            # Make a copy of resource IDs to avoid modification during iteration
            resource_ids = list(self._resources.keys())

        for resource_id in resource_ids:
            try:
                success = self.cleanup_resource(resource_id)
                results[resource_id] = success
            except Exception as e:
                logger.error(f"Error during cleanup of {resource_id}: {e}")
                results[resource_id] = False
                # Force unregister the resource to avoid it being stuck
                try:
                    self.unregister_resource(resource_id)
                except Exception:
                    pass  # Ignore errors during force unregister

        logger.info(f"Cleaned up {len(results)} resources")
        return results

    def get_resource_info(self, resource_id: str) -> Optional[ResourceInfo]:
        """
        Get information about a tracked resource.

        Args:
            resource_id: ID of resource to query

        Returns:
            ResourceInfo if found, None otherwise
        """
        with self._instance_lock:
            return self._resources.get(resource_id)

    def list_resources(self, resource_type: Optional[str] = None) -> List[ResourceInfo]:
        """
        List all tracked resources, optionally filtered by type.

        Args:
            resource_type: Optional filter by resource type

        Returns:
            List of ResourceInfo objects
        """
        with self._instance_lock:
            resources = list(self._resources.values())

        if resource_type:
            resources = [r for r in resources if r.resource_type == resource_type]

        return resources

    def get_resource_count(self) -> Dict[str, int]:
        """
        Get count of resources by type.

        Returns:
            Dictionary mapping resource types to counts
        """
        counts = {}

        with self._instance_lock:
            for resource_info in self._resources.values():
                resource_type = resource_info.resource_type
                counts[resource_type] = counts.get(resource_type, 0) + 1

        return counts


# Context managers for automatic resource management


@contextmanager
def managed_connection(db_path: str, description: str = ""):
    """
    Context manager for database connections with automatic cleanup.

    Args:
        db_path: Path to SQLite database
        description: Optional description for tracking

    Yields:
        SQLite connection
    """
    resource_manager = ResourceManager()
    conn = None
    resource_id = None

    try:
        conn = sqlite3.connect(db_path)
        resource_id = resource_manager.register_connection(conn, description)
        yield conn
    finally:
        if resource_id:
            resource_manager.cleanup_resource(resource_id)
        elif conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection in context manager: {e}")


@contextmanager
def managed_temp_file(
    suffix: str = "", prefix: str = "pyspark_tools_", dir: Optional[str] = None
):
    """
    Context manager for temporary files with automatic cleanup.

    Args:
        suffix: File suffix
        prefix: File prefix
        dir: Directory for temp file

    Yields:
        Path to temporary file
    """
    resource_manager = ResourceManager()
    temp_path = None
    resource_id = None

    try:
        fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
        os.close(fd)  # Close the file descriptor, keep the path

        path = Path(temp_path)
        resource_id = resource_manager.register_temp_file(
            path, f"Temporary file {path.name}"
        )
        yield path
    finally:
        if resource_id:
            resource_manager.cleanup_resource(resource_id)
        elif temp_path:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Error removing temp file in context manager: {e}")


@contextmanager
def managed_temp_dir(
    suffix: str = "", prefix: str = "pyspark_tools_", dir: Optional[str] = None
):
    """
    Context manager for temporary directories with automatic cleanup.

    Args:
        suffix: Directory suffix
        prefix: Directory prefix
        dir: Parent directory for temp directory

    Yields:
        Path to temporary directory
    """
    resource_manager = ResourceManager()
    temp_path = None
    resource_id = None

    try:
        temp_path = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
        path = Path(temp_path)
        resource_id = resource_manager.register_temp_file(
            path, f"Temporary directory {path.name}"
        )
        yield path
    finally:
        if resource_id:
            resource_manager.cleanup_resource(resource_id)
        elif temp_path:
            try:
                import shutil

                shutil.rmtree(temp_path, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Error removing temp directory in context manager: {e}")


@contextmanager
def managed_process(*args, **kwargs):
    """
    Context manager for subprocesses with automatic cleanup.

    Args:
        *args, **kwargs: Arguments passed to subprocess.Popen

    Yields:
        subprocess.Popen instance
    """
    resource_manager = ResourceManager()
    process = None
    resource_id = None

    try:
        process = subprocess.Popen(*args, **kwargs)
        resource_id = resource_manager.register_process(
            process, f"Process {' '.join(args[0]) if args else 'unknown'}"
        )
        yield process
    finally:
        if resource_id:
            resource_manager.cleanup_resource(resource_id)
        elif process:
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            except Exception as e:
                logger.warning(f"Error terminating process in context manager: {e}")


# Global resource manager instance
_global_resource_manager = None


def get_resource_manager() -> ResourceManager:
    """Get the global resource manager instance."""
    global _global_resource_manager
    if _global_resource_manager is None:
        _global_resource_manager = ResourceManager()
    return _global_resource_manager
