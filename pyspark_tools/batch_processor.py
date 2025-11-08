"""
Batch processor for handling multiple SQL files and directories with comprehensive
job management, status tracking, and error handling.

This module provides:
- BatchProcessor class for processing directories and file lists
- Batch job management with status tracking and progress reporting
- Error handling for individual file failures while continuing batch processing
- Comprehensive batch reporting with success/failure statistics
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from .file_utils import ExtractedSQL, FileHandler, FileProcessingResult, OutputManager
from .memory_manager import BatchJob, MemoryManager
from .sql_converter import SQLToPySparkConverter

logger = logging.getLogger(__name__)


class BatchStatus(Enum):
    """Batch job status enumeration."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class ConversionResult:
    """Result of converting a single SQL query to PySpark."""

    sql_query: str
    pyspark_code: str
    optimizations: List[str]
    success: bool
    error_message: Optional[str] = None
    processing_time: float = 0.0
    source_file: str = ""
    query_index: int = 0


@dataclass
class BatchProgress:
    """Progress information for a batch job."""

    job_id: int
    total_files: int
    processed_files: int
    successful_files: int
    failed_files: int
    total_queries: int
    successful_queries: int
    failed_queries: int
    start_time: datetime
    estimated_completion: Optional[datetime] = None
    current_file: Optional[str] = None
    status: BatchStatus = BatchStatus.PENDING


@dataclass
class BatchResult:
    """Complete result of a batch processing job."""

    job_id: int
    job_name: str
    status: BatchStatus
    total_files: int
    successful_files: int
    failed_files: int
    total_queries: int
    successful_queries: int
    failed_queries: int
    file_results: List[FileProcessingResult]
    conversion_results: List[ConversionResult]
    output_directory: str
    processing_time: float
    start_time: datetime
    end_time: Optional[datetime] = None
    error_summary: List[str] = None
    optimization_summary: Dict[str, int] = None


class BatchProcessor:
    """
    Core batch processor for handling multiple SQL files and directories.

    Features:
    - Process directories and file lists
    - Job management with status tracking
    - Progress reporting and monitoring
    - Error handling with continuation
    - Comprehensive reporting
    - Concurrent processing support
    """

    def __init__(
        self,
        memory_manager: Optional[MemoryManager] = None,
        sql_converter: Optional[SQLToPySparkConverter] = None,
        max_workers: int = 4,
        base_output_dir: str = "output",
    ):
        """
        Initialize the batch processor.

        Args:
            memory_manager: Memory manager for job persistence
            sql_converter: SQL to PySpark converter
            max_workers: Maximum number of concurrent workers
            base_output_dir: Base directory for output files
        """
        self.memory_manager = memory_manager or MemoryManager()
        self.sql_converter = sql_converter or SQLToPySparkConverter()
        self.file_handler = FileHandler()
        self.output_manager = OutputManager(base_output_dir)
        self.max_workers = max_workers

        # Job tracking
        self._active_jobs: Dict[int, BatchProgress] = {}
        self._job_lock = threading.Lock()
        self._cancellation_flags: Dict[int, threading.Event] = {}

        # Progress callbacks
        self._progress_callbacks: Dict[int, List[Callable[[BatchProgress], None]]] = {}

        logger.info(f"BatchProcessor initialized with {max_workers} workers")

    def process_directory(
        self,
        directory_path: str,
        output_dir: Optional[str] = None,
        job_name: Optional[str] = None,
        recursive: bool = True,
        progress_callback: Optional[Callable[[BatchProgress], None]] = None,
    ) -> BatchResult:
        """
        Process all supported files in a directory.

        Args:
            directory_path: Path to directory containing SQL files
            output_dir: Output directory for converted files
            job_name: Optional name for the batch job
            recursive: Whether to search subdirectories
            progress_callback: Optional callback for progress updates

        Returns:
            BatchResult with processing results and statistics
        """
        try:
            # Find all files in directory
            files = self.file_handler.find_sql_files(directory_path, recursive)

            if not files:
                logger.warning(
                    f"No supported files found in directory: {directory_path}"
                )
                return self._create_empty_result(
                    job_name or f"dir_{Path(directory_path).name}"
                )

            logger.info(f"Found {len(files)} files to process in {directory_path}")

            # Process the files
            return self.process_files(
                file_paths=files,
                output_dir=output_dir,
                job_name=job_name or f"dir_{Path(directory_path).name}",
                progress_callback=progress_callback,
            )

        except Exception as e:
            logger.error(f"Error processing directory {directory_path}: {e}")
            raise

    def process_files(
        self,
        file_paths: List[str],
        output_dir: Optional[str] = None,
        job_name: Optional[str] = None,
        progress_callback: Optional[Callable[[BatchProgress], None]] = None,
    ) -> BatchResult:
        """
        Process a list of SQL files.

        Args:
            file_paths: List of file paths to process
            output_dir: Output directory for converted files
            job_name: Optional name for the batch job
            progress_callback: Optional callback for progress updates

        Returns:
            BatchResult with processing results and statistics
        """
        start_time = datetime.now()

        # Generate job name if not provided
        if not job_name:
            job_name = f"batch_{start_time.strftime('%Y%m%d_%H%M%S')}"

        # Create batch job in database
        job_id = self.memory_manager.create_batch_job(
            job_name=job_name,
            input_files=file_paths,
            output_directory=output_dir or str(self.output_manager.base_output_dir),
        )

        # Create output directory
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = self.output_manager.create_output_directory(job_name)

        # Initialize progress tracking
        progress = BatchProgress(
            job_id=job_id,
            total_files=len(file_paths),
            processed_files=0,
            successful_files=0,
            failed_files=0,
            total_queries=0,
            successful_queries=0,
            failed_queries=0,
            start_time=start_time,
            status=BatchStatus.RUNNING,
        )

        # Set up cancellation flag
        cancellation_flag = threading.Event()

        with self._job_lock:
            self._active_jobs[job_id] = progress
            self._cancellation_flags[job_id] = cancellation_flag
            if progress_callback:
                self._progress_callbacks[job_id] = [progress_callback]

        try:
            # Update job status to running
            self.memory_manager.update_batch_job_status(
                job_id, BatchStatus.RUNNING.value
            )

            # Process files
            file_results, conversion_results = self._process_files_concurrent(
                file_paths, output_path, progress, cancellation_flag
            )

            # Calculate final statistics
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Determine final status
            if cancellation_flag.is_set():
                final_status = BatchStatus.CANCELLED
            elif progress.failed_files > 0 and progress.successful_files == 0:
                final_status = BatchStatus.FAILED
            else:
                final_status = BatchStatus.COMPLETED

            # Update job status
            self.memory_manager.update_batch_job_status(job_id, final_status.value)

            # Generate optimization summary
            optimization_summary = self._generate_optimization_summary(
                conversion_results
            )

            # Generate error summary
            error_summary = self._generate_error_summary(
                file_results, conversion_results
            )

            # Create batch result
            result = BatchResult(
                job_id=job_id,
                job_name=job_name,
                status=final_status,
                total_files=len(file_paths),
                successful_files=progress.successful_files,
                failed_files=progress.failed_files,
                total_queries=progress.total_queries,
                successful_queries=progress.successful_queries,
                failed_queries=progress.failed_queries,
                file_results=file_results,
                conversion_results=conversion_results,
                output_directory=str(output_path),
                processing_time=processing_time,
                start_time=start_time,
                end_time=end_time,
                error_summary=error_summary,
                optimization_summary=optimization_summary,
            )

            # Create batch summary file
            self._create_batch_summary(result, output_path)

            logger.info(
                f"Batch job {job_name} completed: {progress.successful_files}/{len(file_paths)} files successful"
            )

            return result

        except Exception as e:
            logger.error(f"Batch job {job_name} failed: {e}")
            self.memory_manager.update_batch_job_status(
                job_id, BatchStatus.FAILED.value
            )
            raise
        finally:
            # Clean up tracking
            with self._job_lock:
                self._active_jobs.pop(job_id, None)
                self._cancellation_flags.pop(job_id, None)
                self._progress_callbacks.pop(job_id, None)

    def _process_files_concurrent(
        self,
        file_paths: List[str],
        output_path: Path,
        progress: BatchProgress,
        cancellation_flag: threading.Event,
    ) -> tuple[List[FileProcessingResult], List[ConversionResult]]:
        """Process files concurrently with progress tracking."""
        file_results = []
        conversion_results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all file processing tasks
            future_to_file = {
                executor.submit(
                    self._process_single_file,
                    file_path,
                    output_path,
                    progress,
                    cancellation_flag,
                ): file_path
                for file_path in file_paths
            }

            # Process completed tasks
            for future in as_completed(future_to_file):
                if cancellation_flag.is_set():
                    logger.info("Batch processing cancelled")
                    break

                file_path = future_to_file[future]

                try:
                    file_result, file_conversions = future.result()
                    file_results.append(file_result)
                    conversion_results.extend(file_conversions)

                    # Update progress
                    with self._job_lock:
                        progress.processed_files += 1
                        if file_result.success:
                            progress.successful_files += 1
                        else:
                            progress.failed_files += 1

                        progress.total_queries += len(file_result.extracted_queries)
                        progress.successful_queries += len(
                            [c for c in file_conversions if c.success]
                        )
                        progress.failed_queries += len(
                            [c for c in file_conversions if not c.success]
                        )

                        # Update estimated completion time
                        if progress.processed_files > 0:
                            avg_time_per_file = (
                                datetime.now() - progress.start_time
                            ).total_seconds() / progress.processed_files
                            remaining_files = (
                                progress.total_files - progress.processed_files
                            )
                            progress.estimated_completion = datetime.now() + timedelta(
                                seconds=avg_time_per_file * remaining_files
                            )

                        progress.current_file = None

                        # Notify progress callbacks
                        for callback in self._progress_callbacks.get(
                            progress.job_id, []
                        ):
                            try:
                                callback(progress)
                            except Exception as e:
                                logger.warning(f"Progress callback error: {e}")

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")

                    # Create error result
                    error_result = FileProcessingResult(
                        file_path=file_path,
                        success=False,
                        extracted_queries=[],
                        error_message=str(e),
                    )
                    file_results.append(error_result)

                    # Update progress
                    with self._job_lock:
                        progress.processed_files += 1
                        progress.failed_files += 1

        return file_results, conversion_results

    def _process_single_file(
        self,
        file_path: str,
        output_path: Path,
        progress: BatchProgress,
        cancellation_flag: threading.Event,
    ) -> tuple[FileProcessingResult, List[ConversionResult]]:
        """Process a single file and convert extracted SQL queries."""
        if cancellation_flag.is_set():
            return (
                FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message="Processing cancelled",
                ),
                [],
            )

        # Update current file in progress
        with self._job_lock:
            progress.current_file = file_path

        start_time = datetime.now()

        try:
            # Extract SQL queries from file
            file_result = self.file_handler.process_file(file_path)

            if not file_result.success:
                return file_result, []

            # Convert each extracted query
            conversion_results = []

            for i, extracted_sql in enumerate(file_result.extracted_queries):
                if cancellation_flag.is_set():
                    break

                conversion_result = self._convert_single_query(
                    extracted_sql, file_path, i, output_path
                )
                conversion_results.append(conversion_result)

            return file_result, conversion_results

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return (
                FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message=str(e),
                    processing_time=(datetime.now() - start_time).total_seconds(),
                ),
                [],
            )

    def _convert_single_query(
        self,
        extracted_sql: ExtractedSQL,
        source_file: str,
        query_index: int,
        output_path: Path,
    ) -> ConversionResult:
        """Convert a single SQL query to PySpark code."""
        start_time = datetime.now()

        try:
            # Convert SQL to PySpark
            pyspark_code, optimizations = self.sql_converter.convert_sql_to_pyspark(
                extracted_sql.query
            )

            # Generate output filename
            output_filename = self.output_manager.generate_output_filename(
                source_file, query_index
            )
            output_file_path = output_path / output_filename

            # Save PySpark code
            metadata = {
                "source_file": source_file,
                "query_index": query_index + 1,
                "page_number": extracted_sql.page_number,
                "line_number": extracted_sql.line_number,
                "confidence": extracted_sql.confidence,
                "optimizations": ", ".join(optimizations),
                "generated_at": datetime.now().isoformat(),
            }

            self.output_manager.save_pyspark_code(
                pyspark_code, output_file_path, metadata
            )

            # Store conversion in memory
            self.memory_manager.store_conversion(
                sql_query=extracted_sql.query,
                pyspark_code=pyspark_code,
                optimization_notes="\n".join(optimizations),
            )

            processing_time = (datetime.now() - start_time).total_seconds()

            return ConversionResult(
                sql_query=extracted_sql.query,
                pyspark_code=pyspark_code,
                optimizations=optimizations,
                success=True,
                processing_time=processing_time,
                source_file=source_file,
                query_index=query_index,
            )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"Error converting query from {source_file}[{query_index}]: {e}"
            )

            return ConversionResult(
                sql_query=extracted_sql.query,
                pyspark_code="",
                optimizations=[],
                success=False,
                error_message=str(e),
                processing_time=processing_time,
                source_file=source_file,
                query_index=query_index,
            )

    def get_batch_status(self, job_id: int) -> Optional[BatchProgress]:
        """Get current status of a batch job."""
        with self._job_lock:
            return self._active_jobs.get(job_id)

    def cancel_batch_job(self, job_id: int) -> bool:
        """Cancel a running batch job."""
        with self._job_lock:
            if job_id in self._cancellation_flags:
                self._cancellation_flags[job_id].set()
                if job_id in self._active_jobs:
                    self._active_jobs[job_id].status = BatchStatus.CANCELLED
                self.memory_manager.update_batch_job_status(
                    job_id, BatchStatus.CANCELLED.value
                )
                logger.info(f"Batch job {job_id} cancelled")
                return True
        return False

    def pause_batch_job(self, job_id: int) -> bool:
        """Pause a running batch job (not implemented in this version)."""
        # Note: Pausing would require more complex thread management
        # For now, we only support cancellation
        logger.warning("Batch job pausing not implemented")
        return False

    def get_active_jobs(self) -> List[BatchProgress]:
        """Get list of all active batch jobs."""
        with self._job_lock:
            return list(self._active_jobs.values())

    def add_progress_callback(
        self, job_id: int, callback: Callable[[BatchProgress], None]
    ) -> bool:
        """Add a progress callback for a specific job."""
        with self._job_lock:
            if job_id in self._active_jobs:
                if job_id not in self._progress_callbacks:
                    self._progress_callbacks[job_id] = []
                self._progress_callbacks[job_id].append(callback)
                return True
        return False

    def _generate_optimization_summary(
        self, conversion_results: List[ConversionResult]
    ) -> Dict[str, int]:
        """Generate summary of optimizations applied."""
        optimization_counts = {}

        for result in conversion_results:
            if result.success:
                for optimization in result.optimizations:
                    optimization_counts[optimization] = (
                        optimization_counts.get(optimization, 0) + 1
                    )

        return optimization_counts

    def _generate_error_summary(
        self,
        file_results: List[FileProcessingResult],
        conversion_results: List[ConversionResult],
    ) -> List[str]:
        """Generate summary of errors encountered."""
        errors = []

        # File processing errors
        for result in file_results:
            if not result.success and result.error_message:
                errors.append(f"File {result.file_path}: {result.error_message}")

        # Conversion errors
        for result in conversion_results:
            if not result.success and result.error_message:
                errors.append(
                    f"Query from {result.source_file}[{result.query_index}]: {result.error_message}"
                )

        return errors

    def _create_batch_summary(self, result: BatchResult, output_path: Path) -> None:
        """Create comprehensive batch summary file."""
        summary_data = {
            "job_info": {
                "job_id": result.job_id,
                "job_name": result.job_name,
                "status": result.status.value,
                "start_time": result.start_time.isoformat(),
                "end_time": result.end_time.isoformat() if result.end_time else None,
                "processing_time": result.processing_time,
            },
            "statistics": {
                "total_files": result.total_files,
                "successful_files": result.successful_files,
                "failed_files": result.failed_files,
                "success_rate": (
                    (result.successful_files / result.total_files * 100)
                    if result.total_files > 0
                    else 0
                ),
                "total_queries": result.total_queries,
                "successful_queries": result.successful_queries,
                "failed_queries": result.failed_queries,
                "query_success_rate": (
                    (result.successful_queries / result.total_queries * 100)
                    if result.total_queries > 0
                    else 0
                ),
            },
            "optimizations": result.optimization_summary or {},
            "errors": result.error_summary or [],
            "output_directory": result.output_directory,
        }

        # Save JSON summary
        json_summary_path = output_path / "batch_summary.json"
        with open(json_summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, default=str)

        # Save human-readable summary
        text_summary_path = output_path / "batch_summary.txt"
        with open(text_summary_path, "w", encoding="utf-8") as f:
            f.write("Batch Processing Summary\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Job Name: {result.job_name}\n")
            f.write(f"Job ID: {result.job_id}\n")
            f.write(f"Status: {result.status.value.upper()}\n")
            f.write(f"Start Time: {result.start_time}\n")
            if result.end_time:
                f.write(f"End Time: {result.end_time}\n")
            f.write(f"Processing Time: {result.processing_time:.2f} seconds\n\n")

            f.write("File Processing Statistics:\n")
            f.write(f"  Total Files: {result.total_files}\n")
            f.write(f"  Successful: {result.successful_files}\n")
            f.write(f"  Failed: {result.failed_files}\n")
            f.write(
                f"  Success Rate: {(result.successful_files / result.total_files * 100):.1f}%\n\n"
            )

            f.write("Query Conversion Statistics:\n")
            f.write(f"  Total Queries: {result.total_queries}\n")
            f.write(f"  Successful: {result.successful_queries}\n")
            f.write(f"  Failed: {result.failed_queries}\n")
            if result.total_queries > 0:
                f.write(
                    f"  Success Rate: {(result.successful_queries / result.total_queries * 100):.1f}%\n\n"
                )
            else:
                f.write("  Success Rate: N/A (no queries processed)\n\n")

            if result.optimization_summary:
                f.write("Optimization Summary:\n")
                for optimization, count in sorted(
                    result.optimization_summary.items(),
                    key=lambda x: x[1],
                    reverse=True,
                ):
                    f.write(f"  {optimization}: {count} times\n")
                f.write("\n")

            if result.error_summary:
                f.write("Error Summary:\n")
                for i, error in enumerate(
                    result.error_summary[:10], 1
                ):  # Limit to first 10 errors
                    f.write(f"  {i}. {error}\n")
                if len(result.error_summary) > 10:
                    f.write(f"  ... and {len(result.error_summary) - 10} more errors\n")
                f.write("\n")

            f.write(f"Output Directory: {result.output_directory}\n")

        logger.info(f"Batch summary saved to {text_summary_path}")

    def _create_empty_result(self, job_name: str) -> BatchResult:
        """Create an empty result for cases with no files to process."""
        return BatchResult(
            job_id=0,
            job_name=job_name,
            status=BatchStatus.COMPLETED,
            total_files=0,
            successful_files=0,
            failed_files=0,
            total_queries=0,
            successful_queries=0,
            failed_queries=0,
            file_results=[],
            conversion_results=[],
            output_directory="",
            processing_time=0.0,
            start_time=datetime.now(),
            end_time=datetime.now(),
            error_summary=[],
            optimization_summary={},
        )

    def generate_batch_report(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Generate a comprehensive report for a completed batch job."""
        try:
            # Get job from database
            batch_job = self.memory_manager.get_batch_job(job_id)
            if not batch_job:
                return None

            # Parse input files
            input_files = json.loads(batch_job.input_files)

            # Get recent conversions that might be from this batch
            # (This is a simplified approach - in a full implementation,
            # we'd track conversions by batch job ID)
            recent_conversions = self.memory_manager.get_recent_conversions(
                len(input_files) * 5
            )

            report = {
                "job_info": {
                    "job_id": batch_job.id,
                    "job_name": batch_job.job_name,
                    "status": batch_job.status,
                    "created_at": batch_job.created_at,
                    "input_files": input_files,
                    "output_directory": batch_job.output_directory,
                },
                "file_count": len(input_files),
                "recent_conversions": len(recent_conversions),
                "generated_at": datetime.now().isoformat(),
            }

            return report

        except Exception as e:
            logger.error(f"Error generating batch report for job {job_id}: {e}")
            return None
