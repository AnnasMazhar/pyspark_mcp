"""
Tests for the batch processor functionality.
"""

import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from pyspark_tools.batch_processor import BatchProcessor, BatchResult, BatchStatus
from pyspark_tools.memory_manager import MemoryManager
from pyspark_tools.sql_converter import SQLToPySparkConverter


class TestBatchProcessor:
    """Test cases for BatchProcessor class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_sql_files(self, temp_dir):
        """Create sample SQL files for testing."""
        files = []

        # Create SQL file 1
        sql1_path = Path(temp_dir) / "query1.sql"
        with open(sql1_path, "w") as f:
            f.write("SELECT * FROM users WHERE age > 18;")
        files.append(str(sql1_path))

        # Create SQL file 2
        sql2_path = Path(temp_dir) / "query2.sql"
        with open(sql2_path, "w") as f:
            f.write("SELECT name, email FROM customers ORDER BY name;")
        files.append(str(sql2_path))

        # Create SQL file 3 with multiple queries
        sql3_path = Path(temp_dir) / "query3.sql"
        with open(sql3_path, "w") as f:
            f.write(
                """
            SELECT COUNT(*) FROM orders WHERE status = 'completed';
            
            SELECT product_id, SUM(quantity) as total_qty 
            FROM order_items 
            GROUP BY product_id 
            ORDER BY total_qty DESC;
            """
            )
        files.append(str(sql3_path))

        return files

    @pytest.fixture
    def batch_processor(self, temp_dir):
        """Create a batch processor instance for testing."""
        memory_manager = MemoryManager(
            db_path=str(Path(temp_dir) / "test_memory.sqlite")
        )
        sql_converter = SQLToPySparkConverter()
        output_dir = str(Path(temp_dir) / "output")

        return BatchProcessor(
            memory_manager=memory_manager,
            sql_converter=sql_converter,
            max_workers=2,
            base_output_dir=output_dir,
        )

    def test_batch_processor_initialization(self, batch_processor):
        """Test batch processor initialization."""
        assert batch_processor is not None
        assert batch_processor.max_workers == 2
        assert batch_processor.memory_manager is not None
        assert batch_processor.sql_converter is not None
        assert batch_processor.file_handler is not None
        assert batch_processor.output_manager is not None

    def test_process_files_success(self, batch_processor, sample_sql_files):
        """Test successful batch processing of files."""
        result = batch_processor.process_files(
            file_paths=sample_sql_files, job_name="test_batch"
        )

        assert isinstance(result, BatchResult)
        assert result.job_name == "test_batch"
        assert result.status in [BatchStatus.COMPLETED, BatchStatus.FAILED]
        assert result.total_files == len(sample_sql_files)
        assert result.job_id > 0
        assert result.processing_time > 0
        assert result.start_time is not None
        assert result.end_time is not None
        assert isinstance(result.file_results, list)
        assert isinstance(result.conversion_results, list)

        # Check that output directory was created
        assert os.path.exists(result.output_directory)

        # Check that summary files were created
        summary_json = Path(result.output_directory) / "batch_summary.json"
        summary_txt = Path(result.output_directory) / "batch_summary.txt"
        assert summary_json.exists()
        assert summary_txt.exists()

    def test_process_directory(self, batch_processor, sample_sql_files, temp_dir):
        """Test batch processing of a directory."""
        # sample_sql_files are already in temp_dir
        result = batch_processor.process_directory(
            directory_path=temp_dir, job_name="test_directory_batch", recursive=False
        )

        assert isinstance(result, BatchResult)
        assert result.job_name == "test_directory_batch"
        assert result.total_files >= len(sample_sql_files)  # May include other files
        assert result.job_id > 0

    def test_process_empty_directory(self, batch_processor, temp_dir):
        """Test processing an empty directory."""
        empty_dir = Path(temp_dir) / "empty"
        empty_dir.mkdir()

        result = batch_processor.process_directory(
            directory_path=str(empty_dir), job_name="empty_batch"
        )

        assert result.total_files == 0
        assert result.successful_files == 0
        assert result.failed_files == 0
        assert result.status == BatchStatus.COMPLETED

    def test_process_nonexistent_files(self, batch_processor):
        """Test processing non-existent files."""
        nonexistent_files = ["/path/to/nonexistent1.sql", "/path/to/nonexistent2.sql"]

        result = batch_processor.process_files(
            file_paths=nonexistent_files, job_name="nonexistent_batch"
        )

        assert result.total_files == len(nonexistent_files)
        assert result.successful_files == 0
        assert result.failed_files == len(nonexistent_files)
        assert len(result.error_summary) > 0

    def test_batch_status_tracking(self, batch_processor, sample_sql_files):
        """Test batch status tracking during processing."""
        # This test would be more meaningful with longer-running jobs
        # For now, we'll test the basic status retrieval

        result = batch_processor.process_files(
            file_paths=sample_sql_files[:1],  # Process just one file
            job_name="status_test_batch",
        )

        # After completion, status should not be in active jobs
        status = batch_processor.get_batch_status(result.job_id)
        assert status is None  # Job completed, no longer active

        # But should be retrievable from database
        batch_job = batch_processor.memory_manager.get_batch_job(result.job_id)
        assert batch_job is not None
        assert batch_job.job_name == "status_test_batch"

    def test_get_active_jobs(self, batch_processor):
        """Test getting active jobs."""
        active_jobs = batch_processor.get_active_jobs()
        assert isinstance(active_jobs, list)
        # Should be empty since no jobs are currently running
        assert len(active_jobs) == 0

    def test_batch_report_generation(self, batch_processor, sample_sql_files):
        """Test batch report generation."""
        result = batch_processor.process_files(
            file_paths=sample_sql_files, job_name="report_test_batch"
        )

        report = batch_processor.generate_batch_report(result.job_id)
        assert report is not None
        assert "job_info" in report
        assert "file_count" in report
        assert report["job_info"]["job_id"] == result.job_id
        assert report["job_info"]["job_name"] == "report_test_batch"

    def test_optimization_summary(self, batch_processor, sample_sql_files):
        """Test optimization summary generation."""
        result = batch_processor.process_files(
            file_paths=sample_sql_files, job_name="optimization_test_batch"
        )

        assert result.optimization_summary is not None
        assert isinstance(result.optimization_summary, dict)
        # Should contain some optimizations if conversions were successful
        if result.successful_queries > 0:
            assert len(result.optimization_summary) > 0

    def test_error_handling_with_mixed_files(self, batch_processor, temp_dir):
        """Test error handling with a mix of valid and invalid files."""
        # Create a mix of valid and invalid files
        files = []

        # Valid SQL file
        valid_sql = Path(temp_dir) / "valid.sql"
        with open(valid_sql, "w") as f:
            f.write("SELECT * FROM test_table;")
        files.append(str(valid_sql))

        # Invalid file (empty)
        empty_file = Path(temp_dir) / "empty.sql"
        empty_file.touch()
        files.append(str(empty_file))

        # Non-existent file
        files.append("/path/to/nonexistent.sql")

        result = batch_processor.process_files(
            file_paths=files, job_name="mixed_files_batch"
        )

        assert result.total_files == len(files)
        assert (
            result.successful_files >= 2
        )  # Valid file and empty file (both process successfully)
        assert result.failed_files >= 1  # Non-existent file
        assert len(result.error_summary) > 0

    def test_output_directory_creation(
        self, batch_processor, sample_sql_files, temp_dir
    ):
        """Test custom output directory creation."""
        custom_output = str(Path(temp_dir) / "custom_output")

        result = batch_processor.process_files(
            file_paths=sample_sql_files[:1],
            output_dir=custom_output,
            job_name="custom_output_batch",
        )

        assert result.output_directory == custom_output
        assert os.path.exists(custom_output)

        # Check that PySpark files were created
        py_files = list(Path(custom_output).glob("*.py"))
        assert len(py_files) > 0

    def test_concurrent_processing(self, batch_processor, temp_dir):
        """Test concurrent processing with multiple files."""
        # Create multiple SQL files
        files = []
        for i in range(5):
            sql_file = Path(temp_dir) / f"concurrent_test_{i}.sql"
            with open(sql_file, "w") as f:
                f.write(f"SELECT * FROM table_{i} WHERE id > {i};")
            files.append(str(sql_file))

        result = batch_processor.process_files(
            file_paths=files, job_name="concurrent_batch"
        )

        assert result.total_files == len(files)
        # With valid SQL files, we should have some successful conversions
        assert result.successful_files > 0

    def test_memory_persistence(self, batch_processor, sample_sql_files):
        """Test that batch jobs are persisted in memory manager."""
        result = batch_processor.process_files(
            file_paths=sample_sql_files, job_name="persistence_test_batch"
        )

        # Retrieve job from memory manager
        batch_job = batch_processor.memory_manager.get_batch_job(result.job_id)
        assert batch_job is not None
        assert batch_job.job_name == "persistence_test_batch"
        assert batch_job.status in ["completed", "failed"]

        # Check that input files were stored
        stored_files = json.loads(batch_job.input_files)
        assert len(stored_files) == len(sample_sql_files)

    def test_batch_summary_files(self, batch_processor, sample_sql_files):
        """Test that batch summary files are created correctly."""
        result = batch_processor.process_files(
            file_paths=sample_sql_files, job_name="summary_test_batch"
        )

        output_path = Path(result.output_directory)

        # Check JSON summary
        json_summary = output_path / "batch_summary.json"
        assert json_summary.exists()

        with open(json_summary, "r") as f:
            summary_data = json.load(f)

        assert "job_info" in summary_data
        assert "statistics" in summary_data
        assert summary_data["job_info"]["job_name"] == "summary_test_batch"
        assert summary_data["statistics"]["total_files"] == len(sample_sql_files)

        # Check text summary
        text_summary = output_path / "batch_summary.txt"
        assert text_summary.exists()

        with open(text_summary, "r") as f:
            summary_text = f.read()

        assert "Batch Processing Summary" in summary_text
        assert "summary_test_batch" in summary_text
        assert "File Processing Statistics" in summary_text
