"""
File handling utilities for batch processing SQL files and PDF extraction.

This module provides utilities for:
- Extracting SQL queries from PDF documents
- Handling multiple SQL files and directory traversal
- SQL query detection and extraction from mixed text content
- File naming and organization utilities for output management
"""

import logging
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

try:
    import pypdf

    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False

try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

logger = logging.getLogger(__name__)

# Security constants
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB max file size
MAX_PROCESSING_TIME = 300  # 5 minutes max processing time per file
ALLOWED_EXTENSIONS = {".sql", ".txt", ".pdf"}
BLOCKED_PATHS = {
    "/etc",
    "/proc",
    "/sys",
    "/dev",
    "/root",
}  # System directories to avoid


@dataclass
class ExtractedSQL:
    """Represents an extracted SQL query with metadata."""

    query: str
    source_file: str
    page_number: Optional[int] = None
    line_number: Optional[int] = None
    confidence: float = 1.0  # Confidence in extraction quality (0.0 to 1.0)


@dataclass
class FileProcessingResult:
    """Result of processing a single file."""

    file_path: str
    success: bool
    extracted_queries: List[ExtractedSQL]
    error_message: Optional[str] = None
    processing_time: float = 0.0


class SQLQueryExtractor:
    """Utility class for extracting SQL queries from various text sources."""

    # SQL keywords that typically start a query
    SQL_KEYWORDS = {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "WITH",
        "MERGE",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXPLAIN",
        "ANALYZE",
    }

    # Common SQL patterns
    SQL_PATTERNS = [
        # Standard SQL queries
        r"(?i)\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH|MERGE|TRUNCATE)\b.*?(?=;|\n\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH|MERGE|TRUNCATE|\Z))",
        # Queries that might not end with semicolon
        r"(?i)\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH|MERGE|TRUNCATE)\b[^;]*?(?=\n\s*\n|\Z)",
    ]

    def __init__(self):
        self.compiled_patterns = [
            re.compile(pattern, re.DOTALL | re.MULTILINE)
            for pattern in self.SQL_PATTERNS
        ]

    def extract_from_text(self, text: str, source_file: str = "") -> List[ExtractedSQL]:
        """Extract SQL queries from plain text."""
        queries = []

        # Clean up the text
        text = self._clean_text(text)

        # Try each pattern
        for pattern in self.compiled_patterns:
            matches = pattern.finditer(text)
            for match in matches:
                query = match.group().strip()
                if self._is_valid_sql_query(query):
                    # Calculate line number
                    line_number = text[: match.start()].count("\n") + 1

                    queries.append(
                        ExtractedSQL(
                            query=query,
                            source_file=source_file,
                            line_number=line_number,
                            confidence=self._calculate_confidence(query),
                        )
                    )

        # Remove duplicates and sort by confidence
        queries = self._deduplicate_queries(queries)
        return sorted(queries, key=lambda x: x.confidence, reverse=True)

    def _clean_text(self, text: str) -> str:
        """Clean text for better SQL extraction."""
        # Remove common PDF artifacts
        text = re.sub(r"\s+", " ", text)  # Normalize whitespace
        text = re.sub(r"[^\x00-\x7F]+", " ", text)  # Remove non-ASCII chars
        text = text.replace("\x00", " ")  # Remove null bytes
        return text.strip()

    def _is_valid_sql_query(self, query: str) -> bool:
        """Check if extracted text looks like a valid SQL query."""
        if len(query.strip()) < 10:  # Too short
            return False

        # Must start with SQL keyword
        first_word = query.strip().split()[0].upper()
        if first_word not in self.SQL_KEYWORDS:
            return False

        # Should contain typical SQL elements
        query_upper = query.upper()
        if "SELECT" in query_upper and "FROM" not in query_upper:
            return False  # SELECT without FROM is suspicious

        return True

    def _calculate_confidence(self, query: str) -> float:
        """Calculate confidence score for extracted query."""
        confidence = 0.5  # Base confidence

        query_upper = query.upper()

        # Boost confidence for well-formed queries
        if query.strip().endswith(";"):
            confidence += 0.2

        if "SELECT" in query_upper and "FROM" in query_upper:
            confidence += 0.2

        if any(
            keyword in query_upper
            for keyword in ["WHERE", "GROUP BY", "ORDER BY", "HAVING"]
        ):
            confidence += 0.1

        # Reduce confidence for suspicious patterns
        if len(query.split()) < 5:
            confidence -= 0.2

        if query.count("(") != query.count(")"):
            confidence -= 0.1

        return max(0.0, min(1.0, confidence))

    def _deduplicate_queries(self, queries: List[ExtractedSQL]) -> List[ExtractedSQL]:
        """Remove duplicate queries based on normalized content."""
        seen = set()
        unique_queries = []

        for query in queries:
            # Normalize query for comparison
            normalized = re.sub(r"\s+", " ", query.query.strip().upper())
            if normalized not in seen:
                seen.add(normalized)
                unique_queries.append(query)

        return unique_queries


class PDFExtractor:
    """Utility class for extracting SQL queries from PDF documents."""

    def __init__(self):
        self.sql_extractor = SQLQueryExtractor()

        if not (PYPDF2_AVAILABLE or PDFPLUMBER_AVAILABLE):
            raise ImportError(
                "Neither pypdf nor pdfplumber is available. Please install one of them."
            )

    def extract_sql_from_pdf(self, pdf_path: str) -> List[ExtractedSQL]:
        """Extract SQL queries from a PDF file."""
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        # Try pdfplumber first (better text extraction), then pypdf
        if PDFPLUMBER_AVAILABLE:
            return self._extract_with_pdfplumber(pdf_path)
        elif PYPDF2_AVAILABLE:
            return self._extract_with_pypdf2(pdf_path)
        else:
            raise ImportError("No PDF processing library available")

    def _extract_with_pdfplumber(self, pdf_path: str) -> List[ExtractedSQL]:
        """Extract text using pdfplumber."""
        import pdfplumber

        queries = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    if text:
                        page_queries = self.sql_extractor.extract_from_text(
                            text, pdf_path
                        )
                        # Update page numbers
                        for query in page_queries:
                            query.page_number = page_num
                        queries.extend(page_queries)
        except Exception as e:
            logger.error(f"Error extracting from PDF {pdf_path} with pdfplumber: {e}")
            raise

        return queries

    def _extract_with_pypdf2(self, pdf_path: str) -> List[ExtractedSQL]:
        """Extract text using pypdf."""
        import pypdf

        queries = []

        try:
            with open(pdf_path, "rb") as file:
                pdf_reader = pypdf.PdfReader(file)

                for page_num, page in enumerate(pdf_reader.pages, 1):
                    text = page.extract_text()
                    if text:
                        page_queries = self.sql_extractor.extract_from_text(
                            text, pdf_path
                        )
                        # Update page numbers
                        for query in page_queries:
                            query.page_number = page_num
                        queries.extend(page_queries)
        except Exception as e:
            logger.error(f"Error extracting from PDF {pdf_path} with pypdf: {e}")
            raise

        return queries


def _is_safe_path(file_path: str, base_path: str = None) -> bool:
    """Check if a file path is safe to access (prevents path traversal attacks)."""
    try:
        # Resolve the path to handle .. and symlinks
        resolved_path = Path(file_path).resolve()

        # Check for obvious path traversal patterns in the original path
        if ".." in file_path:
            logger.warning(f"Path traversal pattern detected: {file_path}")
            return False

        # Check if path contains blocked system directories
        for blocked in BLOCKED_PATHS:
            if str(resolved_path).startswith(blocked):
                logger.warning(f"Blocked access to system directory: {resolved_path}")
                return False

        # If base_path is provided, ensure the file is within it
        if base_path:
            base_resolved = Path(base_path).resolve()
            try:
                resolved_path.relative_to(base_resolved)
            except ValueError:
                logger.warning(f"Path traversal attempt detected: {file_path}")
                return False

        return True
    except (OSError, ValueError) as e:
        logger.warning(f"Invalid path: {file_path} - {e}")
        return False


def _is_safe_file_size(file_path: str) -> bool:
    """Check if file size is within safe limits."""
    try:
        file_size = os.path.getsize(file_path)
        if file_size > MAX_FILE_SIZE:
            logger.warning(f"File too large: {file_path} ({file_size} bytes)")
            return False
        return True
    except OSError as e:
        logger.warning(f"Cannot check file size: {file_path} - {e}")
        return False


def _sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent injection attacks."""
    # Remove path components first
    filename = os.path.basename(filename)
    # Remove or replace dangerous characters
    sanitized = re.sub(r"[^\w\-_.]", "_", filename)
    # Prevent hidden files and relative paths
    sanitized = sanitized.lstrip("._")
    # Remove multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Limit length
    if len(sanitized) > 255:
        sanitized = sanitized[:255]
    return sanitized or "unnamed_file"


class FileHandler:
    """Utility class for handling multiple SQL files and directory operations."""

    SUPPORTED_EXTENSIONS = ALLOWED_EXTENSIONS

    def __init__(self, base_directory: str = None):
        self.sql_extractor = SQLQueryExtractor()
        self.pdf_extractor = None
        self.base_directory = base_directory

        # Initialize PDF extractor only if libraries are available
        if PYPDF2_AVAILABLE or PDFPLUMBER_AVAILABLE:
            try:
                self.pdf_extractor = PDFExtractor()
            except ImportError:
                logger.warning(
                    "PDF extraction libraries not available. PDF files will be skipped."
                )
        else:
            logger.warning(
                "PDF extraction libraries not available. PDF files will be skipped."
            )

    def find_sql_files(self, directory: str, recursive: bool = True) -> List[str]:
        """Find all SQL-related files in a directory."""
        # Security check: validate directory path
        if not _is_safe_path(directory, self.base_directory):
            raise ValueError(f"Unsafe directory path: {directory}")

        directory_path = Path(directory).resolve()

        if not directory_path.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        if not directory_path.is_dir():
            raise ValueError(f"Path is not a directory: {directory}")

        files = []

        if recursive:
            pattern = "**/*"
        else:
            pattern = "*"

        try:
            for file_path in directory_path.glob(pattern):
                if (
                    file_path.is_file()
                    and file_path.suffix.lower() in self.SUPPORTED_EXTENSIONS
                    and _is_safe_path(str(file_path), self.base_directory)
                    and _is_safe_file_size(str(file_path))
                ):
                    files.append(str(file_path))
        except (OSError, PermissionError) as e:
            logger.error(f"Error accessing directory {directory}: {e}")
            raise

        return sorted(files)

    def process_file(self, file_path: str) -> FileProcessingResult:
        """Process a single file and extract SQL queries."""
        start_time = datetime.now()

        try:
            # Security checks
            if not _is_safe_path(file_path, self.base_directory):
                return FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message=f"Unsafe file path: {file_path}",
                )

            file_path_obj = Path(file_path).resolve()

            if not file_path_obj.exists():
                return FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message=f"File not found: {file_path}",
                )

            # Check file size
            if not _is_safe_file_size(str(file_path_obj)):
                return FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message=f"File too large: {file_path}",
                )

            extension = file_path_obj.suffix.lower()

            # Validate extension
            if extension not in self.SUPPORTED_EXTENSIONS:
                return FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message=f"Unsupported file extension: {extension}",
                )

            # Process based on file type
            if extension == ".pdf":
                if self.pdf_extractor is None:
                    return FileProcessingResult(
                        file_path=file_path,
                        success=False,
                        extracted_queries=[],
                        error_message="PDF extraction libraries not available. Please install pypdf or pdfplumber.",
                    )
                queries = self.pdf_extractor.extract_sql_from_pdf(str(file_path_obj))
            elif extension in {".sql", ".txt"}:
                # Use safe file reading with timeout
                try:
                    with open(
                        file_path_obj, "r", encoding="utf-8", errors="ignore"
                    ) as f:
                        content = f.read(MAX_FILE_SIZE)  # Limit read size
                    queries = self.sql_extractor.extract_from_text(content, file_path)
                except (OSError, PermissionError) as e:
                    return FileProcessingResult(
                        file_path=file_path,
                        success=False,
                        extracted_queries=[],
                        error_message=f"Cannot read file: {e}",
                    )

            processing_time = (datetime.now() - start_time).total_seconds()

            # Check for processing timeout
            if processing_time > MAX_PROCESSING_TIME:
                logger.warning(f"Processing timeout for file: {file_path}")
                return FileProcessingResult(
                    file_path=file_path,
                    success=False,
                    extracted_queries=[],
                    error_message="Processing timeout exceeded",
                    processing_time=processing_time,
                )

            return FileProcessingResult(
                file_path=file_path,
                success=True,
                extracted_queries=queries,
                processing_time=processing_time,
            )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error processing file {file_path}: {e}")

            return FileProcessingResult(
                file_path=file_path,
                success=False,
                extracted_queries=[],
                error_message=str(e),
                processing_time=processing_time,
            )

    def process_files(self, file_paths: List[str]) -> List[FileProcessingResult]:
        """Process multiple files and extract SQL queries."""
        results = []

        for file_path in file_paths:
            result = self.process_file(file_path)
            results.append(result)
            logger.info(
                f"Processed {file_path}: {len(result.extracted_queries)} queries extracted"
            )

        return results

    def process_directory(
        self, directory: str, recursive: bool = True
    ) -> List[FileProcessingResult]:
        """Process all supported files in a directory."""
        files = self.find_sql_files(directory, recursive)
        logger.info(f"Found {len(files)} files to process in {directory}")

        return self.process_files(files)


class OutputManager:
    """Utility class for organizing and naming output files."""

    def __init__(self, base_output_dir: str = "output"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

    def generate_output_filename(
        self, source_file: str, query_index: int = 0, timestamp: bool = True
    ) -> str:
        """Generate a meaningful filename for converted PySpark code."""
        source_path = Path(source_file)
        base_name = source_path.stem

        # Sanitize the base name for security
        base_name = _sanitize_filename(base_name)

        # Add query index if multiple queries
        if query_index > 0:
            base_name = f"{base_name}_query_{query_index + 1}"

        # Add timestamp if requested
        if timestamp:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"{timestamp_str}_{base_name}"

        return f"{base_name}.py"

    def create_output_directory(self, batch_name: str = None) -> Path:
        """Create a directory for batch output."""
        if batch_name:
            # Sanitize batch name for security
            batch_name = _sanitize_filename(batch_name)
            output_dir = self.base_output_dir / batch_name
        else:
            # Use timestamp-based directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = self.base_output_dir / f"batch_{timestamp}"

        # Ensure output directory is within base directory
        try:
            output_dir = output_dir.resolve()
            self.base_output_dir.resolve().relative_to(output_dir.parent)
        except ValueError:
            raise ValueError(
                f"Output directory outside of base directory: {output_dir}"
            )

        output_dir.mkdir(parents=True, exist_ok=True, mode=0o755)  # Secure permissions
        return output_dir

    def organize_by_source(self, output_dir: Path, source_file: str) -> Path:
        """Create subdirectories organized by source file."""
        source_path = Path(source_file)
        source_dir_name = _sanitize_filename(source_path.stem)

        organized_dir = output_dir / source_dir_name

        # Security check: ensure directory is within output_dir
        try:
            organized_dir = organized_dir.resolve()
            output_dir.resolve().relative_to(organized_dir.parent)
        except ValueError:
            raise ValueError(
                f"Organized directory outside of output directory: {organized_dir}"
            )

        organized_dir.mkdir(parents=True, exist_ok=True, mode=0o755)

        return organized_dir

    def save_query_to_file(
        self, query: str, output_path: Path, metadata: Dict = None
    ) -> None:
        """Save a SQL query to a file with optional metadata."""
        # Security check: validate output path
        if not _is_safe_path(str(output_path)):
            raise ValueError(f"Unsafe output path: {output_path}")

        # Limit query size to prevent resource exhaustion
        if len(query) > MAX_FILE_SIZE:
            raise ValueError(f"Query too large: {len(query)} bytes")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                if metadata:
                    f.write("-- Query Metadata\n")
                    for key, value in metadata.items():
                        # Sanitize metadata to prevent injection
                        safe_key = _sanitize_filename(str(key))
                        safe_value = (
                            str(value).replace("\n", " ").replace("\r", " ")[:500]
                        )
                        f.write(f"-- {safe_key}: {safe_value}\n")
                    f.write("\n")

                f.write(query)

                if not query.strip().endswith(";"):
                    f.write(";")

            # Set secure file permissions
            os.chmod(output_path, 0o644)

        except (OSError, PermissionError) as e:
            logger.error(f"Failed to save query to {output_path}: {e}")
            raise

    def save_pyspark_code(
        self, code: str, output_path: Path, metadata: Dict = None
    ) -> None:
        """Save PySpark code to a file with optional metadata."""
        # Security check: validate output path
        if not _is_safe_path(str(output_path)):
            raise ValueError(f"Unsafe output path: {output_path}")

        # Limit code size to prevent resource exhaustion
        if len(code) > MAX_FILE_SIZE:
            raise ValueError(f"Code too large: {len(code)} bytes")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                if metadata:
                    f.write('"""\n')
                    f.write("Generated PySpark Code\n")
                    f.write("=" * 50 + "\n")
                    for key, value in metadata.items():
                        # Sanitize metadata to prevent injection
                        safe_key = _sanitize_filename(str(key))
                        safe_value = (
                            str(value).replace("\n", " ").replace("\r", " ")[:500]
                        )
                        f.write(f"{safe_key}: {safe_value}\n")
                    f.write('"""\n\n')

                f.write(code)

            # Set secure file permissions
            os.chmod(output_path, 0o644)

        except (OSError, PermissionError) as e:
            logger.error(f"Failed to save PySpark code to {output_path}: {e}")
            raise

    def create_batch_summary(
        self,
        output_dir: Path,
        results: List[FileProcessingResult],
        processing_stats: Dict = None,
    ) -> Path:
        """Create a summary file for batch processing results."""
        summary_path = output_dir / "batch_summary.txt"

        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("Batch Processing Summary\n")
            f.write("=" * 50 + "\n\n")

            if processing_stats:
                f.write("Processing Statistics:\n")
                for key, value in processing_stats.items():
                    f.write(f"  {key}: {value}\n")
                f.write("\n")

            f.write("File Processing Results:\n")
            f.write("-" * 30 + "\n")

            total_queries = 0
            successful_files = 0

            for result in results:
                status = "SUCCESS" if result.success else "FAILED"
                query_count = len(result.extracted_queries)
                total_queries += query_count

                if result.success:
                    successful_files += 1

                f.write(f"[{status}] {result.file_path}\n")
                f.write(f"  Queries extracted: {query_count}\n")
                f.write(f"  Processing time: {result.processing_time:.2f}s\n")

                if not result.success:
                    f.write(f"  Error: {result.error_message}\n")

                f.write("\n")

            f.write("Summary:\n")
            f.write(f"  Total files processed: {len(results)}\n")
            f.write(f"  Successful files: {successful_files}\n")
            f.write(f"  Failed files: {len(results) - successful_files}\n")
            f.write(f"  Total queries extracted: {total_queries}\n")

        return summary_path


# Convenience functions for common operations
def extract_sql_from_pdf(pdf_path: str) -> List[ExtractedSQL]:
    """Convenience function to extract SQL from a PDF file."""
    extractor = PDFExtractor()
    return extractor.extract_sql_from_pdf(pdf_path)


def extract_sql_from_text(text: str, source_file: str = "") -> List[ExtractedSQL]:
    """Convenience function to extract SQL from text."""
    extractor = SQLQueryExtractor()
    return extractor.extract_from_text(text, source_file)


def process_sql_files(
    file_paths: Union[str, List[str]], output_dir: str = "output"
) -> List[FileProcessingResult]:
    """Convenience function to process SQL files."""
    if isinstance(file_paths, str):
        if os.path.isdir(file_paths):
            # Process directory
            handler = FileHandler()
            return handler.process_directory(file_paths)
        else:
            # Process single file
            file_paths = [file_paths]

    handler = FileHandler()
    return handler.process_files(file_paths)
