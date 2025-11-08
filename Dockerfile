FROM python:3.11-slim

# Create non-root user
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

# Install system dependencies (minimal set)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies as root, then switch to non-root user
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Copy essential configuration files
COPY pyproject.toml .
COPY .flake8 .

# Copy application code (only essential files)
COPY pyspark_tools/ ./pyspark_tools/
COPY scripts/ ./scripts/
COPY run_server.py .

# NOTE: Do NOT copy tests into the runtime image for production builds.
# Tests are heavy and should be run in test images only.

# Create data directories with proper permissions
RUN mkdir -p /data /output /cache /input && \
    chown -R appuser:appuser /app /data /output /cache /input && \
    chmod 755 /data /output /cache /input

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYSPARK_TOOLS_DB_PATH=/data/memory.sqlite \
    PYSPARK_TOOLS_OUTPUT_DIR=/output \
    PYSPARK_TOOLS_CACHE_DIR=/cache \
    PYSPARK_TOOLS_INPUT_DIR=/input \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Basic validation: recursively check Python syntax (lightweight)
# Move comprehensive testing to CI/test images.
RUN echo "🔍 Running basic validation..." && \
    # recursively compile python files to check syntax
    find pyspark_tools -name '*.py' -print0 | xargs -0 -n1 python -m py_compile || (echo "❌ Syntax check failed" && exit 1) && \
    python -m py_compile run_server.py && \
    echo "✅ Basic syntax validation passed"

# Expose port and use a small healthcheck script (scripts/healthcheck.py)
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python /app/scripts/healthcheck.py

CMD ["python", "run_server.py"]