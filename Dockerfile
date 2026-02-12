# Dockerfile for Validation Framework
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libsasl2-modules \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ src/
COPY scripts/ scripts/
COPY config/ config/
COPY templates/ templates/

# Create output directories
RUN mkdir -p output/logs output/json output/reports

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Make scripts executable
RUN chmod +x scripts/run_validation.py

# Default command
ENTRYPOINT ["python", "scripts/run_validation.py"]
CMD ["--help"]

# Usage examples:
# Build: docker build -t validation-framework .
# Run: docker run -v $(pwd)/config:/app/config -v $(pwd)/output:/app/output validation-framework run config/validation_config.yaml
# Interactive: docker run -it validation-framework bash
