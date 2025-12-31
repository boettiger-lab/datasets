FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Set working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    rclone \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY README_PACKAGE.md .
COPY cng_datasets/ ./cng_datasets/

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and install the package in the virtual environment
RUN pip install --upgrade pip && \
    pip install -e .

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
