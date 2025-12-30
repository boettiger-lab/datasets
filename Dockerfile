FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies including GDAL and rclone
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    rclone \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY README_PACKAGE.md .
COPY cng_datasets/ ./cng_datasets/

# Upgrade pip and install the package
RUN python -m pip install --upgrade pip && \
    pip install -e .

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
