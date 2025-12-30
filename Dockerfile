FROM ubuntu:24.04

# Set working directory
WORKDIR /app

# Install system dependencies including GDAL with Parquet support
RUN apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:ubuntugis/ubuntugis-unstable \
    && apt-get update && apt-get install -y \
    python3.12 \
    python3-pip \
    gdal-bin \
    git \
    libgdal-dev \
    rclone \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install tippecanoe from GitHub releases
RUN curl -L https://github.com/felt/tippecanoe/releases/download/2.62.1/tippecanoe-linux-amd64 -o /usr/local/bin/tippecanoe && \
    chmod +x /usr/local/bin/tippecanoe

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
RUN pip3 install -e . --break-system-packages

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
