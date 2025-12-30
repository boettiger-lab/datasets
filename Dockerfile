FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0

# Set working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    git \
    rclone \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install tippecanoe from GitHub releases
RUN curl -L https://github.com/felt/tippecanoe/releases/download/2.62.1/tippecanoe-linux-amd64 -o /usr/local/bin/tippecanoe && \
    chmod +x /usr/local/bin/tippecanoe

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY README_PACKAGE.md .
COPY cng_datasets/ ./cng_datasets/

# Upgrade pip and install the package
RUN pip3 install --upgrade pip && \
    pip3 install -e .

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
