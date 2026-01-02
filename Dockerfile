FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Set working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    git \
    rclone \
    curl \
    unzip \
    python3-dev \
    python3-numpy \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY README_PACKAGE.md .
COPY cng_datasets/ ./cng_datasets/

# Create virtual environment and install everything with uv
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install GDAL matching system version, then install the package
RUN export GDAL_VERSION=$(gdal-config --version) && \
    uv pip install "GDAL==${GDAL_VERSION}" && \
    uv pip install -e ".[raster]"

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
