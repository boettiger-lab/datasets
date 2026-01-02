FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Set working directory
WORKDIR /app

# Install additional system dependencies and Python
RUN apt-get update && apt-get install -y \
    git \
    rclone \
    curl \
    unzip \
    python3-pip \
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

# Install GDAL Python bindings matching the system GDAL version
RUN export GDAL_VERSION=$(gdal-config --version) && \
    pip3 install --no-cache-dir "GDAL==${GDAL_VERSION}"

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY README_PACKAGE.md .
COPY cng_datasets/ ./cng_datasets/

# Create virtual environment and install the package with uv
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN uv pip install pip  # Ensure pip is available for users

# Install GDAL in venv to match system version
RUN export GDAL_VERSION=$(gdal-config --version) && \
    uv pip install --no-cache-dir "GDAL==${GDAL_VERSION}"

# Install the package with all extras
RUN uv pip install -e ".[raster]"

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
