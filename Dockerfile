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

# Create virtual environment with access to system packages (for GDAL)
RUN uv venv /opt/venv --system-site-packages
ENV PATH="/opt/venv/bin:$PATH"

# Pin NumPy to 1.x for compatibility with system GDAL
RUN uv pip install "numpy<2" pip

# Install the package (GDAL already available from system)
RUN uv pip install -e "."

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
