FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Set working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    git \
    rclone \
    curl \
    unzip \
    make \
    g++ \
    libsqlite3-dev \
    zlib1g-dev \
    python3-dev \
    python3-numpy \
    && rm -rf /var/lib/apt/lists/*

# Build tippecanoe 2.79.0 from source
RUN git clone --depth 1 --branch 2.79.0 https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && make -C /tmp/tippecanoe -j$(nproc) \
    && make -C /tmp/tippecanoe install \
    && rm -rf /tmp/tippecanoe

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy

# Copy package files
COPY pyproject.toml .
COPY README.md .
COPY cng_datasets/ ./cng_datasets/

# Create virtual environment with access to system packages (for GDAL)
RUN uv venv /opt/venv --system-site-packages
ENV PATH="/opt/venv/bin:$PATH"

# Pin NumPy to 1.x for compatibility with system GDAL
# Also install pip so users can pip install instead of uv pip install.
RUN uv pip install "numpy<2" pip

# Install the package with uv for fast resolution (GDAL already available from system)
# Include pytest and pytest-timeout so tests can run inside the container without extra installs
RUN uv pip install -e "." pytest pytest-timeout

# Fix PROJ_LIB path: gdal:ubuntu-full installs proj.db at a non-standard location
# (e.g. /usr/local/gdal-internal/share/proj). Create a symlink at the canonical
# path so that PROJ_LIB=/usr/local/share/proj always resolves correctly.
RUN mkdir -p /usr/local/share && \
    PROJ_DIR=$(find /usr -name "proj.db" 2>/dev/null | head -1 | xargs -r dirname) && \
    [ -n "${PROJ_DIR}" ] && [ "${PROJ_DIR}" != "/usr/local/share/proj" ] && \
    ln -sfn "${PROJ_DIR}" /usr/local/share/proj || true

# Pre-install DuckDB extensions so pods don't need outbound internet access at runtime
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('INSTALL spatial'); con.execute('INSTALL h3 FROM community')"

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
