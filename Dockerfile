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
RUN uv pip install -e "." pytest pytest-timeout pytest-mock

# Point PROJ CLI tools to the highest-versioned proj.db on the system.
# The gdal:ubuntu-full image ships GDAL 3.13 with a compatible PROJ (requires
# DATABASE.LAYOUT.VERSION.MINOR >= 7), but the Ubuntu system proj-data package
# installs an older proj.db that takes precedence. Find the best one and set
# PROJ_DATA/PROJ_LIB so both CLI gdalwarp and Python osgeo use it.
RUN python3 - <<'EOF'
import sqlite3, glob, os

def get_minor_version(db):
    try:
        row = sqlite3.connect(db).execute(
            "SELECT value FROM metadata WHERE key='DATABASE.LAYOUT.VERSION.MINOR'"
        ).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0

proj_dbs = glob.glob('/usr/**/proj.db', recursive=True) + \
           glob.glob('/opt/**/proj.db', recursive=True)
if not proj_dbs:
    raise RuntimeError("No proj.db found on the system")

best_db = max(proj_dbs, key=get_minor_version)
best_dir = os.path.dirname(best_db)
best_ver = get_minor_version(best_db)
print(f"Best proj.db: {best_db} (version {best_ver})")

target = '/usr/local/share/proj'
if os.path.islink(target) or os.path.exists(target):
    os.remove(target)
os.makedirs('/usr/local/share', exist_ok=True)
if best_dir != target:
    os.symlink(best_dir, target)
    print(f"Symlinked {target} -> {best_dir}")
else:
    print(f"{target} already contains best proj.db")
EOF
ENV PROJ_DATA=/usr/local/share/proj
ENV PROJ_LIB=/usr/local/share/proj

# Pre-install DuckDB extensions so pods don't need outbound internet access at runtime
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('INSTALL spatial'); con.execute('INSTALL h3 FROM community')"

# Set Python to run in unbuffered mode (recommended for containers)
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["cng-datasets", "--help"]
