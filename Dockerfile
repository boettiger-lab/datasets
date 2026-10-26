FROM ghcr.io/osgeo/gdal:ubuntu-full-latest

# Set working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    git \
    rclone \
    curl \
    unzip \
    7zip \
    make \
    g++ \
    libsqlite3-dev \
    zlib1g-dev \
    python3-dev \
    python3-numpy \
    libgeos-dev \
    libproj-dev \
    cmake \
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

# NumPy comes from the base image (Ubuntu 26.04: python3-numpy 2.x), which the
# system GDAL bindings are built against; do not shadow it in the venv.
# Also install pip so users can pip install instead of uv pip install.
RUN uv pip install pip

# Install the package with uv for fast resolution (GDAL already available from system).
# [raster] extras pulls in exactextract and rasterio for the raster→H3 pipeline (#84).
# Include pytest and pytest-timeout so tests can run inside the container without extra installs.
#
# The packages that link GDAL, PROJ or GEOS are built from source against the
# base image's libraries (#285). Their wheels each bundle a private copy, so the
# image held three GDALs, and raster/cog.py loaded two of them in one process.
# GDAL in this base links a private PROJ that has no headers, so pyproj links
# Ubuntu's libproj, which GDAL's own dependencies already load.
# The build constraint pins NumPy in the isolated build environments to the
# system NumPy; a newer one fails shapely's Cython build against older headers.
RUN echo "numpy==$(/usr/bin/python3 -c 'import numpy; print(numpy.__version__)')" > /tmp/build-constraints.txt \
    && uv pip install --build-constraint /tmp/build-constraints.txt \
    --no-binary rasterio --no-binary pyogrio --no-binary pyproj \
    --no-binary shapely --no-binary exactextract \
    -e ".[raster]" pytest pytest-timeout pytest-mock

# Fail the build if any binding carries a GDAL or GEOS other than the system one.
RUN python3 - <<'EOF'
import subprocess
from osgeo import gdal
import pyogrio, rasterio, shapely

gdal_version = subprocess.check_output(["gdal-config", "--version"], text=True).strip()
geos_version = subprocess.check_output(["geos-config", "--version"], text=True).strip()
found = {
    "osgeo": gdal.__version__,
    "rasterio": rasterio.__gdal_version__,
    "pyogrio": pyogrio.__gdal_version_string__,
}
print(f"gdal-config {gdal_version}: {found}")
print(f"geos-config {geos_version}: shapely {shapely.geos_version_string}")
# A development GDAL reports "3.14.0dev-<commit>" where gdal-config says "3.14.0dev".
bad = {k: v for k, v in found.items() if v.split("-")[0] != gdal_version}
if bad or shapely.geos_version_string != geos_version:
    raise SystemExit(f"bindings disagree with the system libraries: {bad}")
EOF

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
