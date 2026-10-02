"""N-D cube (time, lat, lon) → H3 hex processing (issue #181)."""

from .cftime import decode_cf_time
from .processor import MdimProcessor
from .reader import CubeSource

__all__ = ["CubeSource", "MdimProcessor", "decode_cf_time"]
