"""Translate Kubernetes memory quantities into DuckDB's `memory_limit` spelling.

The two look alike and are not the same. Kubernetes writes a binary quantity as
`16Gi`; DuckDB's parser accepts only `KiB/MiB/GiB/TiB` (1024^i) or
`KB/MB/GB/TB` (1000^i) and rejects `16Gi` outright:

    Parser Error: Unknown unit for memory: 'gi'

Because the generator writes the k8s spelling and the merge/repartition steps
feed it straight to `SET memory_limit`, that mismatch killed every generated
merge job on its first statement — after the whole hex fan-out had run
(issue #217). Normalising in one place keeps the manifests readable (`13GiB`,
not a byte count) and keeps the consumers safe against a hand-edited env var.
"""

from typing import List, Optional, Tuple

# DuckDB stops at tera-, so a petabyte-scale request renders as thousands of
# TiB rather than failing. No pod has that much memory; the ladder is written
# this way so an out-of-range value degrades instead of erroring.
_BINARY: List[Tuple[str, int]] = [
    ("TiB", 2 ** 40), ("GiB", 2 ** 30), ("MiB", 2 ** 20), ("KiB", 2 ** 10),
]
_DECIMAL: List[Tuple[str, int]] = [
    ("TB", 10 ** 12), ("GB", 10 ** 9), ("MB", 10 ** 6), ("KB", 10 ** 3),
]

# Every suffix either spelling can present, mapped to its byte factor and the
# ladder it should be rendered back onto. Kubernetes' bare `K/M/G/T/P/E` are
# powers of ten, which is why they land on the decimal ladder and `Ki/Mi/...`
# do not.
_SUFFIXES = {
    "Ki": (2 ** 10, _BINARY), "Mi": (2 ** 20, _BINARY), "Gi": (2 ** 30, _BINARY),
    "Ti": (2 ** 40, _BINARY), "Pi": (2 ** 50, _BINARY), "Ei": (2 ** 60, _BINARY),
    "KiB": (2 ** 10, _BINARY), "MiB": (2 ** 20, _BINARY), "GiB": (2 ** 30, _BINARY),
    "TiB": (2 ** 40, _BINARY),
    "K": (10 ** 3, _DECIMAL), "M": (10 ** 6, _DECIMAL), "G": (10 ** 9, _DECIMAL),
    "T": (10 ** 12, _DECIMAL), "P": (10 ** 15, _DECIMAL), "E": (10 ** 18, _DECIMAL),
    "KB": (10 ** 3, _DECIMAL), "MB": (10 ** 6, _DECIMAL), "GB": (10 ** 9, _DECIMAL),
    "TB": (10 ** 12, _DECIMAL),
    "B": (1, _BINARY),
    "": (1, _BINARY),
}


def _render(total_bytes: int, ladder: List[Tuple[str, int]]) -> str:
    """The most readable exact spelling of *total_bytes* on *ladder*."""
    for suffix, factor in ladder:
        if total_bytes >= factor and total_bytes % factor == 0:
            return f"{total_bytes // factor}{suffix}"
    # Nothing divides evenly, which is what applying a fraction usually leaves.
    # Take the coarsest unit that still keeps two significant digits and round
    # *down*: a limit under what was asked for is safe, over it is not.
    for suffix, factor in ladder:
        if total_bytes // factor >= 10:
            return f"{total_bytes // factor}{suffix}"
    return f"{max(total_bytes, 1)}B"


def to_duckdb_memory_limit(quantity, fraction: float = 1.0) -> Optional[str]:
    """A DuckDB `memory_limit` value for the Kubernetes quantity *quantity*.

    `to_duckdb_memory_limit("16Gi")` is `"16GiB"`; with *fraction* it is that
    share of the input, so `to_duckdb_memory_limit("16Gi", 0.85)` is `"13GiB"`.
    Sizing DuckDB's buffers below the pod's limit leaves room for everything
    allocated outside them — a limit equal to the cgroup's is an OOMKill
    waiting for the first busy partition.

    Anything already in DuckDB's spelling passes through unchanged, and
    anything unparseable (including DuckDB's own `-1` for "no limit") is
    returned as given rather than guessed at, so this can be applied
    defensively to a value a human may have set by hand.
    """
    if quantity is None:
        return None
    text = str(quantity).strip()
    if not text:
        return text

    for suffix in sorted(_SUFFIXES, key=len, reverse=True):
        if suffix and text.endswith(suffix):
            number, (factor, ladder) = text[:-len(suffix)].strip(), _SUFFIXES[suffix]
            break
    else:
        number, (factor, ladder) = text, _SUFFIXES[""]

    try:
        value = float(number)
    except ValueError:
        return text
    if value <= 0:
        return text

    return _render(int(value * factor * fraction), ladder)
