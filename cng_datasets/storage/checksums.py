"""Sizes and checksums of a dataset's outputs, for its STAC assets (issue #340).

Portolan asks every single-file asset for ``file:size`` and ``file:checksum``
(PORTO-CORE-028), the checksum as a multihash (PORTO-CORE-029), and its data
pass checks both against the bytes (PORTO-CORE-030). cng-datasets does not
write STAC; data-workflows does. So the workflow's last step writes one JSON
per dataset that a STAC author merges into the assets:

.. code-block:: json

    {
      "bucket": "public-census",
      "generator": "cng-datasets 0.15.0",
      "created": "2026-11-18T20:00:00Z",
      "files": {
        "census-2024/county.parquet": {"file:size": 1234, "file:checksum": "1220...",
                                       "created": "2026-11-18T19:40:12Z"},
        "census-2024/county/hex/h0=577199624117288959/data_0.parquet": {...}
      },
      "prefixes": {
        "census-2024/county/hex/": {"file:size": 98765, "file_count": 122}
      }
    }

Keys are object keys relative to the bucket. Each ``files`` value can be
merged into a STAC asset as it is: ``created`` is the object's last-modified
time. A hex directory is one STAC asset with a glob href, which a per-file
checksum cannot describe, so each prefix asked for also gets a total size and
file count under ``prefixes``, with no checksum.

The checksums are taken from the stored objects, read back after every step
has finished, rather than from local files as they are written. One code path
then covers every writer (convert, PMTiles, hex, COG), the checksum is of the
bytes a reader downloads, and the same command backfills datasets built before
it existed.
"""

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

# multiformats code for sha2-256, and its digest length
SHA2_256 = 0x12
_SHA2_256_LENGTH = 32
_BLOCK = 8 * 1024 * 1024
DEFAULT_WORKERS = 8


def sha256_multihash(digest: bytes) -> str:
    """A sha2-256 digest as a hex multihash, the form ``file:checksum`` takes.

    Both the code (0x12) and the length (32) are below 0x80, so each is one
    varint byte: the result is ``"1220"`` followed by the hex digest.
    """
    if len(digest) != _SHA2_256_LENGTH:
        raise ValueError(f"sha2-256 digest must be {_SHA2_256_LENGTH} bytes, got {len(digest)}")
    return bytes([SHA2_256, _SHA2_256_LENGTH]).hex() + digest.hex()


def file_entry(blocks: Iterable[bytes]) -> Dict:
    """``file:size`` and ``file:checksum`` of the bytes in ``blocks``."""
    h = hashlib.sha256()
    size = 0
    for block in blocks:
        h.update(block)
        size += len(block)
    return {"file:size": size, "file:checksum": sha256_multihash(h.digest())}


def _split_s3(url: str) -> Tuple[str, str]:
    rest = url[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise ValueError(f"No bucket in {url!r}")
    return bucket, key


def s3_client():
    """A boto3 client for the endpoint the generated jobs set in the environment.

    The jobs set ``AWS_S3_ENDPOINT`` (host only) and ``AWS_HTTPS``; credentials
    come from the usual ``AWS_*`` variables, which boto3 reads itself.
    """
    import boto3
    endpoint = os.environ.get("AWS_S3_ENDPOINT")
    endpoint_url = None
    if endpoint:
        if "://" in endpoint:
            endpoint_url = endpoint
        else:
            https = os.environ.get("AWS_HTTPS", "TRUE").upper() not in ("FALSE", "0", "NO")
            endpoint_url = f"{'https' if https else 'http'}://{endpoint}"
    return boto3.client("s3", endpoint_url=endpoint_url,
                        region_name=os.environ.get("AWS_REGION", "us-east-1"))


class _Store:
    """Lists and reads either one S3 bucket or the local filesystem."""

    def __init__(self, bucket: Optional[str], client=None):
        self.bucket = bucket
        self._client = client

    @property
    def client(self):
        if self._client is None:
            self._client = s3_client()
        return self._client

    def expand(self, target: str) -> List[Tuple[str, datetime]]:
        """``(key, last modified)`` of each object (or local path) a target names.

        A target ending in ``/`` is a prefix (a directory, locally) and names
        every object under it. Anything else names one object, which must exist.
        """
        if self.bucket is None:
            if target.endswith("/") or os.path.isdir(target):
                paths = sorted(
                    os.path.join(root, f)
                    for root, _, files in os.walk(target) for f in files)
                if not paths:
                    raise ValueError(f"No files under {target!r}")
            elif os.path.isfile(target):
                paths = [target]
            else:
                raise ValueError(f"No such file: {target!r}")
            return [(p, datetime.fromtimestamp(os.path.getmtime(p), timezone.utc))
                    for p in paths]

        _, key = _split_s3(target)
        if key.endswith("/") or not key:
            found = []
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=key):
                found.extend((obj["Key"], obj["LastModified"])
                             for obj in page.get("Contents", []))
            found = sorted(f for f in found if not f[0].endswith("/"))
            if not found:
                raise ValueError(f"No objects under {target!r}")
            return found
        try:
            head = self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            raise ValueError(f"Cannot read {target!r}: {e}") from e
        return [(key, head["LastModified"])]

    def blocks(self, key: str) -> Iterator[bytes]:
        if self.bucket is None:
            with open(key, "rb") as f:
                while True:
                    block = f.read(_BLOCK)
                    if not block:
                        return
                    yield block
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            yield from body.iter_chunks(_BLOCK)
        finally:
            body.close()


def _iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_prefix(target: str) -> bool:
    if target.startswith("s3://"):
        key = _split_s3(target)[1]
        return not key or key.endswith("/")
    return target.endswith("/") or os.path.isdir(target)


def compute_checksums(targets: List[str], workers: int = DEFAULT_WORKERS,
                      client=None, exclude: Iterable[str] = ()) -> Dict:
    """The checksum document for a list of objects and prefixes.

    Args:
        targets: ``s3://bucket/key`` URLs, all in one bucket, or local paths.
            One ending in ``/`` is a prefix.
        workers: Objects read at once.
        client: A boto3 S3 client, by default one from the environment.
        exclude: Keys (or paths) to leave out, such as the document itself.
    """
    if not targets:
        raise ValueError("No files to checksum")
    remote = [t.startswith("s3://") for t in targets]
    if any(remote) and not all(remote):
        raise ValueError("Targets must be all s3:// URLs or all local paths")
    bucket = None
    if all(remote):
        buckets = {_split_s3(t)[0] for t in targets}
        if len(buckets) != 1:
            raise ValueError(f"Targets span several buckets: {sorted(buckets)}")
        bucket = buckets.pop()
    store = _Store(bucket, client)

    skip = set(exclude)
    modified: Dict[str, datetime] = {}
    prefix_keys: Dict[str, List[str]] = {}
    for target in targets:
        listed = [(k, t) for k, t in store.expand(target) if k not in skip]
        for key, t in listed:
            modified.setdefault(key, t)
        if _is_prefix(target):
            name = _split_s3(target)[1] if bucket is not None else target
            prefix_keys[name] = [k for k, _ in listed]
    keys = list(modified)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        entries = list(pool.map(lambda k: file_entry(store.blocks(k)), keys))
    files = {}
    for key, entry in zip(keys, entries):
        files[key] = {**entry, "created": _iso(modified[key])}

    from .. import __version__
    doc = {
        "generator": f"cng-datasets {__version__}",
        "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": files,
    }
    if prefix_keys:
        doc["prefixes"] = {
            name: {"file:size": sum(files[k]["file:size"] for k in ks), "file_count": len(ks)}
            for name, ks in prefix_keys.items()
        }
    if bucket is not None:
        doc = {"bucket": bucket, **doc}
    return doc


def write_checksums(targets: List[str], output: str, workers: int = DEFAULT_WORKERS,
                    client=None) -> Dict:
    """Compute the checksum document and write it to ``output`` (local or s3://)."""
    if output.startswith("s3://"):
        out_bucket, out_key = _split_s3(output)
        exclude = [out_key]
    else:
        out_bucket, out_key, exclude = None, None, [output]
    doc = compute_checksums(targets, workers=workers, client=client, exclude=exclude)
    if doc.get("bucket") not in (None, out_bucket) and out_bucket is not None:
        raise ValueError(f"Output bucket {out_bucket!r} is not the files' bucket {doc['bucket']!r}")
    body = (json.dumps(doc, indent=2) + "\n").encode()
    if out_bucket is None:
        with open(output, "wb") as f:
            f.write(body)
    else:
        (client or s3_client()).put_object(
            Bucket=out_bucket, Key=out_key, Body=body, ContentType="application/json")
    return doc
