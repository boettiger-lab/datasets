"""Tests for bucket CORS configuration (issues #35, #79, #87)."""

import json

import pytest

from cng_datasets.storage.s3 import CORS_EXPOSE_HEADERS, S3Manager

# Headers a browser must be allowed to read cross-origin for HTTP Range-based
# rendering of PMTiles and COGs. Accept-Ranges/Content-Range are not
# CORS-safelisted, so they must be exposed explicitly.
REQUIRED_RANGE_HEADERS = {
    "ETag",
    "Content-Length",
    "Content-Type",
    "Accept-Ranges",
    "Content-Range",
}


class _FakeS3Client:
    """Captures the CORSConfiguration handed to put_bucket_cors."""

    def __init__(self):
        self.cors_configuration = None

    def put_bucket_cors(self, Bucket=None, CORSConfiguration=None):  # noqa: N803
        self.cors_configuration = CORSConfiguration


@pytest.mark.timeout(5)
def test_expose_headers_constant_covers_range_headers():
    assert REQUIRED_RANGE_HEADERS.issubset(set(CORS_EXPOSE_HEADERS))


@pytest.mark.timeout(5)
def test_s3manager_configure_cors_exposes_range_headers():
    """The boto3 path (storage cors) must expose the range headers (#35)."""
    manager = S3Manager(endpoint_url="https://example.com")
    fake = _FakeS3Client()
    manager._client = fake  # bypass the lazy boto3 client

    manager.configure_cors("my-bucket")

    rule = fake.cors_configuration["CORSRules"][0]
    assert REQUIRED_RANGE_HEADERS.issubset(set(rule["ExposeHeaders"]))


@pytest.mark.timeout(5)
def test_setup_public_bucket_exposes_range_headers(monkeypatch):
    """The aws-cli path (setup-bucket) must expose the range headers (#79, #87)."""
    from cng_datasets.storage import setup_bucket

    captured = {}

    class _Result:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, *args, **kwargs):
        if "put-bucket-cors" in cmd:
            captured["cors"] = cmd[cmd.index("--cors-configuration") + 1]
        return _Result()

    monkeypatch.setattr(setup_bucket.subprocess, "run", fake_run)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    ok = setup_bucket.setup_public_bucket(
        bucket_name="my-bucket", endpoint="example.com", verbose=False
    )
    assert ok
    cors = json.loads(captured["cors"])
    expose = cors["CORSRules"][0]["ExposeHeaders"]
    assert REQUIRED_RANGE_HEADERS.issubset(set(expose))


@pytest.mark.timeout(5)
def test_both_paths_share_one_expose_header_list():
    """Both CORS code paths reference the same constant so they cannot drift."""
    from cng_datasets.storage import setup_bucket

    assert setup_bucket.CORS_EXPOSE_HEADERS is CORS_EXPOSE_HEADERS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
