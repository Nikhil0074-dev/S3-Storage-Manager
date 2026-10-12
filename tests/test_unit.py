"""Offline unit tests (no S3 needed):  pytest -q"""
import json
from pathlib import Path

import pytest

from storage_manager import (
    StorageManager, categorize, parse_kv, human_size, generate_samples, MAX_PRESIGN_SECONDS,
)


@pytest.mark.parametrize("name,cat", [
    ("a.PNG", "images"), ("b.pdf", "documents"), ("c.csv", "data"),
    ("d.mp3", "audio"), ("e.zip", "archives"), ("f.py", "code"), ("g.unknown", "misc"),
])
def test_categorize(name, cat):
    assert categorize(Path(name)) == cat


def test_build_key_layout():
    assert StorageManager.build_key("x.csv", "private", "data") == "private/data/x.csv"
    assert StorageManager.build_key("x.csv", "public", "data", "/2026/q1/") == "public/data/2026/q1/x.csv"


def test_build_key_strips_path_traversal():
    assert StorageManager.build_key("../../etc/passwd", "private", "misc") == "private/misc/passwd"


def test_build_key_bad_visibility():
    with pytest.raises(ValueError):
        StorageManager.build_key("x", "secret", "data")


def test_parse_kv():
    assert parse_kv(["a=1", "b=x=y"], "tag") == {"a": "1", "b": "x=y"}
    with pytest.raises(ValueError):
        parse_kv(["novalue"], "tag")


def test_public_policy_only_exposes_public_prefix():
    pol = StorageManager.public_read_policy("bkt")
    stmt = pol["Statement"][0]
    assert stmt["Action"] == ["s3:GetObject"]
    assert stmt["Resource"] == ["arn:aws:s3:::bkt/public/*"]
    json.dumps(pol)


def test_human_size():
    assert human_size(10) == "10 B" and human_size(2048) == "2.0 KB"


def test_samples(tmp_path):
    files = generate_samples(str(tmp_path))
    assert len(files) == 6 and all(f.exists() for f in files)


def test_presign_limits():
    from storage_manager import Settings
    mgr = StorageManager(Settings(bucket="b", endpoint_url="http://localhost:1", access_key="a", secret_key="b"))
    for bad in (0, -5, MAX_PRESIGN_SECONDS + 1):
        with pytest.raises(ValueError):
            mgr.presign("k", expires=bad, method="put")
    assert "X-Amz-Expires=60" in mgr.presign("k", expires=60, method="put")
