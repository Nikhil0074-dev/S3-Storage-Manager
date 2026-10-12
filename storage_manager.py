#!/usr/bin/env python3
"""
storage_manager.py - S3-compatible storage management utility.

Works unchanged against:
  * MinIO        (local, free, recommended for testing access policies)
  * LocalStack   (local, free tier)
  * AWS S3       (Free Tier)

Features
  - upload / upload-dir / list / download / delete / info
  - automatic categorisation into folders:  <visibility>/<category>/[subfolder/]<file>
  - custom object metadata (x-amz-meta-*) and object tags
  - bucket policy management: public-read (only public/ prefix) vs private
  - time-limited pre-signed URLs (GET and PUT)
  - anonymous access checks and an end-to-end acceptance `demo`

Run `python storage_manager.py --help` for the full command list.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import mimetypes
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import boto3
import requests
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    ParamValidationError,
)

try:  # optional: load variables from a .env file
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

log = logging.getLogger("storage_manager")

MAX_PRESIGN_SECONDS = 7 * 24 * 3600  # SigV4 hard limit (7 days)
MAX_TAGS = 10  # S3 limit per object
PUBLIC_PREFIX = "public/"
PRIVATE_PREFIX = "private/"

# --------------------------------------------------------------------------- #
# Categorisation
# --------------------------------------------------------------------------- #
CATEGORY_MAP: Dict[str, str] = {
    "images": ".jpg .jpeg .png .gif .bmp .webp .svg .tiff",
    "documents": ".pdf .doc .docx .txt .md .rtf .odt",
    "data": ".csv .json .xml .xlsx .xls .parquet .yaml .yml",
    "audio": ".mp3 .wav .flac .aac .ogg",
    "video": ".mp4 .mov .avi .mkv .webm",
    "archives": ".zip .tar .gz .tgz .7z .rar",
    "code": ".py .js .ts .java .go .rs .sh .html .css",
}
EXT_TO_CATEGORY = {
    ext: cat for cat, exts in CATEGORY_MAP.items() for ext in exts.split()
}


def categorize(path: Path) -> str:
    """Return a category folder name for a file based on its extension."""
    return EXT_TO_CATEGORY.get(path.suffix.lower(), "misc")


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Settings:
    bucket: str
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # None => real AWS S3
    access_key: Optional[str] = None
    secret_key: Optional[str] = None

    @classmethod
    def from_env(cls) -> "Settings":
        if load_dotenv:
            load_dotenv()
        return cls(
            bucket=os.getenv("S3_BUCKET", "storage-demo-bucket"),
            region=os.getenv("AWS_REGION", "us-east-1"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            access_key=os.getenv("AWS_ACCESS_KEY_ID") or None,
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ascii_safe(value: str) -> str:
    """S3 user metadata must be US-ASCII."""
    return value.encode("ascii", "ignore").decode("ascii")


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def parse_kv(pairs: Optional[List[str]], what: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in pairs or []:
        if "=" not in item:
            raise ValueError(f"Invalid {what} '{item}'. Use key=value.")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid {what} '{item}': empty key.")
        out[k] = v.strip()
    return out


# --------------------------------------------------------------------------- #
# Storage manager
# --------------------------------------------------------------------------- #
class StorageManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.bucket = settings.bucket
        addressing = "path" if settings.endpoint_url else "auto"
        base_cfg = dict(
            signature_version="s3v4",
            s3={"addressing_style": addressing},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        )
        common = dict(region_name=settings.region, endpoint_url=settings.endpoint_url)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            config=Config(**base_cfg),
            **common,
        )
        # Anonymous client: never signs requests. Used to prove what the
        # outside world (no credentials) can or cannot read.
        anon_cfg = dict(base_cfg, signature_version=UNSIGNED)
        self.anon = boto3.client("s3", config=Config(**anon_cfg), **common)

    # ----------------------------- bucket ---------------------------------- #
    def ensure_bucket(self) -> bool:
        """Create the bucket if needed and make it private by default.
        Returns True if the bucket was created."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            return False
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ("404", "NoSuchBucket", "NotFound"):
                raise
        kwargs = {"Bucket": self.bucket}
        if self.settings.region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": self.settings.region
            }
        self.s3.create_bucket(**kwargs)
        log.info("Created bucket '%s'", self.bucket)
        self._set_block_public_access(block=True)
        return True

    def _set_block_public_access(self, block: bool) -> None:
        """AWS only: toggle Block Public Access. Emulators may not implement it."""
        cfg = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": block,
            "RestrictPublicBuckets": block,
        }
        try:
            self.s3.put_public_access_block(
                Bucket=self.bucket, PublicAccessBlockConfiguration=cfg
            )
        except ClientError as exc:
            log.debug("PublicAccessBlock not applied: %s", exc.response["Error"]["Code"])

    # ----------------------------- policies -------------------------------- #
    @staticmethod
    def public_read_policy(bucket: str, prefix: str = PUBLIC_PREFIX) -> dict:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadOnlyForPublicPrefix",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{prefix}*"],
                }
            ],
        }

    def set_policy(self, mode: str) -> Optional[dict]:
        """mode = 'public-read' (only public/ prefix) or 'private' (no policy)."""
        if mode == "public-read":
            self._set_block_public_access(block=False)
            policy = self.public_read_policy(self.bucket)
            self.s3.put_bucket_policy(Bucket=self.bucket, Policy=json.dumps(policy))
            return policy
        if mode == "private":
            try:
                self.s3.delete_bucket_policy(Bucket=self.bucket)
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("NoSuchBucketPolicy", "404"):
                    raise
            self._set_block_public_access(block=True)
            return None
        raise ValueError("mode must be 'public-read' or 'private'")

    def get_policy(self) -> Optional[dict]:
        try:
            raw = self.s3.get_bucket_policy(Bucket=self.bucket)["Policy"]
            return json.loads(raw)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchBucketPolicy", "404"):
                return None
            raise

    # ----------------------------- upload ---------------------------------- #
    @staticmethod
    def build_key(
        filename: str,
        visibility: str,
        category: str,
        subfolder: Optional[str] = None,
    ) -> str:
        if visibility not in ("public", "private"):
            raise ValueError("visibility must be 'public' or 'private'")
        parts = [visibility, category]
        if subfolder:
            parts.extend(p for p in subfolder.strip("/").split("/") if p)
        parts.append(Path(filename).name)
        return "/".join(parts)

    def upload_file(
        self,
        path: str,
        visibility: str = "private",
        category: Optional[str] = None,
        subfolder: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"File not found: {path}")
        category = category or categorize(p)
        key = self.build_key(p.name, visibility, category, subfolder)

        meta = {
            "original-name": ascii_safe(p.name),
            "category": category,
            "visibility": visibility,
            "sha256": sha256_file(p),
            "uploaded-at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        meta.update({k.lower(): ascii_safe(v) for k, v in (metadata or {}).items()})

        all_tags = {"category": category, "visibility": visibility}
        all_tags.update(tags or {})
        if len(all_tags) > MAX_TAGS:
            raise ValueError(f"S3 allows at most {MAX_TAGS} tags per object.")

        extra = {
            "ContentType": mimetypes.guess_type(p.name)[0] or "application/octet-stream",
            "Metadata": meta,
            "Tagging": urlencode(all_tags),
        }
        self.s3.upload_file(str(p), self.bucket, key, ExtraArgs=extra)
        return key

    def upload_dir(self, directory: str, visibility: str = "private", **kw) -> List[str]:
        root = Path(directory)
        if not root.is_dir():
            raise NotADirectoryError(directory)
        keys = []
        for f in sorted(root.iterdir()):
            if f.is_file() and not f.name.startswith("."):
                keys.append(self.upload_file(str(f), visibility=visibility, **kw))
        return keys

    # ------------------------------ list ----------------------------------- #
    def get_tags(self, key: str) -> Dict[str, str]:
        resp = self.s3.get_object_tagging(Bucket=self.bucket, Key=key)
        return {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}

    def set_tags(self, key: str, tags: Dict[str, str]) -> None:
        if len(tags) > MAX_TAGS:
            raise ValueError(f"S3 allows at most {MAX_TAGS} tags per object.")
        self.s3.put_object_tagging(
            Bucket=self.bucket,
            Key=key,
            Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]},
        )

    def iter_objects(self, prefix: str = "") -> Iterator[dict]:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            yield from page.get("Contents", [])

    def list_objects(
        self,
        prefix: str = "",
        visibility: Optional[str] = None,
        category: Optional[str] = None,
        tag_filter: Optional[Dict[str, str]] = None,
        with_tags: bool = False,
    ) -> List[dict]:
        rows = []
        for obj in self.iter_objects(prefix):
            key = obj["Key"]
            parts = key.split("/")
            vis = parts[0] if len(parts) > 1 else "-"
            cat = parts[1] if len(parts) > 2 else "-"
            if visibility and vis != visibility:
                continue
            if category and cat != category:
                continue
            tags: Dict[str, str] = {}
            if with_tags or tag_filter:
                tags = self.get_tags(key)
                if tag_filter and any(tags.get(k) != v for k, v in tag_filter.items()):
                    continue
            rows.append(
                {
                    "key": key,
                    "size": obj["Size"],
                    "modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
                    "visibility": vis,
                    "category": cat,
                    "tags": tags,
                }
            )
        return rows

    def info(self, key: str) -> dict:
        head = self.s3.head_object(Bucket=self.bucket, Key=key)
        return {
            "key": key,
            "size": head["ContentLength"],
            "content_type": head.get("ContentType"),
            "etag": head.get("ETag", "").strip('"'),
            "last_modified": head["LastModified"].isoformat(),
            "metadata": head.get("Metadata", {}),
            "tags": self.get_tags(key),
        }

    # ---------------------------- download/delete -------------------------- #
    def download_file(self, key: str, dest: str) -> Path:
        out = Path(dest)
        if out.is_dir() or str(dest).endswith(os.sep):
            out = out / Path(key).name
        out.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket, key, str(out))
        return out

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    # ----------------------------- pre-signed ------------------------------ #
    def presign(self, key: str, expires: int = 300, method: str = "get") -> str:
        if not 1 <= expires <= MAX_PRESIGN_SECONDS:
            raise ValueError(f"expires must be between 1 and {MAX_PRESIGN_SECONDS} seconds")
        if method == "get":
            if not self.exists(key):
                raise FileNotFoundError(f"Object not found: {key}")
            op = "get_object"
        elif method == "put":
            op = "put_object"
        else:
            raise ValueError("method must be 'get' or 'put'")
        return self.s3.generate_presigned_url(
            op, Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires
        )

    # ----------------------------- access checks --------------------------- #
    def anonymous_status(self, key: str) -> int:
        """HTTP status code when fetching the object with NO credentials."""
        url = self.anon.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=60
        )
        return self.http_status(url)

    @staticmethod
    def http_status(url: str) -> int:
        with requests.get(url, timeout=15, stream=True) as resp:
            return resp.status_code


# --------------------------------------------------------------------------- #
# Sample data
# --------------------------------------------------------------------------- #
# 1x1 transparent PNG
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c6360000002000001e221bc330000000049454e44ae426082"
)


def generate_samples(directory: str) -> List[Path]:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    files = {
        "welcome.txt": "Hello from the S3 storage manager demo!\n",
        "notes.md": "# Notes\n\n- Private meeting notes\n- Do not share publicly\n",
        "sales.csv": "month,revenue\nJan,1200\nFeb,1500\nMar,1800\n",
        "config.json": json.dumps({"app": "demo", "version": 1}, indent=2) + "\n",
        "script.py": "print('hello world')\n",
    }
    created = []
    for name, content in files.items():
        p = d / name
        p.write_text(content, encoding="utf-8")
        created.append(p)
    png = d / "logo.png"
    png.write_bytes(_PNG)
    created.append(png)
    return created


# --------------------------------------------------------------------------- #
# End-to-end acceptance demo
# --------------------------------------------------------------------------- #
def run_demo(mgr: StorageManager, expires: int = 5) -> int:
    results: List[tuple] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    with tempfile.TemporaryDirectory() as tmp:
        samples = {p.name: p for p in generate_samples(tmp)}

        print("\n1. Preparing bucket")
        created = mgr.ensure_bucket()
        print(f"   bucket '{mgr.bucket}' {'created' if created else 'already exists'}")

        print("\n2. Uploading objects (public logo, private sales report)")
        pub_key = mgr.upload_file(
            str(samples["logo.png"]), visibility="public",
            tags={"project": "demo", "owner": "marketing"},
            metadata={"description": "Company logo"},
        )
        priv_key = mgr.upload_file(
            str(samples["sales.csv"]), visibility="private", subfolder="2026/q1",
            tags={"project": "demo", "confidential": "true"},
            metadata={"description": "Quarterly sales"},
        )
        print(f"   {pub_key}\n   {priv_key}")
        check("objects uploaded with expected folder layout",
              pub_key == "public/images/logo.png"
              and priv_key == "private/data/2026/q1/sales.csv")

        info = mgr.info(priv_key)
        check("metadata + tags stored",
              info["metadata"].get("description") == "Quarterly sales"
              and info["tags"].get("confidential") == "true")

        check("listing by category works",
              [r["key"] for r in mgr.list_objects(category="images")] == [pub_key])
        check("listing by tag works",
              [r["key"] for r in mgr.list_objects(tag_filter={"confidential": "true"})] == [priv_key])

        print("\n3. Access policy: public-read (public/ prefix only)")
        mgr.set_policy("public-read")
        time.sleep(1)
        s = mgr.anonymous_status(pub_key)
        check("anonymous GET of public object -> 200", s == 200, f"HTTP {s}")
        s = mgr.anonymous_status(priv_key)
        check("anonymous GET of private object -> denied", s in (401, 403), f"HTTP {s}")

        print(f"\n4. Pre-signed URL (expires in {expires}s)")
        url = mgr.presign(priv_key, expires=expires)
        q = parse_qs(urlparse(url).query)
        check("URL carries signed expiry (X-Amz-Expires)",
              q.get("X-Amz-Expires") == [str(expires)] and "X-Amz-Signature" in q,
              f"X-Amz-Expires={q.get('X-Amz-Expires', ['?'])[0]}")
        s = mgr.http_status(url)
        check("pre-signed GET of private object -> 200", s == 200, f"HTTP {s}")
        body = requests.get(url, timeout=15).content
        check("downloaded content is correct", body == samples["sales.csv"].read_bytes())
        tampered = url.replace(priv_key, "private/data/2026/q1/other.csv")
        s = mgr.http_status(tampered)
        check("tampered pre-signed URL rejected", s in (403, 404), f"HTTP {s}")
        print(f"   waiting {expires + 2}s for the URL to expire ...")
        time.sleep(expires + 2)
        s = mgr.http_status(url)
        check("expired pre-signed URL -> denied", s in (401, 403), f"HTTP {s}")

        print("\n5. Switching bucket to fully private")
        mgr.set_policy("private")
        time.sleep(1)
        s = mgr.anonymous_status(pub_key)
        check("anonymous GET of formerly public object -> denied", s in (401, 403), f"HTTP {s}")

        print("\n6. Authenticated download + integrity check")
        out = mgr.download_file(priv_key, str(Path(tmp) / "dl" / "sales.csv"))
        check("authenticated download matches original",
              sha256_file(out) == sha256_file(samples["sales.csv"]))

        print("\n7. Cleanup")
        for k in (pub_key, priv_key):
            mgr.delete(k)
        check("objects deleted", not mgr.exists(pub_key) and not mgr.exists(priv_key))

    failed = [r for r in results if not r[1]]
    print(f"\nResult: {len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="storage_manager.py",
        description="Upload, organise, secure and share files in S3-compatible storage.",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    p.add_argument("--bucket", help="override S3_BUCKET")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create the bucket (private by default)")

    s = sub.add_parser("samples", help="generate sample files")
    s.add_argument("--dir", default="sample_files")

    s = sub.add_parser("upload", help="upload one file")
    s.add_argument("file")
    s.add_argument("--visibility", choices=["public", "private"], default="private")
    s.add_argument("--category", help="override auto category")
    s.add_argument("--subfolder", help="extra folder path, e.g. 2026/q1")
    s.add_argument("--tag", action="append", metavar="K=V")
    s.add_argument("--meta", action="append", metavar="K=V")

    s = sub.add_parser("upload-dir", help="upload every file in a directory")
    s.add_argument("directory")
    s.add_argument("--visibility", choices=["public", "private"], default="private")
    s.add_argument("--subfolder")
    s.add_argument("--tag", action="append", metavar="K=V")
    s.add_argument("--meta", action="append", metavar="K=V")

    s = sub.add_parser("list", help="list objects")
    s.add_argument("--prefix", default="")
    s.add_argument("--visibility", choices=["public", "private"])
    s.add_argument("--category")
    s.add_argument("--tag", action="append", metavar="K=V", help="filter by tag")
    s.add_argument("--show-tags", action="store_true")
    s.add_argument("--json", action="store_true", help="JSON output")

    s = sub.add_parser("info", help="show metadata and tags of an object")
    s.add_argument("key")

    s = sub.add_parser("tag", help="replace tags of an object")
    s.add_argument("key")
    s.add_argument("--tag", action="append", metavar="K=V", required=True)

    s = sub.add_parser("download", help="download an object (authenticated)")
    s.add_argument("key")
    s.add_argument("dest", nargs="?", default="downloads/")

    s = sub.add_parser("delete", help="delete an object")
    s.add_argument("key")

    s = sub.add_parser("policy", help="show or set the bucket access policy")
    s.add_argument("mode", nargs="?", choices=["public-read", "private", "show"], default="show")

    s = sub.add_parser("presign", help="generate a time-limited pre-signed URL")
    s.add_argument("key")
    s.add_argument("--expires", type=int, default=300, help="seconds (1..604800)")
    s.add_argument("--method", choices=["get", "put"], default="get")

    s = sub.add_parser("check-access", help="test anonymous (no credentials) access")
    s.add_argument("key")

    s = sub.add_parser("demo", help="run the full end-to-end acceptance demo")
    s.add_argument("--expires", type=int, default=5, help="pre-signed URL lifetime for the test")
    return p


def print_table(rows: List[dict], show_tags: bool) -> None:
    if not rows:
        print("(no objects)")
        return
    width = max(len(r["key"]) for r in rows)
    header = f"{'KEY':<{width}}  {'SIZE':>9}  {'MODIFIED':<19}" + ("  TAGS" if show_tags else "")
    print(header)
    print("-" * len(header))
    for r in rows:
        line = f"{r['key']:<{width}}  {human_size(r['size']):>9}  {r['modified']:<19}"
        if show_tags:
            line += "  " + ",".join(f"{k}={v}" for k, v in sorted(r["tags"].items()))
        print(line)
    print(f"\n{len(rows)} object(s)")


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if args.command == "samples":
        for f in generate_samples(args.dir):
            print(f"created {f}")
        return 0

    settings = Settings.from_env()
    if args.bucket:
        settings = Settings(**{**settings.__dict__, "bucket": args.bucket})
    mgr = StorageManager(settings)

    try:
        if args.command == "init":
            created = mgr.ensure_bucket()
            print(f"Bucket '{mgr.bucket}' {'created' if created else 'already exists'}.")

        elif args.command == "upload":
            key = mgr.upload_file(
                args.file, args.visibility, args.category, args.subfolder,
                parse_kv(args.tag, "tag"), parse_kv(args.meta, "metadata"),
            )
            print(f"Uploaded -> s3://{mgr.bucket}/{key}")

        elif args.command == "upload-dir":
            keys = mgr.upload_dir(
                args.directory, args.visibility, subfolder=args.subfolder,
                tags=parse_kv(args.tag, "tag"), metadata=parse_kv(args.meta, "metadata"),
            )
            for k in keys:
                print(f"Uploaded -> s3://{mgr.bucket}/{k}")
            print(f"{len(keys)} file(s) uploaded.")

        elif args.command == "list":
            rows = mgr.list_objects(
                args.prefix, args.visibility, args.category,
                parse_kv(args.tag, "tag") or None, args.show_tags,
            )
            if args.json:
                print(json.dumps(rows, indent=2))
            else:
                print_table(rows, args.show_tags)

        elif args.command == "info":
            print(json.dumps(mgr.info(args.key), indent=2))

        elif args.command == "tag":
            mgr.set_tags(args.key, parse_kv(args.tag, "tag"))
            print(json.dumps(mgr.get_tags(args.key), indent=2))

        elif args.command == "download":
            print(f"Downloaded -> {mgr.download_file(args.key, args.dest)}")

        elif args.command == "delete":
            mgr.delete(args.key)
            print(f"Deleted {args.key}")

        elif args.command == "policy":
            if args.mode == "show":
                current = mgr.get_policy()
                print(json.dumps(current, indent=2) if current else "No bucket policy (private).")
            else:
                policy = mgr.set_policy(args.mode)
                print(f"Policy set to '{args.mode}'.")
                if policy:
                    print(json.dumps(policy, indent=2))

        elif args.command == "presign":
            url = mgr.presign(args.key, args.expires, args.method)
            print(url)
            print(f"\n(valid for {args.expires}s, method={args.method.upper()})", file=sys.stderr)

        elif args.command == "check-access":
            status = mgr.anonymous_status(args.key)
            verdict = "PUBLIC (readable without credentials)" if status == 200 else "PROTECTED"
            print(f"Anonymous GET {args.key}: HTTP {status} -> {verdict}")

        elif args.command == "demo":
            return run_demo(mgr, args.expires)

        return 0

    except (ValueError, FileNotFoundError, NotADirectoryError, ParamValidationError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except NoCredentialsError:
        print("Error: no credentials found. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env.", file=sys.stderr)
        return 3
    except EndpointConnectionError:
        print(f"Error: cannot reach {settings.endpoint_url or 'AWS'}. Is the emulator running?", file=sys.stderr)
        return 4
    except ClientError as exc:
        err = exc.response.get("Error", {})
        print(f"S3 error [{err.get('Code')}]: {err.get('Message')}", file=sys.stderr)
        return 5
    except requests.RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    sys.exit(main())
