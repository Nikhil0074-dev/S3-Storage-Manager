# S3 Storage Manager

A Python (boto3) utility for uploading, organising, securing and sharing files in **S3-compatible storage**. The same code runs unchanged against **MinIO**, **LocalStack**, **AWS S3 (Free Tier)** or the **moto** emulator.

| Capability | Command |
|---|---|
| Create bucket (private by default) | `init` |
| Upload file / directory, auto-categorised into folders | `upload`, `upload-dir` |
| Custom metadata + object tags | `--meta k=v`, `--tag k=v`, `tag`, `info` |
| List, filter by folder / category / tag | `list` |
| Download (authenticated) | `download` |
| Public-read vs private bucket policy | `policy public-read\|private\|show` |
| Time-limited pre-signed URLs (GET/PUT) | `presign` |
| Prove what anonymous users can read | `check-access` |
| Full end-to-end acceptance run | `demo` |

## Project layout

```
s3-storage-manager/
├── storage_manager.py   # the utility (library + CLI)
├── docker-compose.yml   # MinIO / LocalStack local emulators
├── .env.example         # credential + endpoint templates
├── requirements.txt
├── sample_files/        # sample data (regenerate with `samples`)
├── tests/test_unit.py   # offline unit tests
└── README.md
```

### Bucket folder structure

```
<bucket>/
├── public/                      # only this prefix can ever be made world-readable
│   └── images/logo.png
└── private/                     # never readable without credentials / pre-signed URL
    ├── data/2026/q1/sales.csv   # --subfolder 2026/q1
    ├── documents/notes.md
    └── code/script.py
```

Key pattern: `<visibility>/<category>/[subfolder/]<filename>`. The category is detected from the file extension (`images`, `documents`, `data`, `audio`, `video`, `archives`, `code`, `misc`) or set with `--category`.

Every object automatically gets **metadata** (`original-name`, `category`, `visibility`, `sha256`, `uploaded-at`, plus your own) and **tags** (`category`, `visibility`, plus your own, max 10).

### How access control works

1. The bucket is created **private** (no policy, Block Public Access on where supported).
2. `policy public-read` attaches a bucket policy allowing anonymous `s3:GetObject` **only on `public/*`**. Everything under `private/` stays locked.
3. For private files, `presign` creates a URL signed with your credentials that works only until `X-Amz-Expires` elapses. After that the storage service answers `403`.

---

## Step-by-step setup

### Step 1: Prerequisites

- Python 3.9+
- Docker + Docker Compose (only for MinIO/LocalStack) **or** an AWS account (for real S3)

### Step 2: Install

```bash
cd s3-storage-manager
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 3: storage backend

####  MinIO (local, free)

>  MinIO Inc. stopped publishing official community Docker images, so `docker pull minio/minio` no longer works. The compose file builds MinIO from the official source tag instead. The first build compiles Go code and takes a few minutes. I could not run Docker in the environment where this project was written, so if the build fails, use Option C, D or an image you trust (`MINIO_IMAGE=<image> docker compose up -d minio`).

```bash
docker compose build minio
docker compose up -d minio
```

- API: http://localhost:9000, console: http://localhost:9001 (login `minioadmin` / `minioadmin`)


### Step 4: Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and keep only the block for your chosen backend (the file contains ready-made blocks for A–D). `.env` is git-ignored, so never commit real keys.

For real AWS you may omit the key variables and use `aws configure` / `AWS_PROFILE` instead (boto3 picks them up automatically).

### Step 5: Create the bucket

```bash
python storage_manager.py init
# Bucket 'storage-demo-bucket' created.
```

### Step 6: Upload files

```bash
python storage_manager.py samples                 # creates sample_files/*

# one private file with tags, metadata and a custom sub-folder
python storage_manager.py upload sample_files/sales.csv \
    --subfolder 2026/q1 --tag project=demo --tag confidential=true --meta description="Quarterly sales"
# Uploaded -> s3://storage-demo-bucket/private/data/2026/q1/sales.csv

# one public file
python storage_manager.py upload sample_files/logo.png --visibility public --tag team=design

# a whole directory
python storage_manager.py upload-dir sample_files --tag project=demo --meta owner=alice
```

### Step 7: List, filter, inspect

```bash
python storage_manager.py list --show-tags
python storage_manager.py list --visibility public
python storage_manager.py list --category data
python storage_manager.py list --tag confidential=true
python storage_manager.py list --prefix private/data/2026/ --json
python storage_manager.py info private/data/2026/q1/sales.csv     # metadata + tags
python storage_manager.py tag private/data/2026/q1/sales.csv --tag reviewed=yes   # replaces the tag set
```

### Step 8: Test access policies (public read vs private)

```bash
# Bucket starts private: even the "public/" file is blocked
python storage_manager.py check-access public/images/logo.png
# Anonymous GET public/images/logo.png: HTTP 403 -> PROTECTED

python storage_manager.py policy public-read
python storage_manager.py check-access public/images/logo.png
# Anonymous GET ...: HTTP 200 -> PUBLIC (readable without credentials)
python storage_manager.py check-access private/data/2026/q1/sales.csv
# Anonymous GET ...: HTTP 403 -> PROTECTED     <- private stays locked

python storage_manager.py policy show             # print the active policy
python storage_manager.py policy private          # remove public access again
```

`check-access` uses an **unsigned** client, i.e. exactly what an anonymous browser would see. You can also verify manually:

```bash
curl -i http://localhost:9000/storage-demo-bucket/private/data/2026/q1/sales.csv   # -> 403 AccessDenied
```

### Step 9: Generate and test pre-signed URLs

```bash
python storage_manager.py presign private/data/2026/q1/sales.csv --expires 30
# http://localhost:9000/storage-demo-bucket/private/...?X-Amz-Expires=30&X-Amz-Signature=...
```

```bash
URL="$(python storage_manager.py presign private/data/2026/q1/sales.csv --expires 10 2>/dev/null)"
curl -s -o /dev/null -w "before expiry: %{http_code}\n" "$URL"     # 200
sleep 12
curl -s -o /dev/null -w "after expiry:  %{http_code}\n" "$URL"     # 403 (on MinIO / AWS)
```

Allowed range is 1 s to 604 800 s (7 days, the SigV4 maximum). Pre-signed **upload** URLs work too: `presign <key> --method put`, then `curl -X PUT --upload-file file.txt "$URL"`.

### Step 10: Download

```bash
python storage_manager.py download private/data/2026/q1/sales.csv downloads/
```

### Step 11: Run the full acceptance demo

```bash
python storage_manager.py demo --expires 5
```

It creates the bucket, uploads a public and a private object, applies and revokes the public-read policy, checks anonymous access, generates a 5-second pre-signed URL, waits for it to expire, verifies download integrity (SHA-256) and cleans up. It exits non-zero if any check fails.

Expected output on MinIO/AWS (all PASS):

```
  [PASS] objects uploaded with expected folder layout
  [PASS] metadata + tags stored
  [PASS] listing by category works
  [PASS] listing by tag works
  [PASS] anonymous GET of public object -> 200  (HTTP 200)
  [PASS] anonymous GET of private object -> denied  (HTTP 403)
  [PASS] URL carries signed expiry (X-Amz-Expires)  (X-Amz-Expires=5)
  [PASS] pre-signed GET of private object -> 200  (HTTP 200)
  [PASS] downloaded content is correct
  [PASS] tampered pre-signed URL rejected  (HTTP 403)
  [PASS] expired pre-signed URL -> denied  (HTTP 403)
  [PASS] anonymous GET of formerly public object -> denied  (HTTP 403)
  [PASS] authenticated download matches original
  [PASS] objects deleted
Result: 14/14 checks passed
```

### Step 12: Offline unit tests

```bash
pytest -q        # 15 tests, no S3 required
```

### Step 13: Cleanup

```bash
docker compose down -v        # stops emulators (add --profile localstack if used)
rm -rf minio-data downloads
```
On AWS, delete test objects and the bucket (or `python storage_manager.py delete <key>` per object) to avoid leftovers.

---

## Acceptance criteria mapping

| Criterion | How it is met / verified |
|---|---|
| Runs on free tiers/emulators at no cost | MinIO, LocalStack, moto are free; AWS usage is a few bytes. Endpoint is configurable via `.env` |
| Private objects inaccessible without auth or pre-signed token | Bucket policy only grants `GetObject` on `public/*`; `check-access` + `demo` verify HTTP 403 on `private/*` |
| Pre-signed URLs expire | URL signs `X-Amz-Expires`; `demo` and Step 9 wait past expiry and expect 403 |

## Verification status (honest notes)

- **Tested here:** all CLI commands and the demo against the **moto** emulator, and the 15 offline unit tests. Everything passed except the expiry-denial check, which moto does not enforce.
- **Not tested here:** MinIO, LocalStack and real AWS (no Docker or AWS account in the authoring sandbox). The code uses only standard S3 APIs (SigV4, bucket policy, object tagging), which these services implement, but run `demo` on your backend to confirm. If you see a failure on a specific backend, run with `-v` for debug logs.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `cannot reach http://localhost:9000` | Emulator not running: `docker compose ps` |
| `SignatureDoesNotMatch` / `InvalidAccessKeyId` | Wrong keys in `.env` (MinIO default `minioadmin`/`minioadmin`) |
| `BucketAlreadyExists` on AWS | Bucket names are global: change `S3_BUCKET` |
| `IllegalLocationConstraintException` | Set `AWS_REGION` to the bucket's real region |
| Public file still 403 on AWS | Account-level Block Public Access is on; disable "block public policy" |
| `AccessDenied` calling `put_public_access_block` | Warning only; add the IAM actions from Step 3C |
| Pre-signed URL fails immediately | Check your machine clock (SigV4 is time-sensitive) |
| Expired URL still returns 200 | You are on moto (doesn't enforce expiry); use MinIO or AWS |

## Security notes

- Never commit `.env`; rotate any key that leaks. Use IAM users with the minimal policy above, not root keys.
- Use short expiries (minutes) for pre-signed URLs. Anyone holding the URL can use it until it expires.
- URLs signed with temporary STS credentials stop working when those credentials expire, even if `--expires` is longer.
- Public access is limited by design to the `public/` prefix; do not put sensitive files there.

## Using the code as a library

```python
from storage_manager import StorageManager, Settings

mgr = StorageManager(Settings.from_env())
key = mgr.upload_file("report.pdf", visibility="private", tags={"dept": "finance"})
url = mgr.presign(key, expires=600)
```

> **Node.js alternative:** the brief allows the AWS SDK for JavaScript. This deliverable implements the Python/boto3 path; the same API calls (`PutObject`, `PutObjectTagging`, `PutBucketPolicy`, `getSignedUrl` from `@aws-sdk/s3-request-presigner`) map one-to-one if you need a port.
