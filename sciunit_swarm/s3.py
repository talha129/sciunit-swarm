"""
S3 push/pull for sciunit-swarm workflow containers.
Uses the same credentials and bucket as sciunit.
"""

import io
import os
import shutil
import tarfile
import tempfile
from datetime import datetime

import boto3
import requests

CF_DOMAIN       = 'https://d3okuktvxs1y4w.cloudfront.net'
CREDENTIALS_URL = f'{CF_DOMAIN}/persistent/sciunit-aws-creds.json'
BUCKET          = 'sciunit-copy'
S3_PREFIX       = 'projects'


def _credentials():
    resp = requests.get(CREDENTIALS_URL)
    resp.raise_for_status()
    return resp.json()


def push(output_dir, workflow_id):
    """
    Bundle manager.tar.gz + worker.tar.gz into a single workflow.tar.gz,
    upload to S3 under projects/{timestamp}/{workflow_id}/workflow.tar.gz.
    Returns the CloudFront download URL.
    """
    workflow_dir = os.path.join(output_dir, workflow_id)

    for filename in ('manager.tar.gz', 'worker.tar.gz'):
        path = os.path.join(workflow_dir, filename)
        if not os.path.isfile(path):
            raise FileNotFoundError(
                f'[push] {filename} not found in {workflow_dir}\n'
                f'       Make sure --output matches the path used in controller exec.'
            )

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as t:
        for filename in ('manager.tar.gz', 'worker.tar.gz'):
            path = os.path.join(workflow_dir, filename)
            t.add(path, arcname=filename)
            print(f'[push] added {filename}')
    payload = buf.getvalue()

    creds = _credentials()
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds['aws_access_key_id'],
        aws_secret_access_key=creds['aws_secret_access_key'],
    )
    timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    key       = f'{S3_PREFIX}/{timestamp}/{workflow_id}/workflow.tar.gz'
    s3.upload_fileobj(io.BytesIO(payload), BUCKET, key)
    mb = len(payload) / 1_048_576
    print(f'[push] uploaded workflow.tar.gz ({mb:.1f} MB)')
    return f'{CF_DOMAIN}/{key}'


def pull(url, output_dir):
    """
    Download workflow.tar.gz from a CloudFront URL.
    Extracts manager.tar.gz and worker.tar.gz into output_dir/<workflow_id>/.
    Returns the workflow_id.
    """
    workflow_id = _workflow_id_from_url(url)
    dest_dir    = os.path.join(output_dir, workflow_id)
    os.makedirs(dest_dir, exist_ok=True)

    with requests.get(url, stream=True) as resp:
        if resp.status_code == 429:
            raise RuntimeError(
                'Monthly download bandwidth limit exceeded. '
                'Try again next month or contact the sciunit maintainers.'
            )
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(dir=dest_dir, delete=False, suffix='.tar.gz') as tmp:
            shutil.copyfileobj(resp.raw, tmp)
            tmp_path = tmp.name

    mb = os.path.getsize(tmp_path) / 1_048_576
    print(f'[pull] downloaded workflow.tar.gz ({mb:.1f} MB)')

    with tarfile.open(tmp_path, 'r:gz') as t:
        t.extractall(dest_dir)
    os.unlink(tmp_path)

    for filename in ('manager.tar.gz', 'worker.tar.gz'):
        path = os.path.join(dest_dir, filename)
        if os.path.isfile(path):
            print(f'[pull] extracted {filename}')
        else:
            print(f'[pull] warning: {filename} not found in archive')

    return workflow_id


def _workflow_id_from_url(url):
    """
    Extract workflow_id from URL.
    Expected format: .../projects/{timestamp}/{workflow_id}/workflow.tar.gz
    """
    parts = url.rstrip('/').split('/')
    try:
        prefix_idx = parts.index(S3_PREFIX)
        return parts[prefix_idx + 2]
    except (ValueError, IndexError):
        import hashlib
        return hashlib.sha1(url.encode()).hexdigest()[:8]
