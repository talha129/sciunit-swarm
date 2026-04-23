"""
S3 push/pull for sciunit-swarm unified containers.
Uses the same credentials and bucket as sciunit.
"""

import os
import shutil
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


def push(tarball_path, workflow_id):
    """
    Upload unified.tar.gz to S3 under projects/{timestamp}/{workflow_id}/.
    Uses the same prefix and bucket as sciunit so lifecycle policies apply.
    Returns the CloudFront download URL.
    """
    creds = _credentials()
    s3 = boto3.client(
        's3',
        aws_access_key_id=creds['aws_access_key_id'],
        aws_secret_access_key=creds['aws_secret_access_key'],
    )
    timestamp = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    filename  = os.path.basename(tarball_path)
    key       = f'{S3_PREFIX}/{timestamp}/{workflow_id}/{filename}'
    s3.upload_file(tarball_path, BUCKET, key)
    return f'{CF_DOMAIN}/{key}'


def pull(url, output_dir):
    """
    Download a unified container tarball from a CF/S3 URL.
    Extracts the original workflow_id from the URL path.
    Saves to output_dir/<workflow_id>/unified.tar.gz.
    Returns the workflow_id.
    """
    workflow_id = _workflow_id_from_url(url)
    dest_dir    = os.path.join(output_dir, workflow_id)
    os.makedirs(dest_dir, exist_ok=True)
    dest        = os.path.join(dest_dir, 'unified.tar.gz')

    with requests.get(url, stream=True) as resp:
        if resp.status_code == 429:
            raise RuntimeError(
                'Monthly download bandwidth limit exceeded. '
                'Try again next month or contact the sciunit maintainers.'
            )
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(dir=dest_dir, delete=False) as tmp:
            shutil.copyfileobj(resp.raw, tmp)
            tmp_path = tmp.name
    os.replace(tmp_path, dest)
    return workflow_id


def _workflow_id_from_url(url):
    """
    Extract original workflow_id from URL.
    Expected format: .../projects/{timestamp}/{workflow_id}/unified.tar.gz
    """
    parts = url.rstrip('/').split('/')
    try:
        prefix_idx = parts.index(S3_PREFIX)
        # workflow_id is two segments after the prefix (prefix/timestamp/workflow_id/file)
        return parts[prefix_idx + 2]
    except (ValueError, IndexError):
        import hashlib
        return hashlib.sha1(url.encode()).hexdigest()[:8]
