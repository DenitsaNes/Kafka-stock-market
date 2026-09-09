import json
import os
import tempfile
from datetime import datetime, timezone
import boto3
import pandas as pd

RAW_PREFIX = 'raw/'
PARQUET_PREFIX = 'parquet/'


def get_bucket_name():
    bucket = os.getenv('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


s3 = boto3.client('s3')


def list_raw_keys(date_str):
    bucket = get_bucket_name()
    prefix = f"{RAW_PREFIX}{date_str.replace('-', '/')}/"
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj['Key'] for obj in page.get('Contents', []) if obj['Key'].endswith('.json'))
    return keys


def parse_message(value):
    # Messages are now normalized MarketTradeEvent records with
    # symbol, price, volume, timestamp_ms, and source keys.
    if isinstance(value, dict) and 'symbol' in value:
        return [{
            'symbol': value['symbol'],
            'price': float(value.get('price', 0)),
            'volume': float(value.get('volume', 0)),
            'timestamp_ms': value.get('timestamp_ms'),
            'source': value.get('source', 'unknown')
        }]

    # Fallback for older batched Finnhub payloads still in S3.
    records = []
    if isinstance(value, dict) and 'data' in value and isinstance(value['data'], list):
        for trade in value['data']:
            records.append({
                'symbol': trade.get('s'),
                'price': float(trade.get('p', 0)),
                'volume': float(trade.get('v', 0)),
                'timestamp_ms': trade.get('t'),
                'source': 'finnhub'
            })
    else:
        records.append({
            'symbol': value.get('s'),
            'price': float(value.get('p', 0)),
            'volume': float(value.get('v', 0)),
            'timestamp_ms': value.get('t'),
            'source': 'demo'
        })
    return records


def process_date(date_str):
    keys = list_raw_keys(date_str)
    if not keys:
        print(f"No raw files for {date_str}")
        return

    records = []
    for i, key in enumerate(keys):
        if i > 0 and i % 50 == 0:
            print(f"Downloaded {i}/{len(keys)} raw files...")
        response = s3.get_object(Bucket=get_bucket_name(), Key=key)
        value = json.loads(response['Body'].read().decode('utf-8'))
        records.extend(parse_message(value))

    df = pd.DataFrame(records)
    if df.empty:
        print(f"No records for {date_str}")
        return

    df['event_time'] = pd.to_datetime(df['timestamp_ms'], unit='ms', utc=True)
    df['date'] = date_str

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, 'output')
        df.to_parquet(local_path, partition_cols=['date'], index=False, engine='pyarrow')

        bucket = get_bucket_name()
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_key = f"{PARQUET_PREFIX}{relative_path}"
                s3.upload_file(local_file, bucket, s3_key)
                print(f"Uploaded: s3://{bucket}/{s3_key}")

    print(f"Processed {len(df)} records for {date_str}")


def main():
    # Process today's partition by default
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    process_date(date_str)


if __name__ == '__main__':
    main()
