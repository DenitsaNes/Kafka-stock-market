import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BASE_DELAY = 1


def upload_to_s3(s3_client, bucket, key, body):
    """Upload an object to S3 with exponential backoff retries."""
    for attempt in range(MAX_RETRIES):
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body
            )
            return True
        except Exception as e:
            delay = BASE_DELAY * (2 ** attempt)
            logger.error(f"S3 upload failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error("S3 upload exhausted all retries")
                raise
