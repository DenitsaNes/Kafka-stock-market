from unittest.mock import Mock, patch

from consumers.s3_upload import upload_to_s3


def test_upload_to_s3_success():
    mock_s3 = Mock()

    upload_to_s3(mock_s3, "my-bucket", "raw/test.json", '{"symbol": "BTC"}')

    mock_s3.put_object.assert_called_once_with(
        Bucket="my-bucket",
        Key="raw/test.json",
        Body='{"symbol": "BTC"}'
    )


def test_upload_to_s3_retries_on_failure_then_succeeds():
    mock_s3 = Mock()
    mock_s3.put_object.side_effect = [Exception("timeout"), None]

    with patch('consumers.s3_upload.time.sleep') as mock_sleep:
        upload_to_s3(mock_s3, "my-bucket", "raw/test.json", '{}')

    assert mock_s3.put_object.call_count == 2
    mock_sleep.assert_called_once()


def test_upload_to_s3_exhausts_retries_and_raises():
    mock_s3 = Mock()
    mock_s3.put_object.side_effect = Exception("timeout")

    with patch('consumers.s3_upload.time.sleep'):
        try:
            upload_to_s3(mock_s3, "my-bucket", "raw/test.json", '{}')
            raise AssertionError("Should have raised after exhausting retries")
        except Exception as e:
            assert "timeout" in str(e)

    assert mock_s3.put_object.call_count == 5
