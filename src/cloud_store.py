import os
import logging
from pathlib import Path
import datetime

try:
    import boto3  
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

try:
    from google.cloud import storage  
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient  
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_to_s3(file_path: str, bucket_name: str, aws_access_key_id: str = None, 
                aws_secret_access_key: str = None, region: str = 'us-east-1'):
    """Upload a file to AWS S3."""
    if not S3_AVAILABLE:
        logger.error("boto3 not installed. Run: pip install boto3")
        return False

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )

        timestamp = datetime.datetime.now().strftime('%Y/%m/%d')
        filename = Path(file_path).name
        s3_key = f"{timestamp}/{filename}"

        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Successfully uploaded to S3: s3://{bucket_name}/{s3_key}")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        return False

def upload_to_gcs(file_path: str, bucket_name: str, credentials_path: str = None):
    """Upload a file to Google Cloud Storage."""
    if not GCS_AVAILABLE:
        logger.error("google-cloud-storage not installed. Run: pip install google-cloud-storage")
        return False

    try:
        # Set credentials if provided
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Upload file with timestamp-based path
        timestamp = datetime.datetime.now().strftime('%Y/%m/%d')
        filename = Path(file_path).name
        blob = bucket.blob(f"{timestamp}/{filename}")

        blob.upload_from_filename(file_path)
        logger.info(f"Successfully uploaded to GCS: gs://{bucket_name}/{blob.name}")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to GCS: {str(e)}")
        return False

def upload_to_azure(file_path: str, container_name: str, connection_string: str):
    """Upload a file to Azure Blob Storage."""
    if not AZURE_AVAILABLE:
        logger.error("azure-storage-blob not installed. Run: pip install azure-storage-blob")
        return False

    try:
        # Create the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Upload file with timestamp-based path
        timestamp = datetime.datetime.now().strftime('%Y/%m/%d')
        filename = Path(file_path).name
        blob_path = f"{timestamp}/{filename}"

        with open(file_path, "rb") as data:
            container_client.upload_blob(name=blob_path, data=data)

        logger.info(f"Successfully uploaded to Azure: {container_name}/{blob_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to Azure: {str(e)}")
        return False

def backup_to_cloud(backup_file: str, cloud_provider: str = 's3', **kwargs):
    """
    Upload a backup file to the specified cloud provider.
    
    Args:
        backup_file: Path to the backup file
        cloud_provider: 's3', 'gcs', or 'azure'
        **kwargs: Provider-specific arguments
            For S3: bucket_name, aws_access_key_id, aws_secret_access_key, region
            For GCS: bucket_name, credentials_path
            For Azure: container_name, connection_string
    """
    if not os.path.exists(backup_file):
        logger.error(f"Backup file not found: {backup_file}")
        return False

    if cloud_provider.lower() == 's3':
        return upload_to_s3(backup_file, **kwargs)
    elif cloud_provider.lower() == 'gcs':
        return upload_to_gcs(backup_file, **kwargs)
    elif cloud_provider.lower() == 'azure':
        return upload_to_azure(backup_file, **kwargs)
    else:
        logger.error(f"Unsupported cloud provider: {cloud_provider}")
        return False

def main():

    backup_file = "/path/to/your/backup.sql.gz"

    backup_to_cloud(
        backup_file,
        cloud_provider='s3',
        bucket_name='my-database-backups',
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_KEY'
    )
    
    backup_to_cloud(
        backup_file,
        cloud_provider='gcs',
        bucket_name='my-database-backups',
        credentials_path='/path/to/google-credentials.json'
    )
    
    backup_to_cloud(
        backup_file,
        cloud_provider='azure',
        container_name='database-backups',
        connection_string='YOUR_AZURE_CONNECTION_STRING'
    )

if __name__ == "__main__":
    main()
