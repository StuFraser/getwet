import os
import boto3
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

R2_ENDPOINT = "R2 Url"

def upload_to_r2(local_file_path, bucket_name, r2_object_name=None):
    # 1. Cloudflare R2 Configuration
    account_id = "<Account Id>"
    access_key_id = "<Access Key>"
    secret_access_key = "<Access Secret>"

    # R2 uses a specific endpoint format
    endpoint_url = R2_ENDPOINT

    # If no R2 object name is specified, use the local file name
    if r2_object_name is None:
        r2_object_name = os.path.basename(local_file_path)

    # 2. Configure Multipart Transfer Settings
    # 3.5GB is large, so we'll use 15MB chunks.
    # We also enable threads for concurrent chunk uploading.
    MB = 1024 * 1024
    config = TransferConfig(
        multipart_threshold=50 * MB,  # Files larger than 50MB will use multipart
        multipart_chunksize=15 * MB,   # Size of each chunk
        max_concurrency=10,            # Number of concurrent upload threads
        use_threads=True
    )

    # 3. Initialize the S3 Client for R2
    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto"  # R2 expects "auto" or can be left as is
    )

    # 4. Setup Progress Bar
    file_size = os.path.getsize(local_file_path)
    progress_bar = tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Uploading {os.path.basename(local_file_path)}"
    )

    # Callback function to update progress bar
    def upload_progress(bytes_amount):
        progress_bar.update(bytes_amount)

    # 5. Perform the Upload
    try:
        print("Starting upload to Cloudflare R2...")
        s3_client.upload_file(
            Filename=local_file_path,
            Bucket=bucket_name,
            Key=r2_object_name,
            Config=config,
            Callback=upload_progress
        )
        progress_bar.close()
        print(f"\nSuccess! Successfully uploaded to R2 as: {r2_object_name}")
        
    except Exception as e:
        progress_bar.close()
        print(f"\nAn error occurred during upload: {e}")

if __name__ == "__main__":
    # --- CONFIGURATION ---
    LOCAL_PARQUET_FILE = "overture_water_global.parquet"
    R2_BUCKET_NAME = "getwet"
    # ---------------------

    upload_to_r2(LOCAL_PARQUET_FILE, R2_BUCKET_NAME)