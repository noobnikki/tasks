from minio import Minio

# Initialize Minio client
minio_client = Minio(
    "minio-url",
    access_key="",
    secret_key="",
    secure=True,
)

def create_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def list_buckets():
    buckets = minio_client.list_buckets()
    print("List of buckets:")
    for bucket in buckets:
        print(bucket.name)

def upload_csv_to_bucket(bucket_name, csv_file_path):
    minio_client.fput_object(bucket_name, "my-csv-file.csv", csv_file_path)
    print(f"CSV file uploaded to '{bucket_name}' successfully.")


bucket_name = "my-bucket"
csv_file_path = "path/file.csv"

create_bucket(bucket_name)
list_buckets()
upload_csv_to_bucket(bucket_name, csv_file_path)