#to ingest a file from minio to datahub using api call
import requests
import json
from minio import Minio

minio_endpoint = ""
minio_access_key = ""
minio_secret_key = ""
minio_bucket_name = ""
minio_file_path = ""

datahub_endpoint = "http://localhost:9002"
datahub_api_key = ""

minio_client = Minio(minio_endpoint,
                     access_key=minio_access_key,
                     secret_key=minio_secret_key,
                     secure=False)

response = minio_client.get_object(minio_bucket_name, minio_file_path)
csv_data = response.data.decode("utf-8")

dataset_name = "My Dataset"
dataset_description = "Description of my dataset"
dataset_tags = ["tag1", "tag2"]

payload = {
    "name": dataset_name,
    "description": dataset_description,
    "tags": dataset_tags,
    "fileData": csv_data
}

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {datahub_api_key}"
}

response = requests.post(datahub_endpoint, data=json.dumps(payload), headers=headers)

if response.status_code == 200:
    print("CSV file ingested successfully!")
    dataset_id = response.json()["id"]
    print(f"Dataset ID: {dataset_id}")
else:
    print("CSV file ingestion failed. Error:")
    print(response.text)