import requests
import json
import csv

datahub_url = "http://localhost:9002"


dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:myplatform,mydataset,PROD)"
dataset_name = "My Dataset"
description = "This is a dataset ingested from a CSV file"
schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [
        {"name": "field1", "type": "string"},
        {"name": "field2", "type": "string"}
    ]
}

csv_file_path = "/path/to/your/csvfile.csv"

with open(csv_file_path, "r") as file:
    csv_data = [dict(row) for row in csv.DictReader(file)]

# Ingestion payload
payload = {
    "metadata": {
        "platform": "myplatform",
        "name": dataset_name,
        "description": description,
        "schema": schema
    },
    "rows": csv_data
}


url = f"{datahub_url}/ingest/{dataset_urn}"
headers = {"Content-Type": "application/json"}
response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print("CSV file ingested successfully.")
else:
    print("Failed to ingest CSV file:", response.text)