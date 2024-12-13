from http.client import responses

import requests
import json
import argparse
from google.cloud import storage
import logging
import subprocess
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
CDAP_BASE_URL = "ci-dev-cdf-asne1-01-apc-serverless-anycloud-dot-ane1.datafusion.googleusercontent.com/"
GCS_BUCKET_NAME = "ci-dev-configurations-asia-northeast1"

# curl -X GET https://ci-dev-cdf-asne1-01-apc-serverless-anycloud-dot-ane1.datafusion.googleusercontent.com/api/v3/namespaces/backup_namespace/apps/ -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" --output Raw_PII_ETL_1.json

storage_client = storage.Client()

# Retry configuration
# retry_strategy = Retry(
#     total=3,
#     backoff_factor=2,
#     status_forcelist=[500, 502, 503, 504],
#     method_whitelist=["GET", "POST"]
# )
adapter = HTTPAdapter()
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)


def get_access_token():
    """
    Retrieve an access token using the gcloud CLI.
    """
    try:
        result = subprocess.run(
            ["gcloud", "auth", "application-default", "print-access-token"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True
        )
        access_token = result.stdout.strip()
        logging.info("Successfully retrieved access token.")
        return access_token
    except subprocess.CalledProcessError as e:
        logging.critical(f"Failed to retrieve access token: {e.stderr}")
        raise Exception("Unable to retrieve access token. Ensure you are authenticated using gcloud.")


# Helper Functions
def fetch_namespaces(headers):
    try:
        response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch namespaces: {e}")
        return []


def fetch_pipelines_list(namespace, headers):
    try:
        # response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/{namespace}/apps", headers=headers)
        response = session.get(f"https://{CDAP_BASE_URL}/api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}/drafts", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipelines for namespace '{namespace}': {e}")
        return []


def fetch_pipeline(namespace, header, draft_id):
    try:
        # response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/{namespace}/apps/{app}", headers=header)
        response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}/drafts/{draft_id}", headers=header)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipeline with id *{draft_id}* from namespace '{namespace}': {e}")
        return


def fetch_connections(namespace, headers):
    try:
        response = session.get(
            f"https://{CDAP_BASE_URL}/api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}/connections",
            headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch connections for namespace '{namespace}': {e}")
        return []


def save_to_gcs(file_name, data):
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps(data))
        logging.info(f"Saved {file_name} to GCS.")
    except Exception as e:
        logging.error(f"Failed to save {file_name} to GCS: {e}")


def load_from_gcs(file_name):
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(file_name)
        data = json.loads(blob.download_as_text())
        logging.info(f"Loaded {file_name} from GCS.")
        return data
    except Exception as e:
        logging.error(f"Failed to load {file_name} from GCS: {e}")
        return {}


# Backup Application State
def backup_application_state(headers):
    namespaces = fetch_namespaces(headers)
    if not namespaces:
        logging.warning("No namespaces found to backup.")
        return

    save_to_gcs("cdf/namespaces.json", namespaces)
    for namespace in namespaces:
        namespace_name = namespace["name"]
        if namespace_name != "default":
            try:

                logging.info(f"*************Fetching Pipelines from {namespace_name}********************")
                pipelines_list = fetch_pipelines_list(namespace_name, headers)
                for pipeline in pipelines_list:
                    # print(pipeline)
                    pipeline_name = pipeline.get('name')
                    draft_id = pipeline.get('id')
                    # print(pipeline_name)
                    content = fetch_pipeline(namespace_name, headers, draft_id)
                    save_to_gcs(f"cdf/{namespace_name}/pipelines/{pipeline_name}.json", content)

                logging.info(f"***************Fetching Connections from {namespace_name}*****************")
                connections = fetch_connections(namespace_name, headers)
                for connection in connections:
                    connection_name = connection.get('name')
                    save_to_gcs(f"cdf/{namespace_name}/connections/{connection_name}.json", connection)
            except Exception as e:
                logging.error(f"Failed to fetch connections/pipelines for namespace '{namespace_name}': {e.args}")


# Restore Application State
def restore_application_state(headers):
    namespaces = load_from_gcs("cdf/namespaces.json")
    if not namespaces:
        logging.warning("No namespaces found in backup. Exiting restore process.")
        return

    for namespace in namespaces:
        name = namespace["name"]

        if name != "default":

            try:
                # Recreate namespace
                response = session.put(f"https://{CDAP_BASE_URL}api/v3/namespaces/{name}", json=namespace,
                                       headers=headers)
                response.raise_for_status()
                logging.info(f"Namespace '{name}' recreated successfully.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to recreate namespace '{name}': {e}")
                continue

            # Recreate pipelines
            try:
                bucket = storage_client.bucket(GCS_BUCKET_NAME)
                print("name --> ", name)
                print("bucket -->", bucket)
                list_pipeline = bucket.list_blobs(prefix=f"cdf/{name}/pipelines/", delimiter="/")
                print("list -->", list_pipeline)
                for blob in list_pipeline:
                    print("blob --> ", blob.name)
                    pipeline = load_from_gcs(blob.name)
                    pipeline_name = pipeline["name"]
                    # print(pipeline)

                    # for pipeline in pipelines:
                    response = session.put(
                        f"https://{CDAP_BASE_URL}api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{name}/drafts/{pipeline_name}",
                        json=pipeline,
                        headers=headers)
                    response.raise_for_status()
                    logging.info(f"Pipeline '{pipeline['name']}' restored in namespace '{name}'.")
            except Exception as e:
                logging.error(f"Failed to restore pipeline '{pipeline['name']}' in namespace '{name}': {e}")

            # Recreate connections
            try:
                bucket = storage_client.bucket(GCS_BUCKET_NAME)
                print("name --> ", name)
                print("bucket -->", bucket)
                list_connections = bucket.list_blobs(prefix=f"cdf/{name}/connections/", delimiter="/")
                print("list -->", list_connections)
                for blob in list_connections:
                    print("blob -->", blob)
                    connection = load_from_gcs(blob.name)
                    connection_name = connection["name"]
                    response = session.put(
                        f"https://{CDAP_BASE_URL}api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{name}/connections/{connection_name}",
                        json=connection,
                        headers=headers)
                    response.raise_for_status()
                    logging.info(f"Connection '{connection['name']}' restored in namespace '{name}'.")
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Failed to restore connection '{connection['name']}' in namespace '{name}': {e}")


# Main Function
def main():
    parser = argparse.ArgumentParser(description="Backup or Restore GCP Data Fusion CDAP Application State.")
    parser.add_argument("operation", choices=["backup", "restore"], help="Specify 'backup' or 'restore'.")
    args = parser.parse_args()

    try:

        access_token = get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        if args.operation == "backup":
            backup_application_state(headers)
        elif args.operation == "restore":
            restore_application_state(headers)
    except Exception as e:
        logging.error(f"An unexpected error occurred during -> {args.operation}: {e}")


if __name__ == "__main__":
    main()
