import os
import requests
import json
import argparse
from google.cloud import storage
import logging
import subprocess
from requests.adapters import HTTPAdapter
from datetime import datetime
import zipfile
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
CDAP_BASE_URL = "ci-dev-cdf-asne1-01-apc-serverless-anycloud-dot-ane1.datafusion.googleusercontent.com/"
GCS_BUCKET_NAME = "ci-dev-configurations-asia-northeast1"
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
TODAY = datetime.now().strftime("%Y-%m-%d")
BACKUP_DIRECTORY = os.path.join(DIRECTORY, f"{TODAY}_backup")
ZIPFILE = os.path.join(DIRECTORY , f"{TODAY}_backup.zip")

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
        response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/{namespace}/apps", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipelines for namespace '{namespace}': {e}")
        return []


def fetch_pipeline(namespace, header, app):
    try:
        response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/{namespace}/apps/{app}", headers=header)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipeline *{app}* from namespace '{namespace}': {e}")
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


def compress_folder_and_upload_to_gcs(folder_path, zip_filename):
    """
    Compresses a folder into a .zip file and uploads it to a GCS bucket.

    Args:
        folder_path (str): The path of the folder to compress.
        zip_filename (str): The name of the .zip file to create and upload.

    Returns:
        str: Success message or raises an exception if an error occurs.
    """
    try:
        # Ensure the folder exists
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder '{folder_path}' does not exist.")

        # Create a .zip file
        zip_path = f"{BACKUP_DIRECTORY}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)

        print(f"Compressed folder saved at: {zip_path}")

        # Upload the .zip file to GCS
        # client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(zip_filename, sufix="cdf/latest/")
        blob.upload_from_filename(zip_path)

        return f"Folder '{folder_path}' compressed and uploaded to GCS bucket '{GCS_BUCKET_NAME}' as '{zip_filename}'."

    except Exception as e:
        raise Exception(f"Failed to compress and upload folder: {e}")


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


def save_to_file(filename, content):
    """
    Create a file with the given JSON content in a folder at the same location as the script.

    Args:
        filename (str): The name of the file to be created.
        content (dict): The JSON content to write into the file.

    Returns:
        str: Success message or raises an exception if an error occurs.
    """
    try:

        # Create a folder at the script's directory if not present
        backup_dir = BACKUP_DIRECTORY
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        # Full path to the file
        file_path = os.path.join(backup_dir, filename)

        # Write the JSON content to the file
        with open(file_path, 'w') as file:
            json.dump(content, file, indent=4)  # Format JSON with indentation

        logging.info(f"File '{file_path}' created successfully.")
    except Exception as e:
        raise Exception(f"Failed to create file '{filename}': {e}")


# Backup Application State
def backup_application_state(headers):
    namespaces = fetch_namespaces(headers)
    if not namespaces:
        logging.warning("No namespaces found to backup.")
        return

    file_path = os.path.join(BACKUP_DIRECTORY, "namespaces.json")
    # save_to_gcs("cdf/namespaces.json", namespaces)
    save_to_file(file_path, namespaces)

    for namespace in namespaces:
        namespace_name = namespace["name"]
        if namespace_name != "default":
            try:

                logging.info(f"*************Fetching Pipelines from {namespace_name}********************")
                pipelines_list = fetch_pipelines_list(namespace_name, headers)
                for pipeline in pipelines_list:
                    # print(pipeline)
                    pipeline_name = pipeline.get('name')
                    # print(pipeline_name)
                    content = fetch_pipeline(namespace_name, headers, pipeline_name)
                    pipeline_file_path = os.path.join(BACKUP_DIRECTORY, namespace_name, f"{pipeline_name}.json")
                    # save_to_gcs(f"cdf/{namespace_name}/pipelines/{pipeline_name}.json", content)
                    save_to_file(pipeline_file_path, content)

                logging.info(f"***************Fetching Connections from {namespace_name}*****************")
                connections = fetch_connections(namespace_name, headers)
                for connection in connections:
                    connection_name = connection.get('name')
                    connection_file_path = os.path.join(BACKUP_DIRECTORY, namespace_name, f"{connection_name}.json")
                    # save_to_gcs(f"cdf/{namespace_name}/connections/{connection_name}.json", connection)
                    save_to_file(connection_file_path, connection)
            except Exception as e:
                logging.error(f"Failed to fetch connections/pipelines for namespace '{namespace_name}': {e.args}")

    try:
        logging.info("***************Compressing and uploading the backup files to GCS******************************")

        compress_folder_and_upload_to_gcs(BACKUP_DIRECTORY, f"{TODAY}_backup.zip")


    except Exception as e:
        logging.error(f"Failed to compress and upload backup files to GCS: {e}")


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
