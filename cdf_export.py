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
BACKUP_DIRECTORY = os.path.join(DIRECTORY, "backup")
RESTORE_DIRECTORY = os.path.join(DIRECTORY, "restore")
ZIPFILE_NAME = f"{TODAY}_backup.zip"
GCS_BACKUP_FOLDER = "cdf/latest"
GCS_ARCHIVE_FOLDER = "cdf/archive"

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

def fetch_applications(headers):
    try:
        # Define the URL for the export API
        response  = session.get(f"https://{CDAP_BASE_URL}api/v3/export/apps", headers=headers, stream=True)

        # Ensure the output directory exists
        if not os.path.exists(BACKUP_DIRECTORY):
            os.makedirs(BACKUP_DIRECTORY)

        # Path to save the downloaded ZIP file
        zip_path = os.path.join(BACKUP_DIRECTORY, "exported_apps.zip")

        if response.status_code != 200:
            raise Exception(f"Failed to export applications: {response.status_code} {response.reason}")

        # Write the response content to the ZIP file
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"Downloaded application export to: {zip_path}")

        # Extract the ZIP file
        extracted_dir = os.path.join(BACKUP_DIRECTORY, f"{TODAY}_backup")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extracted_dir)

        logging.info(f"Extracted application details to: {extracted_dir}")

        return extracted_dir

    except Exception as e:
        raise Exception(f"Error exporting CDAP applications: {e}")


def fetch_pipelines_list(namespace, headers):
    try:
        response = session.get(f"https://{CDAP_BASE_URL}/api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}/drafts", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipelines for namespace '{namespace}': {e}")
        return []


def fetch_pipeline(namespace, header, draft_id):
    try:
        response = session.get(f"https://{CDAP_BASE_URL}api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}/drafts/{draft_id}", headers=header)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch pipeline *{draft_id}* from namespace '{namespace}': {e}")
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


def save_to_gcs(file_name, file_path, bucket_folder):
    try:

        # Upload the .zip file to GCS
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"{bucket_folder}/{file_name}")
        blob.upload_from_filename(file_path)

        logging.info(f"Zip file uploaded to GCS bucket '{GCS_BUCKET_NAME}' as '{file_name}'.")
    except Exception as e:
        logging.error(f"Failed to save {file_name} to GCS: {e}")


def compress_folder(folder_path):
    """
    Compresses a folder into a .zip file and uploads it to a GCS bucket.

    Args:
        folder_path (str): The path of the folder to compress.

    Returns:
        str: Success message or raises an exception if an error occurs.
    """
    try:
        # Ensure the folder exists
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder '{folder_path}' does not exist.")

        # Create a .zip file
        zip_path = f"{folder_path}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)

        logging.info(f"Compressed folder saved at: {zip_path}")

        return zip_path

    except Exception as e:
        raise Exception(f"Failed to compress folder: {e}")


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


def read_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        raise Exception(f"Error reading JSON file at '{file_path}': {e}")


def download_and_unzip_from_gcs(gcs_folder_path, version=None):
    """
    Downloads a .zip file from a GCS bucket, saves it locally, and unzips it.

    Args:
        bucket_name (str): The name of the GCS bucket.
        folder_path (str): The folder path in the bucket where the .zip file is located.
        zip_filename (str): The name of the .zip file to download.
        local_download_path (str): The local directory to save and extract the contents of the .zip file.
        :param version:

    Returns:
        str: Success message or raises an exception if an error occurs.

    """
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)

        blobs = bucket.list_blobs(prefix=gcs_folder_path)

        zip_files = [blob for blob in blobs if blob.name.endswith(".zip")]

        if not zip_files:
            logging.error(f"No .zip files found in folder '{gcs_folder_path}' of bucket '{GCS_BUCKET_NAME}'.")
            raise Exception(f"No .zip files found in folder '{gcs_folder_path}'.")

        if version:
            zip_blob = None
            for blob in zip_files:
                zip_version = blob.name.split("/")[-1].split("_")[0]
                if zip_version == version:
                    zip_blob = blob
                    logging.info(f"Archive have the Specified version for date '{zip_version}'.")
                    break
            if not zip_blob:
                logging.warning(f"Specified version '{version}' was not found in archive.")
                raise Exception(f"Specified version '{version}' was not found in archive.")

        else:
            zip_blob = zip_files[0]
            # zip_filename = zip_blob.name.split("/")[-1]
            print(zip_blob.name)

        zip_filename = zip_blob.name.split("/")[-1]
        extract_dir = zip_blob.name.split("/")[-1][:-4]

        # Ensure the local download directory exists
        if not os.path.exists(RESTORE_DIRECTORY):
            os.makedirs(RESTORE_DIRECTORY)

        # Full path to save the .zip file locally
        local_zip_path = os.path.join(RESTORE_DIRECTORY, zip_filename)
        local_extract_path = os.path.join(RESTORE_DIRECTORY, extract_dir)

        # Download the .zip file from GCS
        zip_blob.download_to_filename(local_zip_path)
        logging.info(f"Downloaded '{zip_filename}' from GCS bucket '{GCS_BUCKET_NAME}/{gcs_folder_path}' to '{RESTORE_DIRECTORY}'.")

        # Unzip the file
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_extract_path)
        logging.info(f"Unzipped contents to '{local_extract_path}'.")

        logging.info(f"File '{zip_filename}' downloaded and extracted successfully to '{local_extract_path}'.")

        return local_extract_path

    except Exception as e:
        raise Exception(f"Failed to download and unzip file: {e}")


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

def format_deployed_apps(apps_location, namespaces):
    logging.info("*********Formatting Application****************")
    for namespace in namespaces:
        name = namespace["name"]
        namespace_dir = os.path.join(apps_location, name)

        if not os.path.exists(namespace_dir):
            logging.warning(f"Namespace directory '{namespace_dir}' does not exist. Skipping.")
            continue

        # Iterate through files in the directory
        for file in os.listdir(namespace_dir):
            file_path = os.path.join(namespace_dir, file)

            if os.path.isfile(file_path):
                try:
                    # Open and read the JSON file
                    with open(file_path, 'r') as f:
                        response = json.load(f)

                    # Extract and parse the configuration field
                    configuration_str = response.get("configuration", "")

                    try:
                        # Decode the configuration string into a dictionary
                        configuration_dict = json.loads(configuration_str)

                        # Replace the original string with the parsed object
                        response["configuration"] = configuration_dict

                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse configuration in file '{file_path}': {e}")
                        continue

                    # Write the formatted JSON back to the same file
                    with open(file_path, 'w') as f:
                        json.dump(response, f, indent=4)

                    # Rename the file with a '_formatted' suffix
                    new_file_path = os.path.join(namespace_dir, f"app_{os.path.splitext(file)[0]}.json")
                    os.rename(file_path, new_file_path)

                    logging.info(f"Formatted and renamed file: {file_path} -> {new_file_path}")

                except Exception as e:
                    logging.error(f"Error processing file '{file_path}': {e}")


# Backup Application State
def backup_application_state(headers):

    logging.info(f"********************Exporting the Deployed applications******************")
    export_dir = fetch_applications(headers)

    namespaces = fetch_namespaces(headers)
    if not namespaces:
        logging.warning("No namespaces found to backup.")
        return

    format_deployed_apps(export_dir, namespaces)

    zip_folder = export_dir
    if not os.path.exists(zip_folder):
        os.makedirs(zip_folder)

    file_path = os.path.join(zip_folder, "namespaces.json")
    save_to_file(file_path, namespaces)

    for namespace in namespaces:
        namespace_name = namespace["name"]

        try:

            namespace_dir = os.path.join(zip_folder, namespace_name)
            if not os.path.exists(namespace_dir):
                logging.info(f"Creating namespace directory : '{namespace_name}'.")
                os.makedirs(namespace_dir)

            logging.info(f"*************Fetching Pipelines from {namespace_name}********************")
            pipelines_list = fetch_pipelines_list(namespace_name, headers)
            for pipeline in pipelines_list:
                pipeline_name = pipeline.get('name')
                draft_id = pipeline.get('id')
                content = fetch_pipeline(namespace_name, headers, draft_id)
                pipeline_file_path = os.path.join(namespace_dir, f"draft_{pipeline_name}.json")

                save_to_file(pipeline_file_path, content)

            logging.info(f"***************Fetching Connections from {namespace_name}*****************")
            connections = fetch_connections(namespace_name, headers)
            for connection in connections:
                connection_name = connection.get('name')
                connection_file_path = os.path.join(namespace_dir, f"conn_{connection_name}.json")
                save_to_file(connection_file_path, connection)
        except Exception as e:
            logging.error(f"Failed to fetch connections/pipelines for namespace '{namespace_name}': {e.args}")

    try:
        logging.info("***************Compressing and uploading the backup files to GCS******************************")

        zip_path = compress_folder(zip_folder)

        save_to_gcs(ZIPFILE_NAME, zip_path ,GCS_ARCHIVE_FOLDER)

        save_to_gcs("backup.zip", zip_path, GCS_BACKUP_FOLDER)

    except Exception as e:
        logging.error(f"Failed to compress and upload backup files to GCS: {e}")


# Restore Application State
def restore_application_state(headers, restore_version=None):

    if restore_version:
        zip_blob_path = download_and_unzip_from_gcs(GCS_ARCHIVE_FOLDER, restore_version)
    else:
        zip_blob_path = download_and_unzip_from_gcs(GCS_BACKUP_FOLDER)

    namespace_file_path = os.path.join(zip_blob_path, "namespaces.json")
    namespaces = read_from_file(namespace_file_path)
    if not namespaces:
        logging.warning("No namespaces found in backup. Exiting restore process.")
        return

    for namespace in namespaces:
        name = namespace["name"]

        conn_files = []
        pipeline_files = []
        deployed_apps = []
        namespace_dir = os.path.join(zip_blob_path, name)

        # Iterate through files in the directory
        for file in os.listdir(namespace_dir):
            if os.path.isfile(os.path.join(namespace_dir, file)):
                if file.startswith("conn"):
                    conn_files.append(file)
                elif file.startswith("draft"):
                    pipeline_files.append(file)
                else:
                    deployed_apps.append(file)


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

        #Recreate Deployed Pipelines
        for app in deployed_apps:
            app_pipeline = read_from_file(os.path.join(namespace_dir, app))

            try:
                app_name = app_pipeline.get("name")
                response = session.put(
                f"https://{CDAP_BASE_URL}api/v3/namespaces/{name}/apps/{app_name}", json=app_pipeline, headers=headers)
                response.raise_for_status()
                logging.info(f"App '{app}' recreated successfully in '{name}' namespace.")

            except Exception as e:
                logging.error(f"Failed to recreate app '{app}': {e}")

        # Recreate Draft pipelines
        for blob in pipeline_files:
            pipeline = read_from_file(os.path.join(namespace_dir, blob))
            try:
                pipeline_name = pipeline["id"]

                response = session.put(
                    f"https://{CDAP_BASE_URL}api/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{name}/drafts/{pipeline_name}",
                    json=pipeline,
                    headers=headers)
                response.raise_for_status()
                logging.info(f"Pipeline '{pipeline['name']}' restored in namespace '{name}'.")
            except Exception as e:
                logging.error(f"Failed to restore pipeline '{pipeline['name']}' in namespace '{name}': {e}")

            # Recreate connections
            for blob in conn_files:
                connection = read_from_file(os.path.join(namespace_dir, blob))
                connection_name = connection["name"]

                try:
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
    parser.add_argument("--restore_version", help="Backup version you want to restore (e.g., '2024-12-06').", required=False)
    args = parser.parse_args()

    try:

        access_token = get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        if args.operation == "backup":
            backup_application_state(headers)
        elif args.operation == "restore":
            restore_application_state(headers, restore_version=args.restore_version)
    except Exception as e:
        logging.error(f"An unexpected error occurred during -> {args.operation}: {e}")


if __name__ == "__main__":
    main()
