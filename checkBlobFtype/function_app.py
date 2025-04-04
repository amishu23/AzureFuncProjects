import azure.functions as func
import logging
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="check_container_ftype")
def check_container_ftype(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request for container file type check.")

    container = req.params.get("container")
    ftype = req.params.get("ftype")

    # Check if missing, then get from JSON body
    try:
        req_body = req.get_json()
        container = container or req_body.get("container")
        ftype = ftype or req_body.get("ftype")
    except ValueError:
        pass  # Ignore JSON parse errors

    # Validate required parameters
    if not container or not ftype:
        return func.HttpResponse(
            "Error: 'container' and 'ftype' parameters are required.",
            status_code=400
        )

    try:
        # Securely fetch the connection string from environment variables
        connection_string = os.getenv("AzureWebJobsStorage")
        if not connection_string:
            raise ValueError("Azure Storage connection string is missing.")

        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container)

        # List blobs and filter them based on prefix and suffix
        blob_list = container_client.list_blobs()
        selected_ftype_names = [
            blob.name for blob in blob_list
            if blob.name.startswith("monthly_") and blob.name.endswith(ftype)
        ]

        return func.HttpResponse(
            f"Connected to Container: {container}\n"
            f"File Type: {ftype}\n"
            f"Total Files: {len(selected_ftype_names)}\n"
            f"File Names: {selected_ftype_names}",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error accessing Azure Storage: {str(e)}")
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )
