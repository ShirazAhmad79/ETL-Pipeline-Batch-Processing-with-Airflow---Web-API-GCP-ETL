import functions_framework
#       added by shiraz start ********
import httplib2
#       added by shiraz end ********
from googleapiclient.discovery import build
#       added by shiraz start ********
from oauth2client.client import GoogleCredentials
#       added by shiraz end ********
import json
from google.auth import default
from google.cloud import storage

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    
    # Extract metadata from the event
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket_name = data["bucket"]
    file_name = data["name"]
    metageneration = data.get("metageneration")
    time_created = data.get("timeCreated")
    updated = data.get("updated")

    region = "us-east1"
    project = "avid-atlas-465409-b8"
    template_path = "gs://dataflow-templates-us-east1/latest/GCS_Text_to_BigQuery"

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {time_created}")
    print(f"Updated: {updated}")

    # Prevent re-processing moved files
    if file_name.startswith("dev11-source-etl/"):
        print(f"File {file_name} is already processed. Skipping Dataflow job.")
        return

    try:
        # Authenticate and build Dataflow service
        credentials, _ = default()

#        dataflow = build('dataflow', 'v1b3', credentials=credentials)

#       added by shiraz start ********
        http = httplib2.Http(timeout=50)
        credentials = GoogleCredentials.get_application_default()
        http = credentials.authorize(http)
        dataflow = build('dataflow', 'v1b3', http=http)
#       added by shiraz end ********

        # Set Dataflow job parameters
        template_body = {
            "jobName": f"cf-bq-load-{event_id[:8]}",
            "parameters": {
                "javascriptTextTransformGcsPath": "gs://dev11-trading-dataflow-metadata/udf.js",
                "JSONPath": "gs://dev11-trading-dataflow-metadata/bq.json",
                "javascriptTextTransformFunctionName": "transform",
                "outputTable": "avid-atlas-465409-b8.dev1_gcs_sink.dev1-df-cf-trading-data",
                "inputFilePattern": f"gs://{bucket_name}/{file_name}",
                "bigQueryLoadingTemporaryDirectory": "gs://dev11-trading-dataflow-metadata",
            }
 #       added by shiraz start ********
                ,"environment": {
                "tempLocation": "gs://dev11-trading-dataflow-metadata/temp"
                }
 #       added by shiraz end ********
        }

        print(f"Launching Dataflow with body:\n{json.dumps(template_body, indent=2)}")

        response = dataflow.projects().locations().templates().launch(
            projectId=project,
            location=region,
            gcsPath=template_path,
            body=template_body
        ).execute()

        print(f"Dataflow job launched successfully:\n{json.dumps(response, indent=2)}")

    except Exception as e:
        print(f"Error: {str(e)}")