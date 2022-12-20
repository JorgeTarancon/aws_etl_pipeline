######################## LIBRARIES ########################
import boto3
import pandas as pd
import json
import time
######################## LIBRARIES ########################

# Load JSON config
with open("config.json","r") as config: config = json.load(config)

def download_and_load_query_results(client:boto3.client, query_response:dict) -> pd.DataFrame:
    while True:
        try:
            # This function only loads the first 100 rows
            client.get_query_results(QueryExecutionId=query_response["QueryExecutionId"])
            break
        except Exception as err:
            if "not yet finished" in str(err): time.sleep(0.001)
            else: raise err
    temp_file_location:str = config["TEMP_FILES_DIRECTORY"]+"athena_query_results.csv"
    s3_client = boto3.client(
                            "s3",
                            aws_access_key_id=config["NON_ADMINISTRATOR_USER"]["ACCESS_KEY"],
                            aws_secret_access_key=config["NON_ADMINISTRATOR_USER"]["SECRET_KEY"],
                            region_name=config["AWS_REGION"])
    s3_client.download_file(
                            config["S3_BUCKET_NAME_TEMP_FILES"],
                            config["S3_OUTPUT_DIRECTORY"]+"/"+query_response["QueryExecutionId"]+".csv",
                            temp_file_location)
    return pd.read_csv(temp_file_location)