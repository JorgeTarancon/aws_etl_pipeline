######################## LIBRARIES ########################
import json
import boto3
import pandas as pd
from io import StringIO
from functions import download_and_load_query_results
######################## LIBRARIES ########################

# Load JSON config
with open("config.json","r") as config: config = json.load(config)
region = config["AWS_REGION"]
bucket = config["S3_BUCKET_NAME_OUTPUT"]
redshift_role = config["REDSHIFT_ADMINISTRATOR_USER"]["ROLE"]

# Initiate athena client
athena_client = boto3.client(
                                "athena",
                                aws_access_key_id=config["NON_ADMINISTRATOR_USER"]["ACCESS_KEY"],
                                aws_secret_access_key=config["NON_ADMINISTRATOR_USER"]["SECRET_KEY"],
                                region_name=region
                            )

# Initiate S3 client
s3_client = boto3.client(
                                "s3",
                                aws_access_key_id=config["NON_ADMINISTRATOR_USER"]["ACCESS_KEY"],
                                aws_secret_access_key=config["NON_ADMINISTRATOR_USER"]["SECRET_KEY"],
                                region_name=region
                            )

# Initiate redshift client
redshift_client = boto3.client(
                                "redshift-data",
                                aws_access_key_id=config["NON_ADMINISTRATOR_USER"]["ACCESS_KEY"],
                                aws_secret_access_key=config["NON_ADMINISTRATOR_USER"]["SECRET_KEY"],
                                region_name=region)

csv_buffer = StringIO() # Create a buffer to tore data in memory for loading it into S3

# Query Athena tables, store results in pandas DataFrames, upload data into S3
# and upload data from S3 into Redshift tables
for sql_file in ["fact_covid","dim_date","dim_hospital","dim_region"]:
    # Load query
    with open(f"sql_queries/{sql_file}.sql","r") as query: query = query.read()
    # Run query
    response = athena_client.start_query_execution(
                                                QueryString = query,
                                                QueryExecutionContext = {"Database":config["SCHEMA_NAME"]},
                                                ResultConfiguration = {
                                                    "OutputLocation":config["S3_STAGING_DIR"],
                                                    "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"}
                                                                    }
                                                )

    # Store query in a pandas DataFrame
    exec(f"{sql_file} = download_and_load_query_results(client=athena_client,query_response=response)")
    
    # Store pandas dataframe in S3 bucket
    exec(f"{sql_file}.to_csv(csv_buffer)")
    s3_client.put_object(Bucket=bucket,Key=f"output/{sql_file}.csv",Body=csv_buffer.getvalue())

    # Obtain sql statement to create SQL table with de DataFrame schema
    exec( f"sql_statement = pd.io.sql.get_schema({sql_file}.reset_index(),f'{sql_file}')" )

    # Create SQL table in Redshift
    redshift_client.execute_statement(
                                        ClusterIdentifier=config['REDSHIFT_ADMINISTRATOR_USER']['CLUSTER'],
                                        Database=config['REDSHIFT_ADMINISTRATOR_USER']['DATABASE'],
                                        DbUser=config['REDSHIFT_ADMINISTRATOR_USER']['USER'],
                                        Sql=sql_statement)

    # Feed SQL created table with S3 data
    response = redshift_client.execute_statement(
                                        ClusterIdentifier=config['REDSHIFT_ADMINISTRATOR_USER']['CLUSTER'],
                                        Database=config['REDSHIFT_ADMINISTRATOR_USER']['DATABASE'],
                                        DbUser=config['REDSHIFT_ADMINISTRATOR_USER']['USER'],
                                        Sql=f"""
                                            copy {sql_file} from 's3://{bucket}/output/{sql_file}.csv'
                                            credentials '{redshift_role}'
                                            delimiter ','
                                            region '{region}'
                                            IGNOREHEADER 1
                                            """)

# Close connections
athena_client.close()
s3_client.close()
redshift_client.close()