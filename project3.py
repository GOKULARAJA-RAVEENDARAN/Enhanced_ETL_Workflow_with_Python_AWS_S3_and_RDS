import os
import pandas as pd
import logging
import boto3
from sqlalchemy import create_engine
from datetime import datetime
import xml.etree.ElementTree as ET
import credentials  # Import the credentials module

# Retrieve environment variables from credentials.py
AWS_ACCESS_KEY_ID = credentials.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = credentials.AWS_SECRET_ACCESS_KEY
AWS_REGION = credentials.AWS_REGION
S3_BUCKET_NAME = credentials.S3_BUCKET_NAME

RDS_HOST = credentials.RDS_HOST
RDS_PORT = credentials.RDS_PORT
RDS_USERNAME = credentials.RDS_USERNAME
RDS_PASSWORD = credentials.RDS_PASSWORD
RDS_DB_NAME = credentials.RDS_DB_NAME

# Logging setup
log_file = "etl_pipeline.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log(msg):
    print(msg)
    logging.info(msg)

def upload_raw_files_to_s3(folder_path):     #uploading to s3
    log("Uploading raw files to S3...")
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)

    file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    for file_path in file_list:
        filename = os.path.basename(file_path)
        s3.upload_file(file_path, S3_BUCKET_NAME, f"raw/{filename}")
        log(f"Uploaded {filename} to s3://{S3_BUCKET_NAME}/raw/")
    log("All raw files uploaded to S3.")

def download_raw_files_from_s3(destination_folder="downloaded_raw"):     #downloading from s3 to local
    log("Downloading raw files from S3...")
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)

    objects = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="raw/")
    for obj in objects.get("Contents", []):
        filename = obj["Key"].split("/")[-1]
        download_path = os.path.join(destination_folder, filename)
        s3.download_file(S3_BUCKET_NAME, obj["Key"], download_path)
        log(f"Downloaded {filename} to {download_path}")
    return destination_folder

def parse_and_transform(folder_path):                    #parsing through different files types and converting the units 
    log("Transforming data...")
    file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    df_list = []

    for file_path in file_list:
        ext = file_path.lower().split(".")[-1]
        if ext == "csv":
            df = pd.read_csv(file_path)
        elif ext == "json":
            df = pd.read_json(file_path,lines=True)
        elif ext == "xml":
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()

                # Look for all <person> elements under <data>
                records = [{child.tag: child.text for child in person} for person in root.findall("person")]

                if records:
                    df = pd.DataFrame(records)
                    df_list.append(df)
                else:
                    print(f"WARNING: No <person> elements found in XML file: {file_path}")
            except Exception as e:
                print(f"ERROR parsing XML file {file_path}: {e}")
        else:
            continue
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True).drop_duplicates()   #Dropping duplicates since we have 3 sets of same data

    if "height" in combined_df.columns:
        combined_df["height"] = (combined_df["height"].astype(float) * 0.0254).round(2)

    if "weight" in combined_df.columns:
        combined_df["weight"] = (combined_df["weight"].astype(float) * 0.453592).round(2)


    output_path = "transformed_output.csv"
    combined_df.to_csv(output_path, index=False)
    log(f"Transformed data saved to {output_path}")
    return output_path

def upload_transformed_to_s3(file_path):           #transformed file copy in s3
    log("Uploading transformed file to S3...")
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)
    filename = os.path.basename(file_path)
    s3.upload_file(file_path, S3_BUCKET_NAME, f"transformed/{filename}")
    log("Transformed file uploaded to S3.")

def load_to_rds(file_path):                             
    log("Loading transformed data into RDS...")
    df = pd.read_csv(file_path)

    engine = create_engine(
        f"mysql+mysqlconnector://{RDS_USERNAME}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB_NAME}"
    )
    df.to_sql("etl_transformed_data", con=engine, if_exists='replace', index=False)
    log("Data loaded into RDS table: etl_transformed_data")

def upload_log_to_s3():                  #Storing logs in s3 for reference
    log("Uploading log file to S3...")
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)
    s3.upload_file(log_file, S3_BUCKET_NAME, f"logs/{log_file}")
    log("Log file uploaded to S3.")

def run_pipeline(folder_path):       #main function running the etl pipeline
    log("=== ETL Pipeline Started ===")
    upload_raw_files_to_s3(folder_path)
    downloaded_path = download_raw_files_from_s3()
    transformed_file = parse_and_transform(downloaded_path)
    upload_transformed_to_s3(transformed_file)
    load_to_rds(transformed_file)
    upload_log_to_s3()
    log("=== ETL Pipeline Completed ===")
    log("#########################################################################################")

# Calling etl pipeline
run_pipeline("source")