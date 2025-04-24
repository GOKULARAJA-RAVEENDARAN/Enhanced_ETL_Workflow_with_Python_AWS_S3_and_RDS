# Enhanced_ETL_Workflow_with_Python-AWS_S3_and_RDS

  This project is an end-to-end ETL (Extract, Transform, Load) pipeline built using Python, AWS S3,AWS RDS, and SQLAlchemy. 
  It processes structured data from CSV, JSON, and XML files, transforms it (including unit conversions), 
and loads the final dataset into a MySQL database on AWS RDS. The entire process is logged and all logs are stored in S3.


Project Structure:
- etl_pipeline.py         # Main script for the ETL pipeline
- transformed_output.csv  # Output file generated after transformation
- etl_pipeline.log        # Log file for monitoring pipeline activity
- /downloaded_raw         # Auto-created folder where raw files are downloaded from S3

Features:
- Extract files from local folder (CSV, JSON, XML)- Upload raw files to AWS S3 (raw/ folder)
- Download files from S3 to a local folder
- Parse and transform data:
- Combines files
- Removes duplicates
- Converts height (inches to cm) and weight (pounds to kg)
- Save transformed data locally
- Upload transformed file to S3 (transformed/ folder)
- Load final data into AWS RDS (MySQL)
- Upload logs to S3 (logs/ folder)

- - Required Python libraries:
  pip install pandas boto3 sqlalchemy mysql-connector-python

How to Run the Pipeline:
 python project3.py
