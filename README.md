# Capstone-Project-4
<h1>Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue for Data Engineers<br/></h1>

**Problem Statement:** <br/> This project focuses on building a reliable ETL (Extract, Transform, Load) pipeline that extracts data from various formats such as CSV, JSON, and XML. The extracted data transforms, including unit conversions, to ensure consistency and accuracy. The transformed data is then loaded into AWS S3 for scalable storage and AWS RDS for relational data management. AWS Glue can be used to automate parts of the ETL process to enhance automation and scalability. Additionally, a logging mechanism will be implemented to monitor and track the progress of ETL operations for transparency and troubleshooting.<br/>

**Technologies Used:** <br/> Python, AWS , SQL.<br/>

**Project Workflow:** <br/>Data retrieval from the specified link, transformation, and loading into AWS S3 and RDS, with optional AWS Glue automation. Additionally, a logging mechanism will be implemented to monitor and track the progress of ETL operations for transparency and troubleshooting.

**Task performed to complete the project:** <br/> **1. Data Extraction** - Input: CSV, JSON, and XML files. Use Python libraries like pandas, JSON, and xml.etree.ElementTree.ElementTree to read data from different file formats. Read files from a designated directory or pull them from an external source if necessary.<br/>
**2. Data Transformation** - Clean the extracted data by removing duplicates, handling missing values, and normalizing text fields. Perform unit conversions (e.g., converting temperature units, currency formats, or date-time zones) using Python functions. Transform data into a consistent structure for easy loading into databases. <br/>
**3. Loading to AWS S3** - Save the transformed data as CSV, Parquet, or JSON files. Use the AWS SDK (boto3 in Python) to upload the files to AWS S3. Define appropriate permissions, bucket policies, and version control for S3. <br/>
**4. Loading to AWS RDS** - Connect to AWS RDS (MySQL/PostgreSQL) using libraries like psycopg2 or SQLAlchemy.Use SQL INSERT or COPY commands to load the data into structured tables in RDS. Ensure data consistency by validating records before loading. <br/>
**5. Logging and Monitoring** - Implement logging (using Python's logging module or AWS CloudWatch) to track ETL progress. Log successful data loads, transformations, and errors for debugging and monitoring purposes. Ensure that the entire pipeline is logged, and store logs either locally or in S3. <br/>



