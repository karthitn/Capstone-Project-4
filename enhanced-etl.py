import boto3
import s3fs
import json
import io
import sqlalchemy
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
import credentials
import logging
import xml.etree.ElementTree as ET

s3 = boto3.resource('s3')

#Saving ETL Logs as text file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('etl_logger')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('etl_log.txt')  # Specify the file name
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


#Fetch list of files in the s3 bucket
for obj in s3.Bucket('me32-etl-bucket').objects.all():
    print(obj)   

logger.info("Starting ETL process")
logger.info("Extracting data from DataFrame...")

#Fetch CSV file from s3 into python Env
obj = s3.Bucket('me32-etl-bucket').Object('source1.csv').get()
csv = pd.read_csv(obj['Body'])
print(csv)

#Fetch Json file from s3 into python Env
obj = s3.Bucket('me32-etl-bucket').Object('source2.json').get()
contents = obj['Body'].read().decode('utf-8')
string_io = io.StringIO(contents)  # Wrap contents in StringIO
json = pd.read_json(string_io, lines=True)
print(json)

#Fetch xml file from s3 into python Env
obj = s3.Bucket('me32-etl-bucket').Object('source3.xml').get()
contents = obj['Body'].read().decode('utf-8')
#Parse the XML Content
root = ET.fromstring(contents)
#Extract data from XML elements
data = []
for element in root.findall('person'):  
    row = {}
    for child in element:
        row[child.tag] = child.text
    data.append(row)
xml = pd.DataFrame(data)
print(xml)    

#Mering data from three different source
result = pd.concat([csv, json, xml],ignore_index=True)
print(result)

logger.info("Extraction completed")

logger.info("Transforming data...")

# Convert columns to numeric
result['height'] = result['height'].astype(float)
result['weight'] = result['weight'].astype(float)

#Convert inches to Metres
result['height']=round(result['height']/39.37,2)

#Convert pounds to kilograms
result['weight']=round(result['weight']/2.205,2)

#Renamed the column names,Capitalize First Letter of Name column and sort by Name
result.rename(columns={"name":"Name","height":"Height(Metres)","weight":"Weight(Kilograms)"},inplace = True )
result['Name'] = result['Name'].str.capitalize()
result = result.sort_values(by='Name')

#saving combined output as dataframe
df = result.reset_index(drop=True)
print(df)

logger.info("Transformation completed")

logger.info("Loading data...")

#Save the df as CSV file
df.to_csv("transformed_output.csv", index=False)

#create new s3 bucket from python for saving transformed data
#s3_client = boto3.client('s3')
bucket_name = 'etl-transformed-bucket'
response = s3.create_bucket(Bucket=bucket_name)

#Fetch list of s3 bucket
# for bucket in s3.buckets.all():
#     print(bucket.name)

#upload transformed data into new s3 bucket
s3.Bucket('etl-transformed-bucket').upload_file(Key='transformed_data.csv',Filename='transformed_output.csv')
logger.info("ETL process finished")

#Fetch file list in the new s3 bucket
# for obj in s3.Bucket('etl-transformed-bucket').objects.all():
#     print(obj)

logger.info("Saving ETL Logs in s3 bucket...")

#Saving logs text file into s3 bucket
s3.Bucket('etl-transformed-bucket').upload_file(Key='etl_logs.txt',Filename='etl_log.txt')
logger.info("Logs saved in s3 bucket...")

#RDS service will be accessible from your SQL workbench and quering table in python
data = credentials.db_url
engine = create_engine(data)
#Create the table in RDS and load the data
df.to_sql('tranformed_data', con=engine,index=False, if_exists='replace')
with engine.connect() as connection:
    result1 = connection.execute(text("select * from tranformed_data where name like 'a%'"))
    output1 = result1.fetchall()
print(output1) 