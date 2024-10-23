import streamlit as st

#Presentation
st.title(":blue[Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue for Data Engineers]")
st.header(":rainbow[Problem Statement]")
st.markdown("In this project, the goal is to design and implement an ETL (Extract, Transform, Load) pipeline that integrates data from different file formats (CSV, JSON, and XML), applies various transformations such as unit conversions, and loads the transformed data into cloud storage solutions like AWS S3 and a relational database (AWS RDS) for long-term persistence. Additionally, ensure logging for effective monitoring and troubleshooting of the ETL operations.")    
st.subheader(":orange[Technologies Used:]")
st.markdown("Python, AWS , SQL ")    
st.subheader(":orange[Project Workflow:]")
st.markdown("Data retrieval from the specified link, transformation, and loading into AWS S3 and RDS, with optional AWS Glue automation. Additionally, a logging mechanism will be implemented to monitor and track the progress of ETL operations for transparency and troubleshooting.")   
st.subheader(":orange[Task performed to complete the project:]")
st.markdown("""
- **:green[Data Extraction]** - Use wget or curl to download the data in three different formats.Once the data is retrieved, unzip the files and store it in s3 bucket.
- **:green[Data Transformation]** - Clean the extracted data by removing duplicates, handling missing values, and normalizing text fields. Perform unit conversions (e.g., converting temperature units, currency formats, or date-time zones) using Python functions. Transform data into a consistent structure for easy loading into databases.
- **:green[Loading to AWS S3 & RDS ]** - Save the transformed data as CSV. Use the AWS SDK (boto3 in Python) to upload the files to AWS S3.Connect to AWS RDS (MySQL/PostgreSQL) using libraries like SQLAlchemy.Use to_sql commands to load the data into structured tables in RDS. Ensure data consistency by validating records before loading
- **:green[Logging and Monitoring]** - Use Python’s logging library to track the progress of the extraction, transformation, and loading phases.Save the logs in a text file and optionally upload them to S3 for centralized log storage.                                               
""")   
st.subheader(":orange[Task performed in Python and Aws Environment:]")
st.markdown("""
**:red[1.Data Extraction]** \n
:green[code:] wget -O "D:/Guvi/vscode/source.zip" https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip \n
Expand-Archive -Path "D:\Guvi\vscode\source.zip" -DestinationPath "D:\Guvi\Vscode\source" \n
:violet[Fetch CSV file from s3 into python Env]\n
obj = s3.Bucket('me32-etl-bucket').Object('source1.csv').get()\n   
:violet[Fetch Json file from s3 into python Env]\n
obj = s3.Bucket('me32-etl-bucket').Object('source2.json').get()\n
:violet[Fetch xml file from s3 into python Env]\n    
obj = s3.Bucket('me32-etl-bucket').Object('source3.xml').get()\n                                         
**:red[2.Data Transformation]** \n
:green[code:] Changing datatypes - result['height'] = result['height'].astype(float) \n
:violet[Transformations - Convert inches to Metres]\n    
result['height']=round(result['height']/39.37,2)\n       
:violet[Transformations - Renamed the column names,Capitalize First Letter of Name column and sort by Name]\n    
result.rename(columns={"name":"Name","height":"Height(Metres)","weight":"Weight(Kilograms)"},inplace = True )\n
result['Name'] = result['Name'].str.capitalize()\n
result = result.sort_values(by='Name')\n                                                 
**:red[3.Loading to AWS S3]** \n
:green[code:] df.to_csv("transformed_output.csv", index=False) \n
:violet[Loading - upload transformed data into new s3 bucket]\n    
s3.Bucket('etl-transformed-bucket').upload_file(Key='transformed_data.csv',Filename='transformed_output.csv')\n  
**:red[4.Create the table in RDS and load the data]** \n
:green[code:] df.to_sql('tranformed_data', con=engine,index=False, if_exists='replace') \n
**:red[5.Querying the data \n]**\n
:green[code:] with engine.connect() as connection:\n
result1 = connection.execute(text("select * from tranformed_data where name like 'a%'"))\n
output1 = result1.fetchall()\n                             
**:red[6.Logging and Monitoring]** \n
:green[code:] logging.basicConfig(level=logging.INFO)\n
logger = logging.getLogger('etl_logger')\n
logger.setLevel(logging.INFO)\n
file_handler = logging.FileHandler('etl_log.txt')  \n    
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')\n                                                                
""") 
st.subheader("", divider=True)
st.subheader("Thank you :smile: !!", divider=True)
