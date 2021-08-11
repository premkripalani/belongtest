#Download Data to C: drive
#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.melbourne.vic.gov.au", None)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.melbourne.vic.gov.au,
#                  MyAppToken,
#                  userame="user@example.com",
#                  password="AFakePassword")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("b2ak-trbp", limit=20000000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

#DOWNLOAD FILE ON C: DRIVE
file_name = "Downloads\mydata.json" 
results_df.to_json(file_name)


# Creating the low level functional client
import boto3
import pandas
client = boto3.client(
    's3',
    aws_access_key_id = '******************',
    aws_secret_access_key = '**************************',
    region_name = 'ap-south-1'
)


# Create the S3 object
obj = client.get_object(
    Bucket = 'beelongtest',
    Key = 'mydata.json'
)
    
# Read data from the S3 object
data = pandas.read_json(obj['Body'])
    
# Print the data frame
print('Printing the data frame...')
print(data)

#Query for Q1 - #Top 10 (most pedestrians) locations by day
temp = data.groupby(['sensor_id', 'day'])[['day']].count()
temp.columns = ["count"]
output = temp.sort_values('count', ascending=False)
output.head(10)



#Query for Q2 - (most pedestrians) locations by month## Load- Data into S3 in an appropriate format for future querying
monthly = data.groupby(['sensor_id', 'month'])[['month']].count()
monthly.columns = ["month_count"]
output = monthly.sort_values('month_count', ascending=False)
output.head(10)
output.head(10).to_csv('Downloads\month.csv')

#UPLOAD FILE TO S3from io import StringIO # python3; python2: BytesIO 
import boto3
bucket = 'beelongtest' # already created on S3
csv_buffer = StringIO()
output.head(10).to_csv(csv_buffer)
s3_resource = boto3.resource('s3', aws_access_key_id = 'AKIARPYMGMBUMW5KT5K3',
    aws_secret_access_key = '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',
    region_name = 'ap-south-1')
s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())