import json
import os
import pandas as pd
import boto3
import io
import sys


s3_client = boto3.client("s3")
glue_client = boto3.client("glue")
sns_client = boto3.client('sns')



def extract_outputdirpath(event):
    """Function to extract required data from s3 event
       param event : json event 
    """
    filename = event["Records"][0]['s3']['object']['key']
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]

    
    event_time = event["Records"][0]["eventTime"]
    date = event_time.split("T")[0]
    time = event_time.split("T")[1]
    day = date.split("-")[2]
    month = date.split("-")[1]
    year = date.split("-")[0]
    hour = time.split(":")[0]
    
    return day,month,year,hour,filename,bucket_name


def send_notification(notification):
    response = sns_client.publish(
        TargetArn = os.environ['sns_arn'],
        Message = json.dumps({'default': notification}),
        MessageStructure = 'json'
        )

def validate_schema(daily_data):
    
    desired_schema = os.environ['schema'].split(",")
    current_schema = list(daily_data.columns)
    if set(current_schema)==set(desired_schema):
        return True
    else:
        return False
        
def upload_to_s3(df,key,s3_client):
    
    bucket = os.environ['output_bucket']
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket,Key=key,Body= csv_buffer.getvalue())
        
def lambda_handler(event, context):

    day,month,year,hour,filename,bucket_name = extract_outputdirpath(event)
    

    if filename.split(".")[1] == "csv":
        response = s3_client.get_object(Bucket=bucket_name,Key=filename)
        daily_data = pd.read_csv(response.get("Body"))
        go_ahead = validate_schema(daily_data)
        print(daily_data.columns,go_ahead)

        output_key = f"output/year={year}/month={month}/day={day}/hour={hour}/{filename}"
        if go_ahead:            
            upload_to_s3(df=daily_data,key=output_key,s3_client=s3_client)
            print("File Uploaded")
            

        else:
            send_notification("Schema Validation Failed")
            
    else:
        send_notification("Invalid File Extension")