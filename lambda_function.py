import json 
import boto3 
import requests 
from datetime import datetime 


# API Configuration
API_KEY = "5KJC6AN2GNFCGA9N" 
BASE_URL = "https://www.alphavantage.co/query" 
CURRENCY_PAIR = "EUR/USD" 

# AWS Configuration 

S3_BUCKET = "forex-data-bucket-v1"  # Update with your bucket name 
S3_FOLDER = "raw" 
s3_client = boto3.client("s3") 

def fetch_forex_data(): 
    params = { 
        "function": "CURRENCY_EXCHANGE_RATE", 
        "from_currency": "EUR", 
        "to_currency": "USD", 
        "apikey": API_KEY 
    } 

    response = requests.get(BASE_URL, params=params) 
    data = response.json() 

    if "Realtime Currency Exchange Rate" in data: 
        return data["Realtime Currency Exchange Rate"] 
    else:
        print("Error fetching data:", data) 
        return None 

def upload_to_s3(data): 
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S") 
    file_name = f"{S3_FOLDER}/{timestamp}.json" 

    s3_client.put_object( 
        Bucket=S3_BUCKET, 
        Key=file_name, 
        Body=json.dumps(data), 
        ContentType="application/json" 

    ) 
    print(f"Data uploaded to S3: s3://{S3_BUCKET}/{file_name}") 

def lambda_handler(event, context): 
    forex_data = fetch_forex_data() 

     

    if forex_data: 
        upload_to_s3(forex_data) 
        return { 
            "statusCode": 200, 
            "body": json.dumps("Forex Data Successfully Uploaded to S3!") 
        } 

    else: 
        return { 
            "statusCode": 500, 
            "body": json.dumps("Failed to Fetch Forex Data") 

        } 

 