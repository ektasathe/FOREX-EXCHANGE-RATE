
import requests
import json
import boto3
from datetime import datetime

# Alpha Vantage API Configuration
API_KEY = "y5KJC6AN2GNFCGA9N"
BASE_URL = "https://www.alphavantage.co/query"
CURRENCY_PAIR = "EUR/USD"

# AWS S3 Configuration
S3_BUCKET = "forex-data-bucket-v1"
S3_FOLDER = "raw"

# Initialize S3 Client
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
    
    print(f"✅ Data uploaded to S3: s3://{S3_BUCKET}/{file_name}")

# Main Execution
if __name__ == "__main__":
    forex_data = fetch_forex_data()
    if forex_data:
        upload_to_s3(forex_data)