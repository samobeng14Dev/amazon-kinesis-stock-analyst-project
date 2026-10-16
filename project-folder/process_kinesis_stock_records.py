import json
import boto3
import base64
from decimal import Decimal

# --- Initialize AWS Clients ---
# DynamoDB resource for storing processed stock data
dynamodb = boto3.resource("dynamodb")
# S3 client for saving raw event payloads
s3 = boto3.client("s3")

# --- Resource Names ---
# DynamoDB table name where processed metrics will be stored
DYNAMO_TABLE = "stock-market-data"
# S3 bucket name for archiving raw event data
S3_BUCKET = "stock-market-data-bucket-demo1"

# --- Table Reference ---
table = dynamodb.Table(DYNAMO_TABLE)


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Triggered by Kinesis stream events.
    Processes each record, stores raw data in S3,
    computes stock metrics, and saves structured data in DynamoDB.
    """
    for record in event['Records']:
        try:
            # --- Decode base64 Kinesis data ---
            raw_data = base64.b64decode(
                record["kinesis"]["data"]).decode("utf-8")
            payload = json.loads(raw_data)
            print(f"Processing record: {payload}")

            # --- Store raw data in S3 ---
            try:
                # Build S3 key path: raw-data/<symbol>/<timestamp>.json
                s3_key = f"raw-data/{payload['symbol']}/{payload['timestamp'].replace(':', '-')}.json"
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=json.dumps(payload),
                    ContentType='application/json'
                )
                print(f"Raw data saved to S3: {s3_key}")
            except Exception as s3_error:
                # Log any S3 storage errors
                print(f"Failed to save raw data to S3: {s3_error}")

            # --- Compute stock metrics ---
            # Price change compared to previous close
            price_change = round(
                payload["price"] - payload["previous_close"], 2)
            # Percentage change relative to previous close
            price_change_percent = round(
                (price_change / payload["previous_close"]) * 100, 2)
            # Flag anomalies if price change exceeds ±5%
            is_anomaly = "Yes" if abs(price_change_percent) > 5 else "No"
            # Simple moving average of OHLC + current price
            moving_average = (
                payload["open"] + payload["high"] + payload["low"] + payload["price"]) / 4

            # --- Structured data for DynamoDB ---
            processed_data = {
                "symbol": payload["symbol"],
                "timestamp": payload["timestamp"],
                "open": Decimal(str(payload["open"])),
                "high": Decimal(str(payload["high"])),
                "low": Decimal(str(payload["low"])),
                "price": Decimal(str(payload["price"])),
                "previous_close": Decimal(str(payload["previous_close"])),
                "change": Decimal(str(price_change)),
                "change_percent": Decimal(str(price_change_percent)),
                "volume": int(payload["volume"]),
                "moving_average": Decimal(str(moving_average)),
                "anomaly": is_anomaly
            }

            # --- Store processed metrics in DynamoDB ---
            table.put_item(Item=processed_data)
            print(f"Stored in DynamoDB: {processed_data}")

        except Exception as e:
            # Catch-all error handler for record processing
            print(f"Error processing record: {e}")

    # --- Lambda Response ---
    return {"statusCode": 200, "body": "Processing Complete"}
