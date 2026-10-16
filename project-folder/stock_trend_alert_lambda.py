import boto3
import json
import decimal
from datetime import datetime, timedelta

# --- AWS Clients ---
# DynamoDB resource for querying stock data
dynamodb = boto3.resource("dynamodb")
# SNS client for publishing trend alerts
sns = boto3.client("sns")

# --- Resource Identifiers ---
TABLE_NAME = "stock-market-data"  # DynamoDB table name
# SNS topic ARN for alerts
SNS_TOPIC_ARN = "arn:aws:sns:eu-north-1:402545554517:Stock_Trend_Alerts"


def get_recent_stock_data(symbol, minutes=5):
    """
    Fetch recent stock data for a given symbol from DynamoDB.
    Uses Query (efficient) instead of Scan.
    Retrieves items from the last 'minutes' window.
    """
    table = dynamodb.Table(TABLE_NAME)
    now = datetime.utcnow()
    past_time = now - timedelta(minutes=minutes)

    try:
        response = table.query(
            KeyConditionExpression="symbol = :symbol AND #ts >= :time",
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={
                ":symbol": symbol,
                ":time": past_time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            ScanIndexForward=True  # Sort ascending by timestamp
        )
        # Return items sorted by timestamp
        return sorted(response.get("Items", []), key=lambda x: x["timestamp"])

    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return []


def calculate_moving_average(data, period):
    """
    Calculate simple moving average (SMA) for the given period.
    Returns Decimal(0) if insufficient data.
    """
    if len(data) < period:
        return decimal.Decimal("0")
    return sum(decimal.Decimal(d["price"]) for d in data[-period:]) / period


def lambda_handler(event, context):
    """
    Main Lambda entry point.
    For each stock symbol:
      - Fetch recent data
      - Calculate SMA(5) and SMA(20)
      - Compare with previous values
      - Detect trend changes (uptrend/downtrend)
      - Publish SNS alert if trend change detected
    """
    symbols = ["AAPL"]  # Extend this list with more stock symbols if needed

    for symbol in symbols:
        stock_data = get_recent_stock_data(symbol)

        # Ensure enough data points exist for SMA(20)
        if len(stock_data) < 20:
            continue

        # --- Compute Current Moving Averages ---
        sma_5 = calculate_moving_average(stock_data, 5)
        sma_20 = calculate_moving_average(stock_data, 20)

        # --- Compute Previous Moving Averages (exclude latest record) ---
        sma_5_prev = calculate_moving_average(stock_data[:-1], 5)
        sma_20_prev = calculate_moving_average(stock_data[:-1], 20)

        # --- Detect Trend Change ---
        message = None
        if sma_5_prev < sma_20_prev and sma_5 > sma_20:
            message = f"{symbol} is in an **Uptrend**! Consider a buy opportunity."
        elif sma_5_prev > sma_20_prev and sma_5 < sma_20:
            message = f"{symbol} is in a **Downtrend**! Consider selling."

        # --- Publish SNS Alert ---
        if message:
            try:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject=f"Stock Alert: {symbol}"
                )
                print(f"SNS Alert published: {message}")
            except Exception as e:
                print(f"Failed to publish SNS message: {e}")

    # --- Lambda Response ---
    return {"statusCode": 200, "body": json.dumps("Trend analysis complete")}
