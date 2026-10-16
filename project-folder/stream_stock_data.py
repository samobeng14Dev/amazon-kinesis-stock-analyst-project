"""Stream stock data to Amazon Kinesis every few seconds.

Security notes:
- No credentials live in this file. boto3 finds them automatically
  (`aws configure`, which stores them outside the repo, or an IAM role).
- Stream name, region, symbol and delay come from environment variables
  (or a local .env file that is git-ignored).
"""
import json
import logging
import os
import time

import boto3
import yfinance as yf
from botocore.exceptions import BotoCoreError, ClientError

try:
    from dotenv import load_dotenv

    load_dotenv()  # reads a local .env if present; harmless if not
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stock-streamer")

# --- Configuration (no secrets) ---
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "stock-market-stream")
AWS_REGION = os.getenv("AWS_REGION")  # must match the region of your stream
STOCK_SYMBOL = os.getenv("STOCK_SYMBOL", "AAPL")
DELAY_TIME = int(os.getenv("DELAY_SECONDS", "30"))

# region_name=None lets boto3 fall back to AWS_DEFAULT_REGION or your AWS config
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)


def get_stock_data(symbol):
    """Fetch the latest daily bar plus the previous close. Returns a dict or None."""
    try:
        # 2 days to get previous close
        data = yf.Ticker(symbol).history(period="2d")
        if len(data) < 2:
            raise ValueError("Insufficient data to fetch previous close.")

        latest, prev = data.iloc[-1], data.iloc[-2]
        change = latest["Close"] - prev["Close"]

        return {
            "symbol": symbol,
            "open": round(float(latest["Open"]), 2),
            "high": round(float(latest["High"]), 2),
            "low": round(float(latest["Low"]), 2),
            "price": round(float(latest["Close"]), 2),
            "previous_close": round(float(prev["Close"]), 2),
            "change": round(float(change), 2),
            "change_percent": round(float(change / prev["Close"] * 100), 2),
            "volume": int(latest["Volume"]),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    except Exception as e:  # network or data errors should not crash the loop
        log.warning("Error fetching stock data: %s", e)
        return None


def send_to_kinesis():
    log.info("Streaming %s to stream '%s' every %ss. Ctrl+C to stop.",
             STOCK_SYMBOL, STREAM_NAME, DELAY_TIME)
    while True:
        stock_data = get_stock_data(STOCK_SYMBOL)
        if stock_data is None:
            log.info("Skipping this iteration (no data).")
        else:
            try:
                response = kinesis_client.put_record(
                    StreamName=STREAM_NAME,
                    Data=json.dumps(stock_data),
                    PartitionKey=STOCK_SYMBOL,
                )
                # Log only what's useful; the full response contains request IDs and headers
                log.info("Sent %s price=%s shard=%s",
                         stock_data["symbol"], stock_data["price"], response["ShardId"])
            except (ClientError, BotoCoreError) as e:
                log.error("Kinesis error: %s", e)
        time.sleep(DELAY_TIME)


if __name__ == "__main__":
    try:
        send_to_kinesis()
    except KeyboardInterrupt:
        log.info(
            "Stopped by user. Remember to delete the Kinesis stream if you're done.")
