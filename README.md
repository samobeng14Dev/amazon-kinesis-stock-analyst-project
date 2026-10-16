# amazon-kinesis-stock-analyst-project

## Python scripts

- `project-folder/stream_stock_data.py` fetches stock prices and sends them to
  Amazon Kinesis.
- `project-folder/process_kinesis_stock_records.py` processes Kinesis records,
  archives raw payloads in S3, and stores calculated metrics in DynamoDB.
- `project-folder/stock_trend_alert_lambda.py` calculates moving averages and
  publishes stock trend alerts through SNS.

For AWS Lambda deployments, set the handler to
`process_kinesis_stock_records.lambda_handler` for the Kinesis record processor
or `stock_trend_alert_lambda.lambda_handler` for the trend alert function.
