import json
import base64
import logging
import boto3


DNDB_SALES_TABLE_NAME = 'Sales'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
  try:
    processed_sales = process_sales_event(event)
    save_sales_data(processed_sales)
  except Exception as e:
    logger.error(e, exc_info=True)
    raise Exception('Error occurred during execution')

def process_sales_event(sales_event) :
  """
    process_sales_event process the sales event message (MSK) received in parameter
      
    :param sales_event : sales event
    :return: the list of processed sales data
  """ 
  processed_sales = []
  
  # Loop over partition
  for partition in sales_event['records'] :
    logger.info(f"Processing partition : {partition}") 
    
    # Loop over messages in partition
    for sales in sales_event['records'][partition] :
      sales_data = json.loads(base64.b64decode(sales['value']).decode('utf-8'))
      logger.info(f"Sales data in message :  {sales_data}")
      processed_sales.append(sales_data)

  return processed_sales
  
  
def save_sales_data(sales_data) :
  """
    save_sales_data put the sales data in DynamoDB
    
    :param sales_data : list of sales data
  """
  logger.info(f"Saving data in DynamoDB table  : {DNDB_SALES_TABLE_NAME}")
  
  table = dynamodb.Table(DNDB_SALES_TABLE_NAME)
  
  with table.batch_writer() as batch:
    
    for data in sales_data :
      batch.put_item(Item=data)
