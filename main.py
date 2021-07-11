import sys
import botocore
import boto3
import json
import logging

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# kinesis data stream and SQS client
kinesis_client = boto3.client('kinesis')
sqs_client = boto3.client('sqs')

# loading config files
with open("config.json", "r") as f:
    config = json.load(f)

def main():
    # get failed record batch from SQS
    sqs_response = retrieve_failed_batch()

    # for each batch perform kinesis getrecords and reingest those records back to kinesis stream
    for message in sqs_response["Messages"]:
        json_body = json.loads(message["Body"])
        stream_name = json_body["KinesisBatchInfo"]["streamArn"].partition("/")[2]

        # read failed records back again from stream
        records = get_failed_records(json_body, stream_name)

        # reingest those failed records
        if len(records["Records"]) > 0:
            logger.info(f"{len(records['Records'])} records available in batch")
            if process_failed_records(records, json_body["KinesisBatchInfo"]["endSequenceNumber"], stream_name):
                # delete SQS message after successful ingestion
                deleteSQSMessage(message["ReceiptHandle"])
        else:
            logger.info(f"getrecords returned zero records")

# get failed batch from SQS
def retrieve_failed_batch():
    try:
        sqs_response = sqs_client.receive_message(
            QueueUrl=config["SQS"]["QUEUEURL"],
            VisibilityTimeout=30
        )
    except botocore.exceptions.ClientError as error:
        display_exceptions(error)

    return sqs_response

# read failed records batch from kinesis data stream
def get_failed_records(batch_info, stream_name):
    try:
        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=batch_info["KinesisBatchInfo"]["shardId"],
            ShardIteratorType='AT_SEQUENCE_NUMBER',
            StartingSequenceNumber=batch_info["KinesisBatchInfo"]["startSequenceNumber"]
        )["ShardIterator"]

        records = kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=batch_info["KinesisBatchInfo"]["batchSize"]
        )
    except botocore.exceptions.ClientError as error:
        display_exceptions(error)

    return records

def process_failed_records(records, endSequenceNumber, stream_name):
    putRecordCount = 0
    recordsForReingestion = []
    for record in records["Records"]:

        if record["SequenceNumber"] > endSequenceNumber:
            logger.info(f"breaking loop")
            break
        rec = {
            'Data':record["Data"],
            'PartitionKey':record["PartitionKey"]
        }
        recordsForReingestion.append(rec)
        putRecordCount = putRecordCount + 1

        # reingest records if record count = 500
        if putRecordCount == 500:
            logger.info(f"ingesting 500 records")
            success = reIngetsRecords(recordsForReingestion, stream_name)
            if success is False:
                return success
            putRecordCount = 0
            recordsForReingestion = []
        
        # reingest records if records size is close to 5MB
        if sys.getsizeof(recordsForReingestion) > 655360:
            logger.info(f"ingesting 5MB data")
            recordsForReingestion.pop()
            success = reIngetsRecords(recordsForReingestion, stream_name)
            if success is False:
                return success
            putRecordCount = 0
            recordsForReingestion = [] 
            recordsForReingestion.append(rec)

    if putRecordCount > 0:
        success = reIngetsRecords(recordsForReingestion, stream_name)
    else:
        logger.info(f"no records to reingest")

    return success

# reingest failed records back to same stream
def reIngetsRecords(rec, stream_name):
    try:
        putRecord_response = kinesis_client.put_records(
            Records=rec,
            StreamName=stream_name
        )
    except botocore.exceptions.ClientError as error:
        display_exceptions(error)

    if putRecord_response["FailedRecordCount"] > 0:
        logger.info(f"Failed record count = {putRecord_response['FailedRecordCount']}")
        return False
    else:
        logger.info(f"successfully reingested {len(putRecord_response['Records'])} records")
        return True

def deleteSQSMessage(handle):
    try:
        sqs_client.delete_message(
            QueueUrl=config["SQS"]["QUEUEURL"],
            ReceiptHandle=handle
        )
    except botocore.exceptions.ClientError as error:
        display_exceptions(error)

# catch all exceptions
def display_exceptions(e):
    if e.response['Error']['Code'] == 'OverLimit':
        logger.warning("maximum number of inflight messages is reached")
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        logger.warning("The requested resource could not be found.")
    elif e.response['Error']['Code'] == 'InvalidArgumentException':
        logger.warning("A specified parameter exceeds its restrictions, is not supported, or can't be used")
    elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
        logger.warning("The request rate for the stream is too high, or the requested data is too large for the available throughput. Reduce the frequency or size of your requests")
    elif e.response['Error']['Code'] == 'ExpiredIteratorException':
        logger.warning("The provided iterator exceeds the maximum age allowed")
    elif e.response['Error']['Code'] == 'InvalidIdFormat':
        logger.warning("The specified receipt handle isn't valid for the current version")
    elif e.response['Error']['Code'] == 'ReceiptHandleIsInvalid':
        logger.warning("The specified receipt handle isn't valid")
    elif "KMS" in e.response['Error']['Code']:
        logger.warning("KMS exception occured")
    
    logger.error(f"Error Message: {e.response['Error']['Message']}")
    logger.info(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
    logger.info(f"Http code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

if __name__ == "__main__":
    main()
