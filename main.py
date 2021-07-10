import sys
import boto3
import json

# kinesis data stream and SQS client
kinesis_client = boto3.client('kinesis')
sqs_client = boto3.client('sqs')

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
            print(f"{len(records['Records'])} records available in batch")
            if process_failed_records(records, json_body["KinesisBatchInfo"]["endSequenceNumber"], stream_name):
                deleteSQSMessage(message["ReceiptHandle"])
        else:
            print(f"getrecords returned zero records")

# get failed batch from SQS
def retrieve_failed_batch():

    sqs_response = sqs_client.receive_message(
        QueueUrl=config["SQS"]["QUEUEURL"],
        VisibilityTimeout=30
    )
    return sqs_response


def get_failed_records(batch_info, stream_name):
    
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

    return records


def process_failed_records(records, endSequenceNumber, stream_name):
    putRecordCount = 0
    recordsForReingestion = []
    for record in records["Records"]:

        if record["SequenceNumber"] > endSequenceNumber:
            print(f"breaking loop")
            break
        rec = {
            'Data':record["Data"],
            'PartitionKey':record["PartitionKey"]
        }
        recordsForReingestion.append(rec)
        putRecordCount = putRecordCount + 1

        if putRecordCount == 500:
            print(f"ingesting 500 records")
            success = reIngetsRecords(recordsForReingestion, stream_name)
            if success is False:
                return success
            putRecordCount = 0
            recordsForReingestion = []
        if sys.getsizeof(recordsForReingestion) > 655360:
            print(f"ingesting 5MB data")
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
        print(f"no records to reingest")

    return success

def reIngetsRecords(rec, stream_name):
    putRecord_response = kinesis_client.put_records(
        Records=rec,
        StreamName=stream_name
    )

    if putRecord_response["FailedRecordCount"] > 0:
        print(f"Failed record count = {putRecord_response['FailedRecordCount']}")
        return False
    else:
        print(f"successfully reingested {len(putRecord_response['Records'])} records")
        return True

def deleteSQSMessage(handle):
    sqs_client.delete_message(
        QueueUrl=config["SQS"]["QUEUEURL"],
        ReceiptHandle=handle
    )

if __name__ == "__main__":
    main()
