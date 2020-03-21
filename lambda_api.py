import boto3
import os
import json

TABLE_NAME = os.environ['TableName']
# Get the environment, so we can run appropriate scripts
IS_DEVELOPMENT = os.environ['STAGE'] == 'DEVELOPMENT'


def get_dynamo_db_client():
    """ Set the dynamodb instance based on environment"""
    if IS_DEVELOPMENT:
        # Set the boto3 session for dynamodb if its development environment
        dynamo_db_session = boto3.Session(profile_name="default")
        return dynamo_db_session.resource('dynamodb')
    else:
        return boto3.resource("dynamodb")


DYNAMO_DB = get_dynamo_db_client()


class Error(Exception):
    """Base class for other exceptions"""
    pass


class NoRecordError(Error):
    """Raised when no record found"""
    pass


class ResultEntity:
    def __init__(self, RecordId, Status, Notes=None):
        """
        This function used to initiate values to the class variables
        """
        self.RecordId = RecordId
        self.Status = Status
        # Only return other values when status is `OK`
        if Status == "OK":
            self.Notes = Notes


class DbEntity:
    def __init__(self, record_id, record_type):
        """
        This function used to initiate values to the class variables
        """
        self.RecordId = record_id
        self.RecordType = record_type


def lambda_handler(event, context):
    """
    This lambda function will check the provided record status in dynamodb and respond with the status message
    :param event: RecordId
    :param context:
    :return: JSON of statusCode, body, error
    """
    try:
        record_id = event['RecordId']
        record_type = "global"
        record = get_record_status(DbEntity(record_id, record_type))
        result = ResultEntity(**record)

        return {
            'statusCode': 200,
            'body': json.dumps(result, default=lambda o: o.__dict__)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'error': str(e)
        }


def get_record_status(item):
    """
    This function will check the entry in dynamodb if exist will return the status of the entry
    :param instance_item: RecordId, RecordType
    :return: status of the given RecordId or return NoRecordError Exception
    """
    table = DYNAMO_DB.Table(TABLE_NAME)

    base_instance_item = table.get_item(
        Key={
            'RecordId': item.RecordId,
            'RecordType': item.RecordType
        },
        ProjectionExpression='RecordId, Notes, #s',
        ExpressionAttributeNames={
            "#s": "Status"
        },
        ConsistentRead=True)

    if "Item" not in base_instance_item:
        raise NoRecordError("No Records Found.")
    else:
        return base_instance_item['Item']