import boto3
import pytest
import json

from moto import mock_dynamodb2
from lambda_api import lambda_handler

TXNS_TABLE = "lambda-table-for-blog"

@pytest.yield_fixture(scope="function")
def dynamo_db_fixture():
    mock_dynamodb2().start()
    client = boto3.client("dynamodb", region_name='eu-west-2')
    resource = boto3.resource("dynamodb", region_name='eu-west-2')

    # Create the table
    resource.create_table(
        TableName=TXNS_TABLE,
        KeySchema=[
            {
                'AttributeName': 'RecordId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RecordType',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'RecordId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RecordType',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    yield client, resource

    mock_dynamodb2().stop()


def test_handler_for_failure(dynamo_db_fixture):
    event = {
        "RecordId": "DonOfDen001"
    }

    return_data = lambda_handler(event, "")
    assert return_data['statusCode'] == 500
    assert return_data['error'] == 'No Records Found.'


def test_handler_for_status_ok(dynamo_db_fixture):
    table = boto3.resource('dynamodb', region_name='eu-west-2').Table(TXNS_TABLE)
    table.put_item(
        Item={
            'RecordId': "DonOfDen002",
            'RecordType': "global",
            'Status': "OK",
            'Notes': "DonOfDen Test Blog! - Unittest"
        }
    )

    event = {
        "RecordId": "DonOfDen002"
    }

    return_data = lambda_handler(event, "")
    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen002'
    assert body['Status'] == 'OK'
    assert body['Notes'] == 'DonOfDen Test Blog! - Unittest'


def test_handler_for_different_status(dynamo_db_fixture):
    table = boto3.resource('dynamodb', region_name='eu-west-2').Table(TXNS_TABLE)
    table.put_item(
        Item={
            'RecordId': "DonOfDen008",
            'RecordType': "global",
            'Status': "DRAFT",
            'Notes': "DonOfDen Test Blog - Unit Test for status not equals to OK"
        }
    )

    event = {
        "RecordId": "DonOfDen008"
    }

    return_data = lambda_handler(event, "")
    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen008'
    assert body['Status'] == 'DRAFT'
    assert 'Notes' not in body