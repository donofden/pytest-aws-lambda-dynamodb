import boto3
import pytest
import json

from moto import mock_dynamodb2
from lambda_api import lambda_handler

TXNS_TABLE = "lambda-table-for-blog"


@pytest.fixture
def use_moto():
    @mock_dynamodb2
    def dynamodb_client():
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')

        # Create the table
        dynamodb.create_table(
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
        return dynamodb
    return dynamodb_client

@mock_dynamodb2
def test_handler_for_failure(use_moto):
    use_moto()
    event = {
        "RecordId": "DonOfDen001"
    }

    return_data = lambda_handler(event, "")
    assert return_data['statusCode'] == 500
    assert return_data['error'] == 'No Records Found.'


@mock_dynamodb2
def test_handler_for_status_ok(use_moto):
    use_moto()
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
    print(return_data)
    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen002'
    assert body['Status'] == 'OK'
    assert body['Notes'] == 'DonOfDen Test Blog! - Unittest'


@mock_dynamodb2
def test_handler_for_different_status(use_moto):
    use_moto()
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
    print(return_data)
    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen008'
    assert body['Status'] == 'DRAFT'
    assert 'Notes' not in body
