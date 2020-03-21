# Python Pytest for AWS Lambdas and Dynamodb

In this project, I have written a AWS lambda with dynamodb and using pytest we have tested the code against aws resources.

## Aim of the application

- Create a AWS Lambda with dynamodb operation.
- Learn how to use `pytest-env`.
- How to use `moto` with `pytest` to mock AWS resources.

## How to Run the application

To install required packages.

```cmd
pip install -r requirements.txt
```

To run test.

```cmd
pytest -v
```

## Explanation

> **Note:** Check my blog [Python Pytest for AWS Lambdas and Dynamodb](http://donofden.com/blog/2020/03/20/Pytest-for-AWS-Lambdas-and-Dynamodb) 


In this article, I'll show you the techniques which I have incorporated into my test suites using Pytest.

I’ve had the chance to use Lambda functions in my recent work, With a low cost of getting started, Lambda has been useful for building small programs which can do a quick set of tasks. Since I know a bit of python I could achieve my tasks but unfortunately, I have no idea about automated testing with lambda with AWS resources.

 I wanted to find a way to test Lambda functions while developing them - preferably, in a test environment that would not make live calls to AWS, and allow me to test the behavior of a given function at a high level.

 In researching this I found some articles which provided some information about using [`pytest`](https://github.com/pytest-dev/pytest) with [`moto`](https://github.com/spulec/moto). Moto’s approach is to mock out AWS services entirely, in a stateful way. Your code still can make calls to create and alter resources, and it will appear as though these changes are actually being made. However, this is all happening in moto’s mocked services, not on AWS directly.

## Task

 > To write a lambda which will be triggered by an API gateway, with according to the request our lambda will server the record status from dynamodb.

Let's start writing our lambda, I always prefer multiple functions/chunks of code then a long list of line, since chunks can be easily tested and reusable. so I'm going to split the function into two functions.

 - `lambda_handler` - This is the handler for the lambda.
 - `get_record_status` - The function to check dynamodb tables.

### lambda_handler

Let's create a file `lambda_api.py` and write our lambda functions.

```python
import boto3
import os
import json

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
```

If you see the above code we take the `event` value which holds the requested `RecordId` (`PARTITION_KEY`) to fetch the status. The `RecordType` is a `SORT_KEY` of dynamodb which additionally we use to retrieve our record details. Please check dynamodb doc for `SORT_KEY` (since I felt it difficult to use the `mock` for it, I thought it will be helpful to other tp show how we can use it.)

`DbEntity` is a class that I use as an entity class for my DynamoDB function.

```python
class DbEntity:
    def __init__(self, record_id, record_type):
        """
        This function used to initiate values to the class variables
        """
        self.RecordId = record_id
        self.RecordType = record_type
```

- `RecordId` & `RecordType` represents my field names in dynamodb.

`ResultEntity` is a class that I use as an entity class for my return response.

```python
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
```

- `RecordId`,`Status` & `Notes` represents my field which i fetch from dynamodb. Also if you look here I return `Notes` only when my status is `OK`. (Instead of using `"OK"` in if condition, you can assign the value to a CONSTANT and validated it in the condition. Just saying...)

### `get_record_status`

Well here comes the tricky part we all are waiting. Let's jump into the code first then will explain what it does.

```python
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
```

If you look at the code, I'm accessing the table and submitting a request using the `get_item` built-in function of dynamodb with appropriate details. The `DYNAMO_DB` is a global variable here will explain it further in the next section. So here I'm defining my `PARTITION_KEY` and `SORT_KEY` in the `Key` section. If you didn't use `SORT_KEY` feel free to remove it.

In `ProjectionExpression` we select the field we need from the table. `TABLE_NAME` is a global variable used in this lambda.

Ok, let's see what are the other chunks I used to manage this lambda to work like `NoRecordError` Exception handler and `DYNAMO_DB` global variable.

A simple and basic Exception handler:

```python
class Error(Exception):
    """Base class for other exceptions"""
    pass


class NoRecordError(Error):
    """Raised when no record found"""
    pass
```

Setting up my dynamodb client for the development environment so we can test our lambda from the local environment rather than running in `AWS`.

Make sure u set `TableName` and `STAGE`in ur env variables.

```python
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
```

With this, we successfully created a lambda. To test run, place the script in the bottom of the lambda file and execute. You know the magic `python lambda.py`.

```python
event = {"RecordId":"008"}
context = ""
result = lambda_handler(event, context)

json.dumps(result, default=lambda o: o.__dict__)
```

At this point, If you have a record in dynamodb the result should be the following.

```json
{
    'statusCode': 200,
    'body': {
        'RecordId': '008',
        'Status': 'OK',
        'Notes':'This is DonOfDen Blog!'
    }
}
```

## Adding a Test File

Let’s create a test file that we can use to test our lamdba function. First, add [`pytest`](https://github.com/pytest-dev/pytest), [`moto`](https://github.com/spulec/moto) and [`pytest-env`](https://github.com/MobileDynasty/pytest-env) to the `requirements.txt` file:

```txt
pytest
pytest-env
moto
```

And then install them using pip

```cmd
pip install -r requirements.txt
```

`pytest-env` This is a py.test plugin that enables you to set environment variables in the `pytest.ini` file.

```ini
# our pytest.ini file
[pytest]
env =
    TableName=lambda-table-for-blog
    STAGE=DEVELOPMENT
```

With `moto` and `pytest` in place, we can create a new file, `test_lambda_api.py`, for our tests:

```python
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
    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen008'
    assert body['Status'] == 'DRAFT'
    assert 'Notes' not in body
```

Since we have written the lambda I thought explaining the test as a whole will be easier to understand. If you see the test file I have imported the necessary packages for the test.

```python
import boto3
import pytest
import json

from moto import mock_dynamodb2
from lambda_api import lambda_handler
```

`boto3` package to work with AWS, `pytest` for testing/mock, `json` to assert the response, from `moto` I'm importing `mock_dynamodb2` since we only using AWS dynamodb resource here. so why `mock_dynamodb2`? not `3?` not `4?` or simple `mock_dynamodb`?

The answer is simple if you've used `boto3` we need to use `mock_dynamodb2` if your using an older version of `boto` need to use `mock_dynamodb`. Ok, So what about `3?`, `4?` there is no such function available at the moment when I was writing this blog. ``¯\_(ツ)_/¯``

Now let's check the `@pytest.fixture` I have written. `use_moto()` this function has `dynamodb_client()` which we marked as a mocker for dynamodb client which we need to specify as `@mock_dynamodb2` at the top.

So inside that, we can create a dynamodb resource and create a table as you did in `aws-cdk`. This will represent your table in production, remember `moto` will not contact `AWS` it will create a virtual environment for us.

So whenever I use dynamodb in my test I will pass the `@pytest.fixture` and add `@mock_dynamodb2` to the top of the function, so our `pytest` understand it and runs with moto.

Let's take the below function and go through it to understand the working.

```python
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

    body = json.loads(return_data['body'])

    assert return_data['statusCode'] == 200
    assert body['RecordId'] == 'DonOfDen008'
    assert body['Status'] == 'DRAFT'
    assert 'Notes' not in body
```

As explained before we need to use `@mock_dynamodb2` to specify the function is using dynamo mock from `moto`. We passed in `use_moto` a `@python.fixture` and called the function inside our test `use_moto()` so it will create a table in dynamo for us to test.

Then, using `put_item` function from dynamodb we inserting a value for us to test while calling `lambda_handler`.

We are calling `lambda_handler(event, "")` along with the `json` format event. It executed the lambda and gets the result we have inserted in the `put_item` function. Note, all of it works in `moto` environment, not in `aws`.

Finally, we assert the response to seeing our lambda behaves as expected. I believe the rest of the tests are self-explanatory.