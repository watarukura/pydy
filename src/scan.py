import boto3

from src.util import get_resource


def scan_table(table_name: str, limit: int, filter_expression: dict,) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.scan(Limit=limit, **filter_expression,)
        response_items = response["Items"]
        count = response.get("Count")
        while count < limit and response.get("LastEvaluatedKey"):
            print(count)
            response = dynamodb_table.scan(
                TableName=table_name,
                ExclusiveStartKey=response.get("LastEvaluatedKey"),
                Limit=(limit - count),
                FIlterExpression=filter_expression,
            )
            count += response.get("Count")
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items
