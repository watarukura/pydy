from src.util import get_resource


def scan_table(table_name: str) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.scan()
        response_items = response["Items"]
        while response.get("LastEvaluatedKey"):
            response = dynamodb.scan(
                TableName=table_name,
                ExclusiveStartKey=response.LastEvaluatedKey,
            )
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items
