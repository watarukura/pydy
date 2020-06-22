from src.util import get_resource


def scan_table(table_name: str) -> dict:
    dynamodb = get_resource()
    try:
        response = dynamodb.scan(TableName=table_name)
        response_items = response["Items"]
        while response.LastEvaluatedKey:
            response = dynamodb.scan(
                TableName=table_name,
                ExclusiveStartKey=response.LastEvaluatedKey,
            )
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items
