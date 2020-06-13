from src.util import get_resource


def put_item(table_name: str, payload: dict) -> dict:
    dynamodb = get_resource()
    try:
        dynamodb_table = dynamodb.Table(table_name)
        response = dynamodb_table.put_item(Item=payload)
    except Exception as e:
        raise e

    return response
