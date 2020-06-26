from src.util import get_resource


def get_item(table_name: str, key_clause: dict) -> dict:
    dynamodb = get_resource()
    try:
        dynamodb_table = dynamodb.Table(table_name)
        response = dynamodb_table.get_item(Key=key_clause)
    except Exception as e:
        raise e

    return response["Item"]
