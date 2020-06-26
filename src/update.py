from src.util import get_resource


def update_item(table_name: str, update_expression: dict) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.update_item(**update_expression)
    except Exception as e:
        print(response)
        raise e

    return response
