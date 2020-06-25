from src.util import get_resource


def drop_table(table_name: str,) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.delete()
    except Exception as e:
        print(response)
        raise e

    return response
