from src.util import get_client


def describe_table(table_name: str) -> dict:
    client = get_client()
    try:
        response = client.describe_table(TableName=table_name)
    except Exception as e:
        print(response)
        raise e

    return response
