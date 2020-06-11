from src.util import get_client


def list_tables() -> list:
    client = get_client()
    try:
        response = client.list_tables()
        table_names = response["TableNames"]
        while "LastEvaluatedTableName" in response:
            last_table = response["LastEvaluatedTableName"]
            response = client.list_tables(ExclusiveStartTableName=last_table)
            table_names += response["TableNames"]
    except Exception as e:
        print(response)
        raise e

    return table_names
