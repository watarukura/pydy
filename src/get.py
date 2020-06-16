from src.util import get_resource


def get_item(table_name: str, pkey: str, skey=None) -> dict:
    dynamodb = get_resource()
    try:
        dynamodb_table = dynamodb.Table(table_name)
        for key in dynamodb_table.key_schema:
            if key["KeyType"] == "HASH":
                pkey_name = key["AttributeName"]
            else:
                skey_name = key["AttributeName"]
        key_clause = {pkey_name: pkey}
        if skey:
            key_clause[skey_name] = skey
        response = dynamodb_table.get_item(Key=key_clause)
    except Exception as e:
        raise e

    return response["Item"]
