from src.util import generate_key_clause, get_key_names, get_resource


def get_item(table_name: str, pkey: str, skey=None) -> dict:
    dynamodb = get_resource()
    try:
        dynamodb_table = dynamodb.Table(table_name)

        pkey_name, skey_name = get_key_names(dynamodb_table.key_schema)
        key_clause = generate_key_clause(
            dynamodb_table.attribute_definitions,
            pkey,
            pkey_name,
            skey,
            skey_name,
        )
        response = dynamodb_table.get_item(Key=key_clause)
    except Exception as e:
        raise e

    return response["Item"]
