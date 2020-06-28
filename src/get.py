from typing import Dict, Optional, Union

from src.util import cast_key_attr, get_client, get_key_names, get_resource


def get_item(table_name: str, key_clause: dict) -> dict:
    dynamodb = get_resource()
    try:
        dynamodb_table = dynamodb.Table(table_name)
        response = dynamodb_table.get_item(Key=key_clause)
    except Exception as e:
        raise e

    return response["Item"]


def generate_key_clause(
    table: str, pkey: Union[str, int], skey: Union[str, int, None],
) -> Dict[Optional[str], Union[str, int]]:
    client = get_client()
    ddl = client.describe_table(TableName=table)
    pkey_name, skey_name = get_key_names(ddl["Table"]["KeySchema"])
    pkey, skey = cast_key_attr(
        ddl["Table"]["AttributeDefinitions"], pkey, pkey_name, skey, skey_name
    )

    if skey:
        key_clause = {pkey_name: pkey, skey_name: skey}
    else:
        key_clause = {pkey_name: pkey}
    return key_clause
