from typing import Dict, List, Union

from src.util import get_key_names, get_resource


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


def generate_key_clause(
    attribute_definitions: List[Dict],
    pkey: str,
    pkey_name: str,
    skey: str,
    skey_name=None,
) -> Dict[str, Union[str, int]]:
    key_clause: Dict[str, Union[str, int]] = {}
    for attr_def in attribute_definitions:
        if attr_def["AttributeName"] == pkey_name:
            if attr_def["AttributeType"] == "N":
                key_clause = {pkey_name: int(pkey)}
            else:
                key_clause = {pkey_name: pkey}
        if skey_name and attr_def["AttributeName"] == skey_name:
            if attr_def["AttributeType"] == "N":
                key_clause[skey_name] = int(skey)
            else:
                key_clause[skey_name] = skey
    return key_clause
