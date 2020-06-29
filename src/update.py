from typing import Union

from src.util import cast_key_attr, get_client, get_key_names, get_resource


def update_item(table_name: str, update_expression: dict) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.update_item(**update_expression)
    except Exception as e:
        print(response)
        raise e

    return response


def generate_update_expression(
    table: str,
    pkey: Union[str, int],
    skey: Union[str, int, None],
    update_attr: str,
    update_value: str,
) -> dict:
    update_set = f"set {update_attr}=:val"
    expression_attr_val = {
        ":val": update_value,
    }

    client = get_client()
    ddl = client.describe_table(TableName=table)
    pkey_name, skey_name = get_key_names(ddl["Table"]["KeySchema"])

    pkey, skey = cast_key_attr(
        ddl["Table"]["AttributeDefinitions"], pkey, pkey_name, skey, skey_name
    )
    if skey:
        return {
            "Key": {pkey_name: pkey, skey_name: skey},
            "UpdateExpression": update_set,
            "ExpressionAttributeValues": expression_attr_val,
        }
    else:
        return {
            "Key": {pkey_name: pkey},
            "UpdateExpression": update_set,
            "ExpressionAttributeValues": expression_attr_val,
        }
