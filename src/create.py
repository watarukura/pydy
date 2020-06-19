import json

from src.util import get_resource


def create_table(
    table_name: str,
    key_schema: list,
    attribute_definition: list,
    lsi=[],
    gsi=[],
) -> dict:
    dynamodb = get_resource()
    ddl = {
        "TableName": table_name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attribute_definition,
        "BillingMode": "PAY_PER_REQUEST",
    }
    if lsi:
        ddl["LocalSecondaryIndexes"] = lsi
    if gsi:
        ddl["GlobalSecondaryIndexes"] = gsi
    try:
        response = dynamodb.create_table(**ddl)
    except Exception as e:
        print(response)
        raise e

    return response
