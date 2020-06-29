from typing import Tuple

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


def generate_ddl(ddl: dict) -> Tuple[str, list, list, list, list]:
    table_name = ddl["Table"]["TableName"]
    key_schema = ddl["Table"]["KeySchema"]
    attr_def = ddl["Table"]["AttributeDefinitions"]
    lsi = []
    if lsi_ddls := ddl["Table"].get("LocalSecondaryIndexes"):
        for lsi_ddl in lsi_ddls:
            lsi.append(
                {
                    "IndexName": lsi_ddl["IndexName"],
                    "KeySchema": lsi_ddl["KeySchema"],
                    "Projection": lsi_ddl["Projection"],
                }
            )
    gsi = []
    if gsi_ddls := ddl["Table"].get("GlobalSecondaryIndexes"):
        for gsi_ddl in gsi_ddls:
            gsi.append(
                {
                    "IndexName": gsi_ddl["IndexName"],
                    "KeySchema": gsi_ddl["KeySchema"],
                    "Projection": gsi_ddl["Projection"],
                }
            )
    return table_name, key_schema, attr_def, lsi, gsi
