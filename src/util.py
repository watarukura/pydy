import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Union

import boto3


def get_client():
    client = boto3.client(
        "dynamodb",
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    return client


def get_resource() -> boto3.resources.base.ServiceResource:
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    return dynamodb


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


def json_serial(obj):
    # 日付型の場合には、文字列に変換します
    if isinstance(obj, (datetime)):
        return datetime.strftime(obj, "%Y-%m-%d %H:%M:%S")
    if isinstance(obj, (Decimal)):
        return int(obj)
    # 上記以外はサポート対象外
    raise TypeError("Type %s not serializable" % type(obj))


def get_key_names(key_schema: List[Dict]) -> Tuple[str, None]:
    if len(key_schema) == 1:
        return key_schema[0]["AttributeName"], None
    else:
        return key_schema[0]["AttributeName"], key_schema[1]["AttributeName"]


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


def generate_filter_expression(
    filter_key: str, filter_cond: str, filter_value: str
) -> dict:
    filter_expression = {
        "FilterExpression": f"Key({filter}).{filter_cond}({filter_value})"
    }
    return filter_expression
