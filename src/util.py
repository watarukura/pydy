import os
from datetime import datetime
from typing import Tuple

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
    # 上記以外はサポート対象外
    raise TypeError("Type %s not serializable" % type(obj))
