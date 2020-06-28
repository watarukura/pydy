import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Union

import boto3


def get_client() -> boto3.client:
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


def json_serial(obj):
    # 日付型の場合には、文字列に変換します
    if isinstance(obj, (datetime)):
        return datetime.strftime(obj, "%Y-%m-%d %H:%M:%S")
    if isinstance(obj, (Decimal)):
        return int(obj)
    # 上記以外はサポート対象外
    raise TypeError("Type %s not serializable" % type(obj))


def get_key_names(key_schema: List[Dict]) -> Tuple[str, Union[str, None]]:
    if len(key_schema) == 1:
        return key_schema[0]["AttributeName"], None
    else:
        return key_schema[0]["AttributeName"], key_schema[1]["AttributeName"]


def cast_key_attr(
    attribute_definitions: list,
    pkey: Union[str, int],
    pkey_name: str,
    skey=None,
    skey_name=None,
) -> Tuple[Union[str, int], Union[str, int, None]]:
    for attr_def in attribute_definitions:
        if attr_def["AttributeName"] == pkey_name:
            if attr_def["AttributeType"] == "N":
                pkey = int(pkey)
        if skey and attr_def["AttributeName"] == skey_name:
            if attr_def["AttributeType"] == "N":
                skey = int(skey)

    return pkey, skey
