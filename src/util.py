import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Union

import boto3
from boto3.dynamodb.conditions import Attr, Key


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
    filter_attr = Attr(filter_key)
    if filter_cond == "eq":
        return {"FilterExpression": filter_attr.eq(filter_value)}
    elif filter_cond == "ne":
        return {"FilterExpression": filter_attr.eq(filter_value)}
    elif filter_cond == "gt":
        return {"FilterExpression": filter_attr.gt(filter_value)}
    elif filter_cond == "ge":
        return {"FilterExpression": filter_attr.gte(filter_value)}
    elif filter_cond == "lt":
        return {"FilterExpression": filter_attr.lt(filter_value)}
    elif filter_cond == "le":
        return {"FilterExpression": filter_attr.lte(filter_value)}
    elif filter_cond == "begins_with":
        return {"FilterExpression": filter_attr.begins_with(filter_value)}
    elif filter_cond == "between":
        return {"FilterExpression": filter_attr.between(*[filter_value])}
    elif filter_cond == "contains":
        return {"FilterExpression": filter_attr.contains(filter_value)}
    else:
        raise AttributeError("filter condition missing")


def generate_key_conditions(
    table: str,
    pkey: Union[str, int],
    skey: Union[str, int],
    skey_cond: str,
    index: str,
) -> dict:
    client = get_client()
    ddl = client.describe_table(TableName=table)
    if index:
        if lsi := ddl["Table"].get("LocalSecondaryIndexes"):
            for lsi_index in lsi:
                if index in lsi_index["IndexName"]:
                    pkey_name, skey_name = get_key_names(
                        lsi_index["KeySchema"]
                    )
        if gsi := ddl["Table"].get("GlobalSecondaryIndexes"):
            for gsi_index in gsi:
                if index == gsi.get("IndexName"):
                    pkey_name, skey_name = get_key_names(
                        gsi_index["KeySchema"]
                    )
    else:
        pkey_name, skey_name = get_key_names(ddl["Table"]["KeySchema"])

    for attr_def in ddl["Table"]["AttributeDefinitions"]:
        if attr_def["AttributeName"] == pkey_name:
            if attr_def["AttributeType"] == "N":
                pkey = int(pkey)
        if attr_def["AttributeName"] == skey_name:
            if attr_def["AttributeType"] == "N":
                skey = int(skey)
    key_condtion_pkey = Key(pkey_name).eq(pkey)

    if skey is None:
        return {"KeyConditionExpression": key_condtion_pkey}
    else:
        key_condtion_skey = Key(skey_name)
        if skey_cond == "eq":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.eq(skey)
            }
        elif skey_cond == "ne":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.ne(skey)
            }
        elif skey_cond == "gt":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.gt(skey)
            }
        elif skey_cond == "ge":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.gte(skey)
            }
        elif skey_cond == "lt":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.lt(skey)
            }
        elif skey_cond == "le":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.lte(skey)
            }
        elif skey_cond == "begins_with":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.begins_with(skey)
            }
        elif skey_cond == "between":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.between(*[skey])
            }
        elif skey_cond == "contains":
            return {
                "KeyConditionExpression": key_condtion_pkey
                & key_condtion_skey.contains(skey)
            }
        else:
            raise AttributeError("key condition missing")
