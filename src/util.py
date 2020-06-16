import os
from typing import Dict, Tuple

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


def get_resource():
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    return dynamodb


def validate_ddl(
    pkey: str, pkey_attr: str, skey=None, skey_attr=None, gsi=[]
) -> Tuple[list, list]:
    # Partition Key
    key_schema = [{"AttributeName": pkey, "KeyType": "HASH"}]
    attr_def = [{"AttributeName": pkey, "AttributeType": pkey_attr}]

    # Sort Key
    if skey:
        key_schema.append({"AttributeName": skey, "KeyType": "RANGE"})
        attr_def.append({"AttributeName": skey, "AttributeType": skey_attr})

    if gsi:
        for gsi_ddl in gsi:
            # Partition Key
            if not gsi_ddl.get("IndexName"):
                raise AttributeError
            if not gsi_ddl.get("KeySchema"):
                raise AttributeError
            if not gsi_ddl.get("KeySchema")[0].get("AttributeName"):
                raise AttributeError
            if (
                not gsi_ddl.get("KeySchema")[0].get("KeyType")
                or gsi_ddl.get("KeySchema")[0].get("KeyType") != "HASH"
            ):
                raise AttributeError

            # Sort Key
            if not gsi_ddl.get("KeySchema")[1].get("AttributeName"):
                raise AttributeError
            if (
                not gsi_ddl[1].get("KeyType")
                or gsi_ddl[1].get("KeyType") != "RANGE"
            ):
                raise AttributeError
    return key_schema, attr_def
