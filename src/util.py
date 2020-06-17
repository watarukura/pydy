import os
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


def get_resource():
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    return dynamodb


def generate_ddl(
    pkey: str, pkey_attr: str, skey=None, skey_attr=None, gsi_list=[]
) -> Tuple[list, list, list]:
    # Partition Key
    key_schema = [{"AttributeName": pkey, "KeyType": "HASH"}]
    attr_def = [{"AttributeName": pkey, "AttributeType": pkey_attr}]

    # Sort Key
    if skey:
        key_schema.append({"AttributeName": skey, "KeyType": "RANGE"})
        attr_def.append({"AttributeName": skey, "AttributeType": skey_attr})

    gsi = []
    if gsi_list:
        for gsi_ddl in gsi_list:
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
            if not gsi_ddl.get("'Projection"):
                gsi_ddl["Projection"] = {"ProjectionType": "ALL"}

            # Sort Key
            if gsi_ddl.get("KeySchema")[1]:
                if not gsi_ddl.get("KeySchema")[1].get("AttributeName"):
                    raise AttributeError
                if (
                    not gsi_ddl.get("KeySchema")[1].get("KeyType")
                    or gsi_ddl.get("KeySchema")[1].get("KeyType") != "RANGE"
                ):
                    print(gsi_ddl)
                    raise AttributeError
            gsi.append(gsi_ddl)
    return key_schema, attr_def, gsi
