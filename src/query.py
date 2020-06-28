from typing import Union

from boto3.dynamodb.conditions import Key

from src.util import cast_key_attr, get_client, get_key_names, get_resource


def query_item(
    table_name: str, key_conditions: dict, limit: int, index=None,
) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        if index:
            response = dynamodb_table.query(
                Limit=limit, **key_conditions, IndexName=index,
            )
        else:
            response = dynamodb_table.query(Limit=limit, **key_conditions)
        response_items = response["Items"]
        count = response.get("Count")
        while count < limit and response.get("LastEvaluatedKey"):
            if index:
                response = dynamodb_table.query(
                    Limit=(limit - count),
                    **key_conditions,
                    IndexName=index,
                    ExclusiveStartKey=response.get("LastEvaluatedKey"),
                )
            else:
                response = dynamodb_table.query(
                    Limit=(limit - count),
                    **key_conditions,
                    ExclusiveStartKey=response.get("LastEvaluatedKey"),
                )
            count += response.get("Count")
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items


def generate_key_conditions(
    table: str,
    pkey: Union[str, int],
    skey: Union[str, int, None],
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

    pkey, skey = cast_key_attr(
        ddl["Table"]["AttributeDefinitions"], pkey, pkey_name, skey, skey_name
    )

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
