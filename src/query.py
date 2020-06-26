import pprint

from src.util import get_resource


def query_item(
    table_name: str,
    key_conditions: dict,
    limit: int,
    filter_expression: dict,
    index=None,
) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        if index:
            response = dynamodb_table.query(
                Limit=limit,
                **key_conditions,
                **filter_expression,
                IndexName=index
            )
        else:
            response = dynamodb_table.query(
                Limit=limit, **key_conditions, **filter_expression
            )
        response_items = response["Items"]
        count = response.get("Count")
        while count < limit and response.get("LastEvaluatedKey"):
            response = dynamodb_table.scan(
                TableName=table_name,
                ExclusiveStartKey=response.get("LastEvaluatedKey"),
                Limit=(limit - count),
                **key_conditions,
                **filter_expression,
                IndexName=index
            )
            count += response.get("Count")
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items
