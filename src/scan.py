from boto3.dynamodb.conditions import Attr

from src.util import get_resource


def scan_table(table_name: str, limit: int, filter_expression: dict,) -> dict:
    dynamodb = get_resource()
    dynamodb_table = dynamodb.Table(table_name)
    try:
        response = dynamodb_table.scan(Limit=limit, **filter_expression,)
        response_items = response["Items"]
        count = response.get("Count")
        while count < limit and response.get("LastEvaluatedKey"):
            response = dynamodb_table.scan(
                TableName=table_name,
                ExclusiveStartKey=response.get("LastEvaluatedKey"),
                Limit=(limit - count),
                FIlterExpression=filter_expression,
            )
            count += response.get("Count")
            response_items.append(response["Items"])

    except Exception as e:
        print(response)
        raise e

    return response_items


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
