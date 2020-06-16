from src.util import get_resource


def create_table(
    table_name: str, key_schema: list, attribute_definition: list, gsi=[]
) -> dict:
    dynamodb = get_resource()
    try:
        if gsi:
            response = dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definition,
                BillingMode="PAY_PER_REQUEST",
                GlobalSecondaryIndexes=gsi,
            )
        else:
            response = dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definition,
                BillingMode="PAY_PER_REQUEST",
            )
    except Exception as e:
        print(response)
        raise e

    return response
