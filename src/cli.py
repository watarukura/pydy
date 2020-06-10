import boto3
import click

client = boto3.client("dynamodb")


@click.group()
def cli() -> None:
    pass


@click.command()
@click.option("--table", help="get-item")
@click.option("--pkey", help="get-item")
@click.option("--skey", default=None, help="get-item")
def get(table: str, pkey: str, skey: str) -> None:
    response = client.get_item(TableName=table, Key={"S": pkey})
    print(response)
    # click.echo(f"get {pkey}, {skey}")


@click.command()
@click.option("--pkey", help="put-item")
@click.option("--skey", default=None, help="put-item")
def put(pkey: str, skey: str) -> None:
    click.echo(f"put {pkey}, {skey}")


@click.command()
def list() -> None:
    try:
        response = client.list_tables()
        if last_evaluated_table_name := response["LastEvaluatedTableName"]:
            add_table_list = client.list_tables(
                ExclusiveStartTableName=last_evaluated_table_name)
            response["TableNames"].append(add_table_list["TableNames"])
    except Exception as e:
        click.echo(response["ResponseMetadata"])
        raise e

    click.echo(response["TableNames"])


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
