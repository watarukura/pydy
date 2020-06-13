import json

import click

from src.desc import describe_table
from src.list import list_tables


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, help="partition key")
@click.option("--skey", default=None, help="sort key")
def get(table: str, pkey: str, skey: str) -> None:
    # response = client.get_item(TableName=table, Key={"S": pkey})
    # print(response)
    click.echo(f"get {pkey}, {skey} from {table}")


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, help="partition key")
@click.option("--skey", default=None, help="sort key")
def put(table: str, pkey: str, skey: str) -> None:
    click.echo(f"put {pkey}, {skey} from {table}")


@cli.command()
def list() -> None:
    table_names = list_tables()
    click.echo(json.dumps(table_names))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
def desc(table: str) -> None:
    table_info = describe_table(table)
    click.echo(table_info)


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
cli.add_command(desc)
