import json

import click

from src.desc import describe_table
from src.list import list_tables


@click.group()
def cli() -> None:
    pass


@click.command()
@click.option("--table", help="get-item")
@click.option("--pkey", help="get-item")
@click.option("--skey", default=None, help="get-item")
def get(table: str, pkey: str, skey: str) -> None:
    # response = client.get_item(TableName=table, Key={"S": pkey})
    # print(response)
    click.echo(f"get {pkey}, {skey}")


@click.command()
@click.option("--pkey", help="put-item")
@click.option("--skey", default=None, help="put-item")
def put(pkey: str, skey: str) -> None:
    click.echo(f"put {pkey}, {skey}")


@click.command()
def list() -> None:
    table_names = list_tables()
    click.echo(json.dumps(table_names))


@click.command()
@click.option("--table", required=True, type=str, help="table name")
def desc(table: str) -> None:
    table_info = describe_table(table)
    click.echo(table_info)


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
cli.add_command(desc)
