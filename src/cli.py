import json

import click

from src.create import create_table
from src.desc import describe_table
from src.get import get_item
from src.list import list_tables
from src.put import put_item
from src.util import generate_ddl, json_serial


@click.group(help="DynamoDB CLI")
def cli() -> None:
    pass


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, help="partition key")
@click.option("--skey", default=None, help="sort key")
def get(table: str, pkey: str, skey: str) -> None:
    result = get_item(table, pkey, skey)
    click.echo(json.dumps(result))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--payload", required=True, type=str, help="JSON payload")
def put(table: str, payload: str) -> None:
    payload_dict = json.loads(payload)
    result = put_item(table, payload_dict)
    click.echo(json.dumps(result))


@cli.command()
def list() -> None:
    table_names = list_tables()
    click.echo(json.dumps(table_names))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
def desc(table: str) -> None:
    table_info = describe_table(table)
    click.echo(json.dumps(table_info, default=json_serial))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, type=str, help="partition key")
@click.option(
    "--pkey_attr",
    required=True,
    type=click.Choice(["S", "N", "B"], case_sensitive=False),
    help="partition key type (S|N|B)",
)
@click.option("--skey", type=str, help="sort key")
@click.option(
    "--skey_attr",
    type=click.Choice(["S", "N", "B"], case_sensitive=False),
    help="sort key type (S|N|B)",
)
@click.option("--gsi_json", type=str, help="gsi ddl JSON")
def create(
    table: str,
    pkey: str,
    pkey_attr: str,
    skey: str,
    skey_attr: str,
    gsi_json=None,
) -> None:
    if gsi_json:
        gsi_list = json.loads(gsi_json)
    else:
        gsi_list = []
    try:
        key_schema, attr_def, gsi = generate_ddl(
            pkey, pkey_attr, skey, skey_attr, gsi_list
        )
        result = create_table(table, key_schema, attr_def, gsi)
    except Exception as e:
        raise e

    click.echo(json.dumps(result))


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
cli.add_command(desc)
cli.add_command(create)
