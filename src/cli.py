import json

import click

from src.create import create_table
from src.delete import delete_item
from src.desc import describe_table
from src.get import get_item
from src.list import list_tables
from src.put import put_item
from src.scan import scan_table
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
    click.echo(json.dumps(result, default=json_serial))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--payload", required=True, type=str, help="JSON payload")
def put(table: str, payload: str) -> None:
    payload_dict = json.loads(payload)
    result = put_item(table, payload_dict)
    click.echo(json.dumps(result))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, help="partition key")
@click.option("--skey", default=None, help="sort key")
def delete(table: str, pkey: str, skey: str) -> None:
    result = delete_item(table, pkey, skey)
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
@click.option("--ddl", type=str, help="ddl JSON")
@click.option(
    "--ddl_file", type=click.Path(exists=True), help="ddl JSON filepath"
)
def create(ddl: str, ddl_file: str) -> None:
    if ddl_file:
        with open(ddl_file, "r") as f:
            ddl_dict = json.load(f)
    elif ddl:
        ddl_dict = json.loads(ddl)
    else:
        raise AttributeError
    try:
        table_name, key_schema, attr_def, lsi, gsi = generate_ddl(ddl_dict)
        result = create_table(table_name, key_schema, attr_def, lsi, gsi)
    except Exception as e:
        raise e

    click.echo(result)


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--limit", default=100, type=int, help="output count limit")
def scan(table: str, limit: int) -> None:
    result = scan_table(table, limit)
    click.echo(json.dumps(result, default=json_serial))


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
cli.add_command(desc)
cli.add_command(create)
cli.add_command(delete)
cli.add_command(scan)
