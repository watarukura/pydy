import json

import click

from src.create import create_table
from src.delete import delete_item
from src.desc import describe_table
from src.drop import drop_table
from src.get import get_item
from src.list import list_tables
from src.put import put_item
from src.query import query_item
from src.scan import scan_table
from src.update import update_item
from src.util import (
    generate_ddl,
    generate_filter_expression,
    generate_key_clause,
    generate_key_conditions,
    generate_update_expression,
    json_serial,
)


@click.group(help="DynamoDB CLI")
def cli() -> None:
    pass


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", required=True, help="partition key")
@click.option("--skey", default=None, help="sort key")
def get(table: str, pkey: str, skey: str) -> None:
    key_clause = generate_key_clause(table, pkey, skey)
    result = get_item(table, key_clause)
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
@click.option("--filter_key", type=str, help="filtering key name")
@click.option(
    "--filter_cond",
    type=click.Choice(
        ["eq", "ge", "gt", "lt", "le", "begins_with", "between", "contains"]
    ),
    help="filtering key condition",
)
@click.option("--filter_value", type=str, help="filtering value")
def scan(
    table: str,
    limit: int,
    filter_key: str,
    filter_cond: str,
    filter_value: str,
) -> None:
    if filter_key and filter_cond and filter_value:
        filter_expression = generate_filter_expression(
            filter_key, filter_cond, filter_value
        )
    else:
        filter_expression = {}
    result = scan_table(table, limit, filter_expression)
    click.echo(json.dumps(result, default=json_serial))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
def drop(table: str) -> None:
    table_info = drop_table(table)
    click.echo(json.dumps(table_info, default=json_serial))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--limit", default=100, type=int, help="output count limit")
@click.option("--pkey", type=str, help="partition key value")
@click.option("--skey", type=str, help="sort key value")
@click.option(
    "--skey_cond",
    type=click.Choice(
        ["eq", "ge", "gt", "lt", "le", "begins_with", "between", "contains"]
    ),
    help="where key condition",
)
@click.option("--filter_key", type=str, help="filtering key name")
@click.option(
    "--filter_cond",
    type=click.Choice(
        ["eq", "ge", "gt", "lt", "le", "begins_with", "between", "contains"]
    ),
    help="filtering key name",
)
@click.option("--filter_value", type=str, help="filtering key name")
@click.option("--index", type=str, default=None, help="index name")
def query(
    table: str,
    limit: int,
    pkey: str,
    skey: str,
    skey_cond: str,
    filter_key: str,
    filter_cond: str,
    filter_value: str,
    index: str,
) -> None:
    if filter_key and filter_cond and filter_value:
        filter_expression = generate_filter_expression(
            filter_key, filter_cond, filter_value
        )
    else:
        filter_expression = {}
    key_conditions = generate_key_conditions(
        table, pkey, skey, skey_cond, index
    )
    result = query_item(table, key_conditions, limit, filter_expression, index)
    click.echo(json.dumps(result, default=json_serial))


@cli.command()
@click.option("--table", required=True, type=str, help="table name")
@click.option("--pkey", type=str, help="partition key value")
@click.option("--skey", type=str, help="sort key value")
@click.option("--update_attr", type=str, help="update attribute")
@click.option("--update_value", type=str, help="update value")
def update(
    table: str, pkey: str, skey: str, update_attr: str, update_value
) -> None:
    update_expression = generate_update_expression(
        table, pkey, skey, update_attr, update_value
    )
    result = update_item(table, update_expression)
    click.echo(json.dumps(result, default=json_serial))


cli.add_command(get)
cli.add_command(put)
cli.add_command(list)
cli.add_command(desc)
cli.add_command(create)
cli.add_command(delete)
cli.add_command(scan)
cli.add_command(query)
cli.add_command(update)
