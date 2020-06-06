import click


@click.group()
def cli() -> None:
    pass


@click.command()
@click.option("--pkey", help="get-item")
@click.option("--skey", default=None, help="get-item")
def get(pkey: str, skey: str) -> None:
    click.echo(f"get {pkey}, {skey}")


@click.command()
@click.option("--pkey", help="put-item")
@click.option("--skey", default=None, help="put-item")
def put(pkey: str, skey: str) -> None:
    click.echo(f"put {pkey}, {skey}")


cli.add_command(get)
cli.add_command(put)
