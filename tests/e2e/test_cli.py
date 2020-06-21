import pytest
from click.testing import CliRunner

from src.cli import create, get, list, put


runner = CliRunner()


@pytest.mark.parametrize(
    "filepath, expect",
    (
        ("tests/e2e/json/Forum.json", "dynamodb.Table(name='Forum')\n",),
        (
            "tests/e2e/json/ProductCatalog.json",
            "dynamodb.Table(name='ProductCatalog')\n",
        ),
        ("tests/e2e/json/Reply.json", "dynamodb.Table(name='Reply')\n",),
        ("tests/e2e/json/Thread.json", "dynamodb.Table(name='Thread')\n",),
    ),
)
def test_create(filepath: str, expect: str) -> None:
    result = runner.invoke(create, args=["--ddl_file", filepath])
    assert result.output == expect


def test_list() -> None:
    result = runner.invoke(list)
    assert result.output == '["Forum", "ProductCatalog", "Reply", "Thread"]\n'


@pytest.mark.parametrize(
    "table, payload, pkey, expect",
    (("ProductCatalog", '{"Id": 1}', 1, '{"Id": 1}\n',),),
)
def test_put_get(table: str, payload: str, pkey: str, expect: str) -> None:
    runner.invoke(put, args=["--table", table, "--payload", payload])
    result = runner.invoke(get, args=["--table", table, "--pkey", pkey])
    assert result.output == expect
