import pytest
from click.testing import CliRunner

from src.cli import create, delete, get, list, put, scan


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
    "table, payload, pkey, skey, expect",
    (
        ("ProductCatalog", '{"Id": 1}', 1, None, '{"Id": 1}\n',),
        (
            "Reply",
            '{"Id": "1", "ReplyDateTime": "20200622184100"}',
            "1",
            "20200622184100",
            '{"Id": "1", "ReplyDateTime": "20200622184100"}\n',
        ),
    ),
)
def test_put_get_delete(
    table: str, payload: str, pkey: str, skey: str, expect: str
) -> None:
    runner.invoke(put, args=["--table", table, "--payload", payload])
    args = ["--table", table, "--pkey", pkey]
    if skey:
        args.append("--skey")
        args.append(skey)
    result = runner.invoke(get, args=args)
    assert result.output == expect

    runner.invoke(delete, args=args)
    delete_result = runner.invoke(get, args=args)
    assert delete_result.output == ""


@pytest.mark.parametrize(
    "table, payload, expect",
    (
        ("ProductCatalog", '{"Id": 1}', '[{"Id": 1}]\n',),
        (
            "Reply",
            '{"Id": "1", "ReplyDateTime": "20200622184100"}',
            '[{"Id": "1", "ReplyDateTime": "20200622184100"}]\n',
        ),
    ),
)
def test_scan(table: str, payload: str, expect: str) -> None:
    runner.invoke(put, args=["--table", table, "--payload", payload])
    result = runner.invoke(scan, args=["--table", table])
    assert result.output == expect


@pytest.mark.parametrize(
    "table, payload1, payload2, limit, expect1, expect2",
    (
        (
            "ProductCatalog",
            '{"Id": 1}',
            '{"Id": 2}',
            1,
            '[{"Id": 1}]\n',
            '[{"Id": 2}]\n',
        ),
    ),
)
def test_scan_limit(
    table: str, payload1: str, payload2: str, limit: str, expect1, expect2
) -> None:
    runner.invoke(put, args=["--table", table, "--payload", payload1])
    runner.invoke(put, args=["--table", table, "--payload", payload2])
    result = runner.invoke(scan, args=["--table", table, "--limit", limit])
    assert result.output == expect1 or result.output == expect2
