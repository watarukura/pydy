from typing import Any, Dict, List

import pytest

from src.util import generate_key_clause, get_key_names


@pytest.mark.parametrize(
    "key_schema, expect_pkey_name, expect_skey_name",
    (
        ([{"AttributeName": "Id", "KeyType": "HASH"}], "Id", None),
        (
            [
                {"AttributeName": "Id", "KeyType": "HASH"},
                {"AttributeName": "ReplyDateTime", "KeyType": "RANGE"},
            ],
            "Id",
            "ReplyDateTime",
        ),
    ),
)
def test_get_key_name(
    key_schema: List[Dict], expect_pkey_name: str, expect_skey_name: str
):
    pkey_name, skey_name = get_key_names(key_schema)
    assert pkey_name == expect_pkey_name
    assert skey_name == expect_skey_name


@pytest.mark.parametrize(
    "attr_def, pkey, pkey_name, skey,skey_name, expect",
    (
        (
            [
                {"AttributeName": "Id", "AttributeType": "S"},
                {"AttributeName": "ReplyDateTime", "AttributeType": "S"},
                {"AttributeName": "PostedBy", "AttributeType": "S"},
            ],
            "1",
            "Id",
            None,
            None,
            {"Id": "1"},
        ),
        (
            [
                {"AttributeName": "Id", "AttributeType": "S"},
                {"AttributeName": "ReplyDateTime", "AttributeType": "S"},
                {"AttributeName": "PostedBy", "AttributeType": "S"},
            ],
            "1",
            "Id",
            "20200623062000",
            "ReplyDateTime",
            {"Id": "1", "ReplyDateTime": "20200623062000"},
        ),
    ),
)
def test_generate_key_clause(
    attr_def: List[Dict],
    pkey: str,
    pkey_name: str,
    skey: str,
    skey_name: str,
    expect: str,
):
    key_clause = generate_key_clause(
        attr_def, pkey, pkey_name, skey, skey_name
    )
    assert key_clause == expect
