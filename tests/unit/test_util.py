from typing import Any, Dict, List

import pytest

from src.util import get_key_names


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
