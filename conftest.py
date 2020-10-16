"""pytest fixtures"""

import pytest
from collections import namedtuple


TestCase = namedtuple("TestCase", ("pb_msg_type", "expected_schema"))


def schema1():
    return "1"


@pytest.fixture(
    params=[
        TestCase("1", schema1()),
        TestCase("2", "2"),
        TestCase("3", "3"),
        TestCase("4", "4"),
    ]
)
def param(request):
    return request.param