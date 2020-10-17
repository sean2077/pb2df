"""pytest fixtures"""

import pytest
from collections import namedtuple

from pyspark.sql import types
import example_pb2


TestCase = namedtuple("TestCase", ("pb_msg_type", "expected_schema"))


simple_msg_schema = types.StructType(
    [
        types.StructField("field", types.IntegerType()),
    ]
)

keyvalue_msg_schema = types.StructType(
    [
        types.StructField("key", types.StringType()),
        types.StructField("value", types.IntegerType()),
    ]
)

map_msg_schema = types.StructType(
    [
        types.StructField(
            "repeated_keyvalue_field", types.ArrayType(keyvalue_msg_schema)
        ),
        types.StructField(
            "map_field", types.MapType(types.StringType(), types.IntegerType())
        ),
    ]
)

complex_msg_schema = types.StructType(
    [
        types.StructField("double_field", types.DoubleType()),
        types.StructField("float_field", types.FloatType()),
        types.StructField("int32_field", types.IntegerType()),
        types.StructField("int64_field", types.LongType()),
        types.StructField("uint32_field", types.IntegerType()),
        types.StructField("uint64_field", types.LongType()),
        types.StructField("sint32_field", types.IntegerType()),
        types.StructField("sint64_field", types.LongType()),
        types.StructField("fixed32_field", types.IntegerType()),
        types.StructField("fixed64_field", types.LongType()),
        types.StructField("sfixed32_field", types.IntegerType()),
        types.StructField("sfixed64_field", types.LongType()),
        types.StructField("bool_field", types.BooleanType()),
        types.StructField("string_field", types.StringType()),
        types.StructField("bytes_field", types.BinaryType()),
        types.StructField("enum_field", types.IntegerType()),
        types.StructField("repeated_field", types.ArrayType(types.IntegerType())),
        types.StructField("nested_field", simple_msg_schema),
        types.StructField("repeated_nested_field", types.ArrayType(simple_msg_schema)),
        types.StructField(
            "repeated_keyvalue_field", types.ArrayType(keyvalue_msg_schema)
        ),
        types.StructField(
            "simple_map_field", types.MapType(types.StringType(), types.IntegerType())
        ),
        types.StructField(
            "complex_map_field", types.MapType(types.StringType(), map_msg_schema)
        ),
        types.StructField("repeated_map_field", types.ArrayType(map_msg_schema)),
    ]
)


@pytest.fixture(
    params=[
        TestCase(example_pb2.SimpleMessage, simple_msg_schema),
        TestCase(example_pb2.KeyValueMessage, keyvalue_msg_schema),
        TestCase(example_pb2.MapMessage, map_msg_schema),
        TestCase(example_pb2.ComplexMessage, complex_msg_schema),
    ]
)
def param(request):
    return request.param