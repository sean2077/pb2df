"""pytest file for testing pb2df"""
from pb2df import schema_for
from pb.example_pb2 import TimeMessage


def _test_schema(pb_msg_type, expected_schema):
    schema = schema_for(pb_msg_type.DESCRIPTOR)
    assert schema == expected_schema


def test_schema(param):
    _test_schema(param.pb_msg_type, param.expected_schema)


if __name__ == "__main__":

    schema = schema_for(TimeMessage.DESCRIPTOR)
    print(schema)
